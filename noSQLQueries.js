//===============================================================
//BC2402 Designing & Developing Databases
//Purpose: NoSQL Queries
//Seminar: SEM1
//Group: Grp8
//Author: Angel Han Feng Yi, Aileen Laksmono, Andrew Imanuel, Julius Daniel Sarwono, Han Xiao
//Supervising Tutor: Ben Choi
//===============================================================

use submission

//---------------------------------------------------------------
//1.	Generate a list of unique locations (countries) in Asia
db.getCollection("owid-covid-data-1").distinct("location",{"continent": "Asia"})
//---------------------------------------------------------------

//---------------------------------------------------------------
//2.	Generate a list of unique locations (countries) in Asia and Europe, with more than 10 total cases on 2020-04-01
db.getCollection("owid-covid-data-1").distinct("location",
{$and:[{"total_cases":{$gt:10}},{"date":{$gte:ISODate("2020-04-01")}},
{"date":{$lte:ISODate("2020-04-02")}},
{$or:[{"continent": "Asia"},{"continent":"Europe"}]}]})

//---------------------------------------------------------------

//---------------------------------------------------------------
//3.	Generate a list of unique locations (countries) in Africa, with less than 10,000 total cases between 2020-04-01 and 2020-04-20 (inclusive of the start date and end date)
db.getCollection("owid-covid-data-1").distinct("location",{$and:[{"total_cases":{$lt:10000}},
{"date":{$gte:ISODate("2020-04-01")}},{"date":{$lte:ISODate("2020-04-20")}},
{"continent":"Africa"}]})


//---------------------------------------------------------------

//---------------------------------------------------------------
//4.	Generate a list of unique locations (countries) without any data on total tests
db.getCollection("owid-covid-data-1").aggregate([
    {$group: { _id :"$location" , totalTests: {$sum:"$total_tests"}}},
    {$match : {"totalTests":0}},
    {$sort: {_id:1}}
    ])
//---------------------------------------------------------------

//---------------------------------------------------------------
//5.	Conduct trend analysis, i.e., for each month, compute the total number of new cases globally. 
db.getCollection("owid-covid-data-1").aggregate([
    {$match:{$and:[{"continent":{$ne:""}},{"new_cases":{$ne:NaN}}]}},
    {$group:
        {_id:
            {groupByMonth:{$month:"$date"}, groupByYear:{$year:"$date"}}, sumCases:{$sum:"$new_cases"}}},
    {$project:{_id:1, sumCases:1}}
    ]).sort({_id: 1 } );

//---------------------------------------------------------------

//---------------------------------------------------------------
//6.	Conduct trend analysis, i.e., for each month, compute the total number of new cases in each continent
db.getCollection("owid-covid-data-1").aggregate([
    {$match:{$and:[{"continent":{$ne:""}},{"new_cases":{$ne:NaN}}]}},
    {$group:
        {_id:
            {groupByMonth:{$month:"$date"}, groupByYear:{$year:"$date"},groupByContinent:"$continent"}, countByDate:{$sum:"$new_cases"}}},
    {$project:{_id:1, countByDate:1}}
    ])
//---------------------------------------------------------------

//---------------------------------------------------------------
//7.	Generate a list of EU countries that have implemented mask related responses (i.e., response measure contains the word “mask”).
var list_europe = db.getCollection("owid-covid-data-1").aggregate([
    {$match:{"continent": "Europe"}},
    {$project:{"location":1}}
    ]).map(function (doc){ return doc.country;})


db.getCollection("response_graphs").aggregate([
    {$match:{$and:[{$or:[{"Response_measure":{$regex: /Mask/}},{"Response_measure":{$regex: /mask/}}]},{"country":{$in: list_europe}}]}},
    {$group: {_id: {country:"$Country"}}},
    ])
//---------------------------------------------------------------

//---------------------------------------------------------------
//8.	Compute the period (i.e., start date and end date) in which most EU countries has implemented MasksMandatory as the response measure. For NA values, use 1-Auguest 2020.
var max = db.getCollection("maskmandatory_countries").aggregate([
                { "$unwind": "$DATEVAR" },
                {
                    "$group": {
                        "_id": "$DATEVAR",
                        "count": { "$sum": 1 }}
                },
                { "$sort": { "count": -1 } },
                { "$limit": 1 } ,
                {$project:{count:1, _id:0}}]).map(function (doc){ return doc.count; });

db.getCollection("maskmandatory_countries").aggregate([
    { "$unwind": "$DATEVAR" },
    {
        "$group": {
            "_id": "$DATEVAR",
            "count": { "$sum": 1 }}
    },
    { "$sort": { "count": -1 } },
    {"$match":{"count":{"$in":max}}},
    { "$limit": 10}
])
//---------------------------------------------------------------

//---------------------------------------------------------------
//9.	Based on the period above, conduct trend analysis for Europe and North America, i.e., for each day during the period, compute the total number of new cases.
var max = db.getCollection("maskmandatory_countries").aggregate([
                { "$unwind": "$DATEVAR" },
                {
                    "$group": {
                        "_id": "$DATEVAR",
                        "count": { "$sum": 1 }}
                },
                { "$sort": { "count": -1 } },
                { "$limit": 1 } ,
                {$project:{count:1, _id:0}}]).map(function (doc){ return doc.count; });
    
var dates =
db.getCollection("maskmandatory_countries").aggregate([
    { "$unwind": "$DATEVAR" },
    {
        "$group": {
            "_id": "$DATEVAR",
            "count": { "$sum": 1 }}
    },
    { "$sort": { "count": -1 } },
    {"$match":{"count":{"$in":max}}},
    {"$project":{_id:1}},
    { "$limit": 10}
])
.map(function (doc){ return doc._id; });


db.getCollection("owid-covid-data-1").aggregate([
    {$match:{$and:[{"date": {"$in":dates}},
    {$or:[{"continent":"Europe"},{"continent":"North America"}]}]}},
    {$group:{_id:{groupByContinent: "$continent",groupByDate:"$date"}, sumNewCases:{$sum:"$new_cases"}}},
    {$project:{_id:1,groupByContinent:1,sumNewCases:1}}
    ])
    
//---------------------------------------------------------------

//---------------------------------------------------------------
//10. Generate a list of unique locations (countries) that have successfully flattened the curve (i.e., achieved more than 14 days of 0 new cases, after recording more than 50 cases)
//For question 10 and 11, we restructured the owid-covid-data JSON file to add some additional columns
db.getCollection("owid-covid-data").aggregate([
    {$match:{$and:[{total_cases:{$gte:50}}, {Consecutive0Cases:{$gte:14}}]}},
    {$group: {_id:{groupByLocation:"$location"}, maxDays:{$max:"$Consecutive0Cases"}}},
    {$project:{_id:1, maxDays:1}},
    {$sort:{_id:1}}
    ])
//---------------------------------------------------------------

//---------------------------------------------------------------
//11. Second wave detection – generate a list of unique locations (countries) that have flattened the curve (as defined above) but suffered upticks in new cases (i.e., after >= 14 days, registered more than 50 cases in a subsequent 7-day window)
db.getCollection("owid-covid-data").aggregate([
    {$match:{$and:[{Consecutive0Cases:{$gt:14}},{total_cases:{$gte:50}},{total_cases_7_days:{$gt:50}}]}},
    {$group:{_id:{groupByLocation:"$location"}, maxDays:{$max:"$Consecutive0Cases"}}},
    {$project:{_id:1}}
    ]).sort({_id:1})
//---------------------------------------------------------------

//---------------------------------------------------------------
//12. Display the top 3 countries in terms of changes from baseline in each of the place categories (i.e., grocery and pharmacy, parks, transit stations, retail and recreation, residential, and workplaces)
//retail_and_recreation_percent_change_from_baseline
db.global_mobility_report.aggregate([
    {$match:{$and:[{"sub_region_1":{$eq:""}},{"sub_region_2":{$eq:""}}]}},
    {$group : {
       _id : {groupByCountry:"$country_region"},
       ave: { $avg: {$abs: {$convert:{input:"$retail_and_recreation_percent_change_from_baseline",to:"int",onError:0}}} }
    }}
    ]).sort({ave:-1}).limit(3);

//grocery_and_pharmacy_percent_change_from_baseline
db.global_mobility_report.aggregate([
    {$match:{$and:[{"sub_region_1":{$eq:""}},{"sub_region_2":{$eq:""}}]}},
    {$group : {
       _id : {groupByCountry:"$country_region"},
       ave: { $avg: {$abs: {$convert:{input:"$grocery_and_pharmacy_percent_change_from_baseline",to:"int",onError:0}}} }
    }}
    ]).sort({ave:-1}).limit(3);
    
//parks_percent_change_from_baseline
db.global_mobility_report.aggregate([
    {$match:{$and:[{"sub_region_1":{$eq:""}},{"sub_region_2":{$eq:""}}]}},
    {$group : {
       _id : {groupByCountry:"$country_region"},
       ave: { $avg: {$abs: {$convert:{input:"$parks_percent_change_from_baseline",to:"int",onError:0}}} }
    }}
    ]).sort({ave:-1}).limit(3);
    
//transit_stations_percent_change_from_baseline
db.global_mobility_report.aggregate([
    {$match:{$and:[{"sub_region_1":{$eq:""}},{"sub_region_2":{$eq:""}}]}},
    {$group : {
       _id : {groupByCountry:"$country_region"},
       ave: { $avg: {$abs: {$convert:{input:"$transit_stations_percent_change_from_baseline",to:"int",onError:0}}} }
    }}
    ]).sort({ave:-1}).limit(3);
    
//workplaces_percent_change_from_baseline
db.global_mobility_report.aggregate([
    {$match:{$and:[{"sub_region_1":{$eq:""}},{"sub_region_2":{$eq:""}}]}},
    {$group : {
       _id : {groupByCountry:"$country_region"},
       ave: { $avg: {$abs: {$convert:{input:"$workplaces_percent_change_from_baseline",to:"int",onError:0}}} }
    }}
    ]).sort({ave:-1}).limit(3);

//residential_percent_change_from_baseline
db.global_mobility_report.aggregate([
    {$match:{$and:[{"sub_region_1":{$eq:""}},{"sub_region_2":{$eq:""}}]}},
    {$group : {
       _id : {groupByCountry:"$country_region"},
       ave: { $avg: {$abs: {$convert:{input:"$residential_percent_change_from_baseline",to:"int",onError:0}}} }
    }}
    ]).sort({ave:-1}).limit(3);
//---------------------------------------------------------------

//---------------------------------------------------------------
//13. Conduct mobility trend analysis, i.e., in Indonesia, identify the date where more than 20,000 cases were recorded (D-day). Based on D-day, show the daily changes in mobility trends for the 3 place categories (i.e., retail and recreation, workplaces, and grocery and pharmacy).
var dday = db.getCollection("owid-covid-data").aggregate([
    {$match:{$and:[{"location":{$eq:"Indonesia"}},{"total_cases":{$gt:20000}}]}},
    {$project:{date:1}}
    ]).map(function (doc){ return doc.date; });

db.global_mobility_report.aggregate([
    {$match:{$and:[{"country_region":{$eq:"Indonesia"}},{"date": {"$in":dday}},{"sub_region_1":{$eq:""}}]}},
    {$project:{date:1, country_region:1, retail_and_recreation_percent_change_from_baseline:1, grocery_and_pharmacy_percent_change_from_baseline:1, workplaces_percent_change_from_baseline:1}}
    ]).sort({date:1});
//---------------------------------------------------------------