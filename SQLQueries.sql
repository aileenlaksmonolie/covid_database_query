#===============================================================
#BC2402 Designing & Developing Databases
#Purpose: SQL Queries
#Seminar: SEM1
#Group: Grp8
#Author: Angel Han Feng Yi, Aileen Laksmono, Andrew Imanuel, Julius Daniel Sarwono, Han Xiao
#Supervising Tutor: Ben Choi
#===============================================================

#---------------------------------------------------------------
#1.	Generate a list of unique locations (countries) in Asia
SELECT distinct(location) FROM `owid-covid-data`
where continent="Asia";
#---------------------------------------------------------------

#---------------------------------------------------------------
#2.	Generate a list of unique locations (countries) in Asia and Europe, with more than 10 total cases on 2020-04-01
SELECT distinct(location) FROM `owid-covid-data`
where continent="Asia" or continent="Europe"
and date = "2020-04-01" and total_cases>10;
#---------------------------------------------------------------

#---------------------------------------------------------------
#3.	Generate a list of unique locations (countries) in Africa, with less than 10,000 total cases between 2020-04-01 and 2020-04-20 (inclusive of the start date and end date)
SELECT distinct(location) FROM `owid-covid-data`
where continent="Africa"
and date between "2020-04-01" and "2020-04-20" 
and total_cases<10000;
#---------------------------------------------------------------

#---------------------------------------------------------------
#4.	Generate a list of unique locations (countries) without any data on total tests
#To prove that there are no 0 in total_tests column
select total_tests
FROM `owid-covid-data`
where total_tests=0;
#no 0 values, only null

SELECT DISTINCT location, sum(total_tests) AS total_tests
FROM `owid-covid-data`
GROUP BY location
HAVING total_tests = "" and sum(total_tests) = 0; 
#---------------------------------------------------------------

#---------------------------------------------------------------
#5.	Conduct trend analysis, i.e., for each month, compute the total number of new cases globally. 
select year(date) as Year, month(date) as Month, sum(new_cases) as TotalCases
from `owid-covid-data`
where continent != ""
group by year(date), month(date);
#---------------------------------------------------------------

#---------------------------------------------------------------
#6.	Conduct trend analysis, i.e., for each month, compute the total number of new cases in each continent
select continent, year(date) as Year, month(date) as Month, sum(new_cases) as Cases
from `owid-covid-data`
where continent != ""
group by continent, year(date), month(date)
order by continent;
#---------------------------------------------------------------

#---------------------------------------------------------------
#7.	Generate a list of EU countries that have implemented mask related responses (i.e., response measure contains the word “mask”).
select distinct(Country)
from `response_graphs_2020-08-13` a, `owid-covid-data` b
where a.Country = b.location and b.continent= 'Europe' 
and a.Response_measure like '%Mask%' or a.Response_measure like '%mask%';
#---------------------------------------------------------------

#---------------------------------------------------------------
#8.	Compute the period (i.e., start date and end date) in which most EU countries has implemented MasksMandatory as the response measure. For NA values, use 1-Auguest 2020.
#To create temporary table
CREATE TABLE temp_table (
	Country varchar(255),
    date_start varchar(255),
	date_end varchar(255),
    number varchar(255)
);

#Insert data into the temporary table
#Contains countries' info that implements maskmandatory.
insert INTO temp_table
SELECT distinct(COUNTRY), date_start, replace(date_end, 'NA', '2020-08-01') as date_end, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS number
from `response_graphs_2020-08-13`
where Response_measure = "MasksMandatory" 
and Country in (select distinct(location)
from `owid-covid-data`
where continent = "Europe");

#Final Result is stored here
CREATE TABLE MaskMandatory_countries (
    DATEVAR varchar(255),
    COUNT_COUNTRIES varchar(255)
);

DELIMITER $$
CREATE PROCEDURE collectDate()
BEGIN
	DECLARE finished INTEGER DEFAULT 0;
	DECLARE counter varchar(255);

	-- declare cursor for countries
	DECLARE counuter_from_table 
		CURSOR FOR 
			select number from temp_table;

	-- declare NOT FOUND handler
	DECLARE CONTINUE HANDLER 
		FOR NOT FOUND SET finished = 1;

	OPEN counuter_from_table;

	getDates: LOOP
		FETCH counuter_from_table INTO counter;	
        #expand date from date ranges 
		INSERT INTO MaskMandatory_countries (DATEVAR, COUNT_COUNTRIES) #append each countries' expanded date ranges into TABLE5 (consolidatees all countries' data)
		SELECT Date, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS number
		from (
			select curdate() - INTERVAL (a.a + (10 * b.a) + (100 * c.a) + (1000 * d.a) ) DAY as Date, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS number
			from (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as a
			cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as b
			cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as c
			cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as d
		) a
		where a.Date between (#start date
		select date_start
		from temp_table
        where number = counter)
		and
		( #end date
		select date_end
        from temp_table
        where number = counter)
		group by a.Date;

		IF finished = 1 THEN 
			LEAVE getDates;
		
		END IF;
        
	END LOOP getDates;
	CLOSE counuter_from_table;
    
END$$
DELIMITER ;

CALL collectDate(); 

#Query to see the final table
SELECT DATEVAR, COUNT(COUNT_COUNTRIES) AS NoOfCountries
FROM MaskMandatory_countries
group by DATEVAR
HAVING NoOfCountries =
(SELECT MAX(C)
FROM(
SELECT DATEVAR, COUNT(COUNT_COUNTRIES) AS C
FROM MaskMandatory_countries
group by DATEVAR)T1);
#---------------------------------------------------------------

#---------------------------------------------------------------
#9.	Based on the period above, conduct trend analysis for Europe and North America, i.e., for each day during the period, compute the total number of new cases.

#Non-hard coded answer
select date, continent , sum(new_cases) as total_new_cases
from`owid-covid-data`
where (continent = 'Europe' or continent = 'North America')
and date between (SELECT MIN(DATEVAR) FROM(
SELECT DATEVAR, COUNT(COUNT_COUNTRIES) AS NoOfCountries
FROM MaskMandatory_countries
group by DATEVAR
HAVING NoOfCountries =
(SELECT MAX(C)
FROM(
SELECT DATEVAR, COUNT(COUNT_COUNTRIES) AS C
FROM MaskMandatory_countries
group by DATEVAR)T1))T2) and (SELECT MAX(DATEVAR) FROM(
SELECT DATEVAR, COUNT(COUNT_COUNTRIES) AS NoOfCountries
FROM MaskMandatory_countries
group by DATEVAR
HAVING NoOfCountries =
(SELECT MAX(C)
FROM(
SELECT DATEVAR, COUNT(COUNT_COUNTRIES) AS C
FROM MaskMandatory_countries
group by DATEVAR)T1))T2)
group by continent, date;

#Hard-coded answer
select date, continent , sum(new_cases) as total_new_cases
from`owid-covid-data`
where (continent = 'Europe' or continent = 'North America')
and date between '2020-07-27' and '2020-08-01'
group by continent, date;

#---------------------------------------------------------------

#---------------------------------------------------------------
#10. Generate a list of unique locations (countries) that have successfully flattened the curve (i.e., achieved more than 14 days of 0 new cases, after recording more than 50 cases)
#To prove that missing dates will not affect the query result as the gap is not significant
select location,min(date),max(date),datediff(max(date),min(date))+1 as d1, count(date) as d2
from `owid-covid-data`
group by location;

select location, d1, d2
from (select location,min(date),max(date),datediff(max(date),min(date))+1 as d1, count(date) as d2
from `owid-covid-data`
where total_cases>=50
group by location) as m
where m.d1 != m.d2;

#To generate the list using iteration
set @test = 0, @loc=0, @count=0;

select m.loc, max(count) as consecutive_rows
from (
select 
 @count := if(new_cases = 0 and location = @loc, @count+1, 0) as count,
 @test := new_cases,
 @loc := location as loc, date
from `owid-covid-data` where total_cases>=50) as m
group by m.loc
having consecutive_rows > 14;
#---------------------------------------------------------------

#---------------------------------------------------------------
#11. Second wave detection – generate a list of unique locations (countries) that have flattened the curve (as defined above) but suffered upticks in new cases (i.e., after >= 14 days, registered more than 50 cases in a subsequent 7-day window)

CREATE TABLE qn11 (
	start_date varchar(255),
    end_date varchar(255),
	NumberOfConseqDays varchar(255),
    Country varchar(255)
);

insert into qn11
select DATE_ADD(m2.max_date, INTERVAL 1 DAY) as start_date, DATE_ADD(m2.max_date, INTERVAL 8 DAY) as end_date,m2.NumberOfConseqDays, m2.Country 
from(
select max(m1.d) as max_date,max(m1.count) as NumberOfConseqDays ,m1.id as Country
from(
select 
 @count := if(new_cases = 0 and location = @id, @count+1, 0) as count,
 @test := new_cases,
 @id := location as id,
 date as d
from covid_world.`owid-covid-data` where total_cases>=50)m1
where m1.count >=14  	
group by m1.id) m2;

select Country
from (select Country, sum(new_cases) as total_cases_7_days
from qn11 a, `owid-covid-data` b
where a.Country = b.Location and b.date between a.start_date and a.end_date
group by Country
having total_cases_7_days>50) as qn10_answer;
#---------------------------------------------------------------

#---------------------------------------------------------------
#12. Display the top 3 countries in terms of changes from baseline in each of the place categories (i.e., grocery and pharmacy, parks, transit stations, retail and recreation, residential, and workplaces)
#retail_and_recreation_percent_change_from_baseline
select (country_region), avg(abs(retail_and_recreation_percent_change_from_baseline)) retail_and_recreation
FROM global_mobility_report
WHERE sub_region_1 = "" and sub_region_2 = ""
group by country_region
order by retail_and_recreation desc
limit 3;

#grocery_and_pharmacy_percent_change_from_baseline
select (country_region), avg(abs(grocery_and_pharmacy_percent_change_from_baseline)) groceryAndPharmacyChange
FROM global_mobility_report
WHERE sub_region_1 = "" and sub_region_2 = ""
group by country_region
order by groceryAndPharmacyChange desc
limit 3;

#parks_percent_change_from_baseline
select (country_region), avg(abs(parks_percent_change_from_baseline)) parksChange
FROM global_mobility_report
WHERE sub_region_1 = "" and sub_region_2 = ""
group by country_region
order by parksChange desc
limit 3;

#transit_stations_percent_change_from_baseline
select (country_region), avg(abs(transit_stations_percent_change_from_baseline)) transitStationsChange
FROM global_mobility_report
WHERE sub_region_1 = "" and sub_region_2 = ""
group by country_region
order by transitStationsChange desc
limit 3;

#workplaces_percent_change_from_baseline
select (country_region), avg(abs(workplaces_percent_change_from_baseline)) workplacesChange
FROM global_mobility_report
WHERE sub_region_1 = "" and sub_region_2 = ""
group by country_region
order by workplacesChange desc
limit 3;

#residential_percent_change_from_baseline
select (country_region), avg(abs(residential_percent_change_from_baseline)) residentialChange
FROM global_mobility_report
WHERE sub_region_1 = "" and sub_region_2 = ""
group by country_region
order by residentialChange desc
limit 3;
#---------------------------------------------------------------

#---------------------------------------------------------------
#13. Conduct mobility trend analysis, i.e., in Indonesia, identify the date where more than 20,000 cases were recorded (D-day). Based on D-day, show the daily changes in mobility trends for the 3 place categories (i.e., retail and recreation, workplaces, and grocery and pharmacy).
select date, country_region, retail_and_recreation_percent_change_from_baseline, grocery_and_pharmacy_percent_change_from_baseline,workplaces_percent_change_from_baseline
from global_mobility_report
where country_region = "Indonesia" and sub_region_1 = "" and date in
	(select date
	from `owid-covid-data`
	where location ='Indonesia' and total_cases >20000)
order by date;
#---------------------------------------------------------------
