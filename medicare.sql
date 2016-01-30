--/* Hive DDL Statements */
--/*Define Medicare Database*/
CREATE DATABASE medicare;
use medicare;
--/*Add my JAR file to Hive Library and define UDFs*/
add jar myhiveudfs.jar;
CREATE TEMPORARY FUNCTION premcalc as 'com.rama.myhiveudfs.PremCalcUDF';
CREATE TEMPORARY FUNCTION copaycalc as 'com.rama.myhiveudfs.CopayCalcUDF';
CREATE TEMPORARY FUNCTION coinscalc as 'com.rama.myhiveudfs.CoinsCalcUDF';
--/*Set Hive Optimization criterias*/
set hive.enforce.bucketing=true;
set hive.optimize.bucketmapjoin=true;
set hive.exec.parallel=true;
--/* Load Star Rating summary table from Pig into Hive */
CREATE TABLE starratetable 
(ContractId  STRING, summary_score STRING,  star_rating_Current STRING, star_rating_previous STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/medicareplan/starsummary';
--/* Load County table from Pig into Hive */
CREATE TABLE countytable 
(contract_id  STRING, plan_id  STRING,  segment_id  STRING,  org_name  STRING,  plan_name STRING,
 plan_type STRING, countyFIPcode STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/medicareplan/county';
--/* Load County table with bucket clustering using the field CountyFIPcode: from Pig into Hive */
CREATE TABLE countybuckettable
(contract_id  STRING, plan_id  STRING,  segment_id  STRING,  org_name  STRING,  plan_name STRING,
 plan_type STRING, countyFIPcode STRING)
CLUSTERED BY (CountyFIPcode) INTO 64 BUCKETS
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/medicareplan/countybucket';
--/* Load planservice table from Pig into Hive */
CREATE TABLE planservicetable 
(contractid  STRING,   planid  STRING,  segmentid  STRING,   category  STRING, categoryCode STRING, 
 benefit STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/medicareplan/planservice';
--/* Load countynames table from Pig into Hive */
CREATE TABLE countynamestable 
(FIPScode STRING,   countyname STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/medicareplan/countynames';
--/*Hive DML Statements*/
--/* 1. Identify top 5 plans with lowest premiums for a given county across the  US*/
SELECT  CountyFIPcode,org_name, plan_name,max(premcalc(benefit)) as prem
FROM  countytable 
JOIN planservicetable 
ON countytable.contract_id  = planservicetable.contractid  AND countytable.plan_id = planservicetable.planid
WHERE CategoryCode="1" AND (instr(benefit,"monthly premium")>0 OR instr(benefit,"per month")>0  OR instr(benefit,"per year")>0)
AND countyFIPcode = "97701"
GROUP BY countyFIPcode,org_name, plan_name
SORT BY prem
LIMIT 5;
--/*2.  To find plans that have highest co-pays for doctors in a given county*/
SELECT  countyFIPcode,org_name, plan_name, max(copaycalc(benefit)) as copay
FROM  countytable 
JOIN planservicetable 
ON countytable.contract_id  = planservicetable.contractid  AND countytable.plan_id = planservicetable.planid
WHERE CategoryCode="10" AND countyFIPcode = "97701"
GROUP BY countyFIPcode,org_name, plan_name
ORDER BY copay
LIMIT 5;
--/*3. To compare plans based on features like plans that offer free ambulance services*/
SELECT  countyFIPcode,org_name, plan_name
from countytable 
Join planservicetable 
ON countytable.contract_id  = planservicetable.contractid  AND countytable.plan_id = planservicetable.planid
where CategoryCode="5"  AND copaycalc(benefit)="0"
GROUP BY countyFIPcode,org_name, plan_name
ORDER BY countyFIPcode
LIMIT 5;
--/*4. To compare plans based on features like the benefits available for diabetes under specific plan*/
SELECT  countyFIPcode,org_name, plan_name
from countytable 
Join planservicetable 
ON countytable.contract_id  = planservicetable.contractid  AND countytable.plan_id = planservicetable.planid
where countyFIPcode = "25003" and (CategoryCode="8"  OR instr(benefit,"diabetes")>0) 
GROUP BY countyFIPcode,org_name, plan_name
ORDER BY countyFIPcode
LIMIT 5;
--/*5. To compare plan benefits on diabetes and mental healthcare offered by all companies in a particular*/ ----county
SELECT  countyFIPcode,org_name, plan_name
from countytable 
Join planservicetable 
ON countytable.contract_id  = planservicetable.contractid  AND countytable.plan_id = planservicetable.planid
where countyFIPcode = "25003" and (CategoryCode="8" OR CategoryCode="16" OR instr(benefit,"diabetes")>0 OR instr(benefit,"mental")>0)
GROUP BY countyFIPcode,org_name, plan_name
ORDER BY countyFIPcode
LIMIT 5;
--/*6. To find out plans provided by organizations having maximum twitter presence*/;
--/* Add serde jar to hive*/;
add jar hive-serdes-1.0-SNAPSHOT.jar;
set hive.support.sql11.reserved.keywords=false;
--/*Create Hive Table Structure for twitter messages*/;
CREATE EXTERNAL TABLE tweets (
id BIGINT,
created_at STRING,
source STRING,
favorited BOOLEAN,
retweeted_status STRUCT<
text:STRING,
user:STRUCT<screen_name:STRING,name:STRING>,
retweet_count:INT>,
entities STRUCT<
urls:ARRAY<STRUCT<expanded_url:STRING>>,
user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
hashtags:ARRAY<STRUCT<text:STRING>>>,
text STRING,
user STRUCT<
screen_name:STRING,
name:STRING,
friends_count:INT,
followers_count:INT,
statuses_count:INT,
verified:BOOLEAN,
utc_offset:INT,
time_zone:STRING>,
in_reply_to_screen_name STRING )
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/twitter/twitter';
--/* Create a Hive Table for Plan Provider Names*/
CREATE table words (word STRING);
INSERT INTO words
VALUES
('Westmoreland'),('WellCare'),('VillageCareMAX'),('SummaCare'),('StayWell'),('SelectCare'),('PrimeWest'),('PacificSource'),('OneCare'),('MedStar'),('KelseyCare'),('InovaCares'),('lliniCare'),('IEHP'),('HUMANA'),('HealthSun'),('HealthSpring'),('HealthPlus'),('Healthfirst'),('GLOBALHEALTH'),('CDPHP'),('CareSource'),('CarePlus'),('CarePartners'),('CareOregon'),('CareMore'),('Care1st'),('BlueCross'),('BlueChoice'),('BlueCare'),('ArchCare'),('AmeriHealth'),('Amerigroup Healthcare'),('AltaMed'),('AlphaCare'),('AllCare'),('Aetna'),('Advicare');
--/*Create a Hive Table and insert the twitter message text field from table tweets*/
CREATE table tweettexts (texts STRING);
INSERT INTO tweettexts
SELECT  text FROM  tweets;
--/*Create a Hive Table and insert the Plan provider names and the number of text having this name in the */ --/*message text field from table tweets*/
CREATE table tweetpresence(Planprovider STRING, presence DOUBLE);
INSERT INTO  tweetpresence
SELECT  word, COUNT(texts) FROM  words, tweettexts WHERE instr(texts,word)>0
GROUP BY word; 
--/* Select 5 plan providers having highest reference in the twitter messages*/
SELECT  Planprovider, presence FROM  tweetpresence ORDER BY presence desc limit 5;
