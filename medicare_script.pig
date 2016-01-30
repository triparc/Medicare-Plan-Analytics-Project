---register_piggybank jar and define a function name for CSVExcelStorage()
REGISTER /home/hadoop/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVExcelStorage();
----Load countyplan for PlanInfoCounty_FipsCodeMoreThan30000
PLAN1 = LOAD '/medicareplan/Plan1.csv' USING CSVLoader AS (contract_id:chararray, plan_id:chararray, segment_id:chararray, contract_year:chararray, org_name:chararray, plan_name:chararray, sp_plan_name:chararray,
geo_name:chararray, tax_stus_cd:chararray, tax_status_desc:chararray, sp_tax_status_desc:chararray,
plan_type:chararray, plan_type_desc:chararray, web_address:chararray, partd_wb_adr:chararray,
frmlry_wbst_adr:chararray, phrmcy_wbst_adr:chararray, fed_approval_status:chararray, sp_fed_approval_status:chararray, pos_available_flag:chararray, mail_ordr_avlblty:chararray, cvrg_gap_ofrd:chararray,
cvrg_gap_ind:chararray, cvrg_gap_desc:chararray, contract_important_note:chararray, 
sp_contract_important_note:chararray, plan_important_note:chararray, sp_plan_important_note:chararray,
segment_important_note:chararray, sp_segment_important_note:chararray, legal_entity_name:chararray,
trade_name:chararray, network_english:chararray, network_spanish:chararray, contact_person:chararray,
street_address:chararray, city:chararray, state_code:chararray, zip_code:chararray, email_prospective:chararray,
local_phone_prospective:chararray, tollfree_phone_prospective:chararray, local_tty_prospective:chararray,
tollfree_tty_prospective:chararray, email_current:chararray, local_phone_current:chararray,
tollfree_phone_current:chararray, local_tty_current:chararray, tollfree_tty_current:chararray,
contact_person_pd:chararray, street_address_pd:chararray, city_pd:chararray,
state_code_pd:chararray, zip_code_pd:chararray, email_prospective_pd:chararray, local_phone_prospective_pd:chararray, tollfree_phone_prospective_pd:chararray,
local_tty_prospective_pd:chararray, tollfree_tty_prospective_pd:chararray, email_current_pd:chararray,
local_phone_current_pd:chararray, tollfree_phone_current_pd:chararray, local_tty_current_pd:chararray,
tollfree_tty_current_pd:chararray, ma_pd_indicator:chararray, ppo_pd_indicator:chararray, snp_id:chararray,
snp_desc:chararray, sp_snp_desc:chararray, lis_100:chararray, lis_75:chararray, lis_50:chararray, lis_25:chararray,
regional_indicator:chararray, CountyFIPSCode:chararray); 
----Clean up the data using filter	
PC1 = FILTER PLAN1 BY contract_id != 'contract_id' AND contract_id !='' AND plan_id != '' AND segment_id != '' AND plan_name != '' AND CountyFIPSCode != '';
----Select required Fields
PL1 = FOREACH PC1 GENERATE contract_id, plan_id, segment_id, org_name, plan_name, plan_type,CountyFIPSCode;
----Perform the same functions for PlanInfoCounty_FipsCodeLessThan30000 data
PLAN2 = LOAD '/medicareplan/Plan2.csv' USING CSVLoader AS (contract_id:chararray, plan_id:chararray, segment_id:chararray, contract_year:chararray, org_name:chararray, plan_name:chararray, sp_plan_name:chararray,
geo_name:chararray, tax_stus_cd:chararray, tax_status_desc:chararray, sp_tax_status_desc:chararray,
plan_type:chararray, plan_type_desc:chararray, web_address:chararray, partd_wb_adr:chararray,
frmlry_wbst_adr:chararray, phrmcy_wbst_adr:chararray, fed_approval_status:chararray, sp_fed_approval_status:chararray, pos_available_flag:chararray, mail_ordr_avlblty:chararray, cvrg_gap_ofrd:chararray,
cvrg_gap_ind:chararray, cvrg_gap_desc:chararray, contract_important_note:chararray, 
sp_contract_important_note:chararray, plan_important_note:chararray, sp_plan_important_note:chararray,
segment_important_note:chararray, sp_segment_important_note:chararray, legal_entity_name:chararray,
trade_name:chararray, network_english:chararray, network_spanish:chararray, contact_person:chararray,
street_address:chararray, city:chararray, state_code:chararray, zip_code:chararray, email_prospective:chararray,
local_phone_prospective:chararray, tollfree_phone_prospective:chararray, local_tty_prospective:chararray,
tollfree_tty_prospective:chararray, email_current:chararray, local_phone_current:chararray,
tollfree_phone_current:chararray, local_tty_current:chararray, tollfree_tty_current:chararray,
contact_person_pd:chararray, street_address_pd:chararray, city_pd:chararray,
state_code_pd:chararray, zip_code_pd:chararray, email_prospective_pd:chararray, local_phone_prospective_pd:chararray, tollfree_phone_prospective_pd:chararray,
local_tty_prospective_pd:chararray, tollfree_tty_prospective_pd:chararray, email_current_pd:chararray,
local_phone_current_pd:chararray, tollfree_phone_current_pd:chararray, local_tty_current_pd:chararray,
tollfree_tty_current_pd:chararray, ma_pd_indicator:chararray, ppo_pd_indicator:chararray, snp_id:chararray,
snp_desc:chararray, sp_snp_desc:chararray, lis_100:chararray, lis_75:chararray, lis_50:chararray, lis_25:chararray,
regional_indicator:chararray, CountyFIPSCode:chararray); 

PC2 = FILTER PLAN2 BY contract_id != 'contract_id' AND contract_id !='' AND  plan_id != '' AND segment_id != '' AND plan_name != '' AND CountyFIPSCode != '';
PL2 = FOREACH PC2 GENERATE contract_id, plan_id, segment_id, org_name, plan_name, plan_type,CountyFIPSCode;

----Combine the two countyplan data using Union and then store the data on HDFS

PLAN  = UNION PL1, PL2;
STORE PLAN INTO '/medicareplan/countybucket' using PigStorage(',','-schema');

----Load Plan service data from vwPlanServices.csv
PLANSERVICE = LOAD '/medicareplan/vwplanservices.csv' USING CSVLoader AS 
(Language:chararray, Contract_Year:chararray, Contract_ID:chararray,
Plan_ID:chararray,
Segment_ID:chararray,
CategoryDescription:chararray, 
CategoryCode:chararray, Benefit:chararray );
PS_F = FILTER PLANSERVICE BY Language == 'English' AND Contract_ID != 'Contract_ID' AND Contract_ID !='' AND 
Plan_ID != '' AND Segment_ID != '';
PS= FOREACH PS_F GENERATE Contract_ID, Plan_ID, Segment_ID, 
CategoryDescription, CategoryCode,Benefit;
STORE PS INTO '/medicareplan/planservice' using PigStorage(',','-schema');
----Load county name data from vwGeography.csv
COUNTY_NAME = LOAD '/medicareplan/vwgeography.csv' USING CSVLoader AS 
(StateCode:chararray,	StateName:chararray,	CountyName:chararray,	FIPSCode:chararray, zip_code:chararray);
CNF = FILTER COUNTY_NAME BY StateCode!= 'StateCode' AND CountyName != '' AND FIPSCode != '';
CN = FOREACH CNF GENERATE FIPSCode, CountyName;
STORE CN INTO '/medicareplan/countynames' using PigStorage(',','-schema');
----Load Star Rating Summary data from vwStarRating_SummaryScores
STARRATE = LOAD '/medicareplan/vwstarsummary.csv' USING CSVLoader AS 
(Contract_ID:chararray,  Summary_Score:chararray, Star_Rating_Current:chararray, 
Star_Rating_Previous: chararray, lang_dscrptn: chararray);
SR_F = FILTER STARRATE BY lang_dscrptn == 'English' AND Contract_ID != 'Contract_ID' 
AND Contract_ID !='';
SR_B = FOREACH SR_F GENERATE Contract_ID, Summary_Score, Star_Rating_Current,Star_Rating_Previous;STORE SR_B INTO '/medicareplan/starsummary' using PigStorage(',','-schema');