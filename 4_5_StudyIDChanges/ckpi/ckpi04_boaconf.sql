/*Mapping script for AE Tracking
Table name : ckpi.cKPI03_CD
Project name : Taiho*/

drop table if exists ckpi.ckpi04_boaconf_new;

create table ckpi.ckpi04_boaconf_new
as 
select * from (
select *, 
count(overall_response_confirmed) over (partition by study, sitenumber, subject) as count_overall_response_confirmed
from (
select *, 
case 
when overall_response = 'CR'  and 
next_overall_response = 'CR' and 
date_weeks >= 4 then 'CR'
when overall_response = 'PR' and 
(next_overall_response = 'CR' or next_overall_response = 'PR') and 
date_weeks >= 4 then 'PR'
when overall_response = 'PR' and 
(next_overall_response = 'SD' and next_to_next_overall_response = 'PR') and
date_weeks >= 4 then 'PR'
end as overall_response_confirmed
from(
select d.*,
round((extract(days from  next_date_of_overall_response_confirmed - date_of_overall_response_confirmed))/7)::int as date_weeks,
extract(days from  next_date_of_overall_response_confirmed - date_of_overall_response_confirmed)::int as date_days
from (
select a.*,
lead(date_of_overall_response_confirmed,1) over (partition by sitenumber,subject order by date_of_overall_response_confirmed) as next_date_of_overall_response_confirmed,
lead(overall_response,1) over (partition by sitenumber,subject order by date_of_overall_response_confirmed) as next_overall_response,
lead(overall_response,2) over (partition by sitenumber,subject order by date_of_overall_response_confirmed) as next_to_next_overall_response
from 
(select  distinct 'TAS-120-202' as study,
o."SiteNumber" as sitenumber,
o."Subject" as subject,
o."ORDAT" as date_of_overall_response_confirmed,
o."ORRES" as overall_response,
d."DMCOH" as cohort
from tas120_202."OR1" o
left join (SELECT "SiteNumber", "Subject", "DMCOH" FROM tas120_202."DM" WHERE concat("SiteNumber","Subject","RecordPosition") IN  
(SELECT concat("SiteNumber","Subject",min("RecordPosition"))FROM tas120_202."DM" GROUP BY "SiteNumber","Subject")) d on 
o."SiteNumber" = d."SiteNumber" and
o."Subject" = d."Subject"
)a )d
)b
)c
)e 
where count_overall_response_confirmed > 0 ;

drop table if exists ckpi.ckpi04_boaconf_orig;

alter table if exists ckpi.ckpi04_boaconf rename to ckpi04_boaconf_orig;

alter table if exists ckpi.ckpi04_boaconf_new rename to ckpi04_boaconf;	
	
--ALTER TABLE ckpi.ckpi04_boaconf OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE ckpi.ckpi04_boaconf_orig OWNER TO "taiho-stage-app-clinical-master-write";	


