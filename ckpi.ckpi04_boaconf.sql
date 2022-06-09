create table ckpi.ckpi04_boaconf as 
select * from (
select *, 
case 
when overall_response = 'CR'  and 
next_overall_response = 'CR' and 
other_cond >= 4 then 'CR'
when overall_response = 'PR' and 
(next_overall_response = 'CR' or next_overall_response = 'PR') and 
other_cond >= 4 then 'PR'
when overall_response = 'PR' and 
(next_overall_response = 'SD' and next_to_next_overall_response = 'PR') and
other_cond >= 4 then 'PR'
end as overall_response_confirmed
from(
select *,
round((extract(days from  next_date_of_overall_response_confirmed - date_of_overall_response_confirmed))/7)::int as other_cond
from (
select  distinct 'TAS120_202' as study,
o."SiteNumber" as sitenumber,
o."Subject" as subject,
o."ORDAT" as date_of_overall_response_confirmed,
lead(o."ORDAT",1) over (partition by o."SiteNumber",o."Subject" order by o."ORDAT") as next_date_of_overall_response_confirmed,
o."ORRES" as overall_response,
lead(o."ORRES",1) over (partition by o."SiteNumber",o."Subject" order by o."ORDAT") as next_overall_response,
lead(o."ORRES",2) over (partition by o."SiteNumber",o."Subject" order by o."ORDAT") as next_to_next_overall_response,
d."DMCOH" as cohort
from tas120_202."OR1" o
left join tas120_202."DM" d on 
o."SiteNumber" = d."SiteNumber" and
o."Subject" = d."Subject")a
)b
)c
--where overall_response_confirmed in ('CR', 'PR')

--ALTER TABLE ckpi.ckpi04_boaconf OWNER TO "taiho-dev-app-clinical-master-write";