/*Mapping script for Swimmer Plot
Table name : ckpi.ckpi07_swimp
Project name : Taiho*/

drop table if exists ckpi.ckpi07_swimp_new;

create table ckpi.ckpi07_swimp_new
as 
select a.*,
 (a.study || '~' || a.sitenumber || '~' || a.subject || '~' || a.seq)::text  as id
from (
select  distinct v."project" as study,
v."SiteNumber" as sitenumber,
v."Subject" as subject,
v."VISITDAT" as c1d1_date_of_visit,
row_number() over(partition by v."project" ,v."SiteNumber" ,v."Subject" order by v."VISITDAT" )::integer AS seq,
d."DMCOH" as cohort,
INITCAP(COALESCE (CASE WHEN c."CDTYPE" = 'Other' THEN c."CDTYOTH" ELSE c."CDTYPE" end, c1."CDCANTYP"))  AS cancer_diag,
coalesce(e."EOTLDDAT",current_date) as date_of_last_dose,
e."EOTDAT" date_of_trt_disc_met,
coalesce(nullif(e."EOTREAS",''),'Ongoing') as eot_reason,
e."EOTREAS" as eot_reason_raw,
--floor((extract(days from coalesce(e."EOTLDDAT",current_date) - v."VISITDAT")+1)/7)::int  as weeks_from_c1d1,
round(date_part('day',coalesce(e."EOTLDDAT",current_date) - v."VISITDAT")/7::float) :: int  as weeks_from_c1d1,
--date_part('week',coalesce(e."EOTLDDAT",current_date)) - date_part('week', v."VISITDAT") :: int  as weeks_from_c1d1,
--extract(days from coalesce(e."EOTLDDAT",current_date) - v."VISITDAT")::int as days_from_c1d1,
date_part('day', coalesce(e."EOTLDDAT",current_date) - v."VISITDAT")::int as days_from_c1d1,
COALESCE(dth."DTHDAT",dth."DTHDAT_INT") AS date_of_death,
--s."SFCNFDAT" AS last_date_of_surv,
coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") AS last_date_of_surv,
s."SFCAT" AS last_surv_stat,
case	WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is null AND coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") IS NOT null  
			THEN round((date_part('day',coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") - e."EOTLDDAT"))/7::float)::int 
			
		WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is not null
			THEN round((date_part('day',COALESCE(dth."DTHDAT",dth."DTHDAT_INT") - e."EOTLDDAT"))/7::float)::int 
END  as weeks_from_c1d1_surv_con,
--date_part('week', COALESCE(dth."DTHDAT",dth."DTHDAT_INT")) as a,
--date_part('week',e."EOTLDDAT") as b,
/*case	WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is null AND coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") IS NOT null  
			THEN date_part('week',coalesce(s."SFCNFDAT",s."SFCNFDAT_INT")) - date_part('week',e."EOTLDDAT")::int 
			
		WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is not null
			THEN date_part('week', COALESCE(dth."DTHDAT",dth."DTHDAT_INT")) - date_part('week',e."EOTLDDAT")::int 
END  as weeks_from_c1d1_surv_con,*/

case	WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is null AND coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") IS null 
			THEN round((date_part('day',current_date - e."EOTLDDAT"))/7::float)::int 
		WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is null AND coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") IS NOT NULL 
			THEN round((date_part('day',current_date - s."SFCNFDAT"))/7::float)::int 
END  as weeks_from_c1d1_surv_uncon,

/*case	WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is null AND coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") IS null 
			THEN date_part('week',current_date) - date_part('week',e."EOTLDDAT")::int 
		WHEN coalesce(nullif(e."EOTREAS",''),'Ongoing') != 'Ongoing' AND COALESCE(dth."DTHDAT",dth."DTHDAT_INT") is null AND coalesce(s."SFCNFDAT",s."SFCNFDAT_INT") IS NOT NULL 
			THEN date_part('week',current_date) - date_part('week',s."SFCNFDAT")::int 
END  as weeks_from_c1d1_surv_uncon,*/

or1."ORRES" AS overall_respnse,
or1."ORDAT" AS date_of_or
from tas120_202."VISIT2" v
left join tas120_202."EOT" e on 
v."SiteNumber" = e."SiteNumber" and
v."Subject" = e."Subject"
left join (SELECT "SiteNumber", "Subject", "DMCOH" FROM tas120_202."DM" WHERE concat("SiteNumber","Subject","RecordPosition") IN  
(SELECT concat("SiteNumber","Subject",min("RecordPosition"))FROM tas120_202."DM" GROUP BY "SiteNumber","Subject")) d on 
v."SiteNumber" = d."SiteNumber" and
v."Subject" = d."Subject"
LEFT JOIN tas120_202."CD" c on 
v."SiteNumber" = c."SiteNumber" and
v."Subject" = c."Subject"
LEFT JOIN tas120_202."CD1" c1 on 
v."SiteNumber" = c1."SiteNumber" and
v."Subject" = c1."Subject"
LEFT JOIN tas120_202."DTH" dth on 
v."SiteNumber" = dth."SiteNumber" and
v."Subject" = dth."Subject"
LEFT JOIN (SELECT "SiteNumber", "Subject", "SFCNFDAT","SFCNFDAT_INT", "SFCAT" FROM tas120_202."SF" WHERE concat("SiteNumber","Subject","SFCNFDAT") IN(SELECT concat("SiteNumber","Subject",max("SFCNFDAT"))FROM tas120_202."SF" GROUP BY "SiteNumber","Subject")) s on 
v."SiteNumber" = s."SiteNumber" and
v."Subject" = s."Subject"
LEFT JOIN tas120_202."OR1" or1 on 
v."SiteNumber" = or1."SiteNumber" and
v."Subject" = or1."Subject" AND 
or1."ORRES" in ('PR','CR')
where v."Folder" = 'CYC01D01'
and v."VISITDAT" is not null)a;


drop table if exists ckpi.ckpi07_swimp_orig;

alter table if exists ckpi.ckpi07_swimp rename to ckpi07_swimp_orig;

alter table if exists ckpi.ckpi07_swimp_new rename to ckpi07_swimp;	
	
--ALTER TABLE ckpi.ckpi07_swimp OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE ckpi.ckpi07_swimp_orig OWNER TO "taiho-dev-app-clinical-master-write";
