/*Mapping script for AE Tracking
Table name : ckpi.cKPI03_CD
Project name : Taiho*/

drop table if exists ckpi.cKPI03_CD_new;

create table ckpi.cKPI03_CD_new as 
with data_612 as 
(select "project" as studyid, 
        case when length(trim("SiteNumber")) = 11 then replace("SiteNumber",'_','_101_') 
        else trim("SiteNumber")
        end
        as siteid,
        "Subject" as usubjid,
	case when "CDTYPE" = 'Other' then "CDTYPE"||' : '||"CDTYOTH" else "CDTYPE" end as cdtype,
	null as fgfrtype,
	null as cdhisto
from
	tas0612_101."CD"
),

data_201 as 
(select "project" as studyid, "SiteNumber" as siteid,"Subject" as usubjid,
	"CDTYPE" as cdtype,
	"FGFRTYPE" as fgfrtype,
	null as cdhisto
 from
	tas120_201."CD"
),

data_202 as 
(
select distinct a."project" as studyid, a."SiteNumber" as siteid,a."Subject" as usubjid,a.cdtype, a.cdhisto ,fg."FGFRRECP" as fgfrtype from(
select "project", "SiteNumber","Subject",
	case when "CDTYPE" = 'Other' then "CDTYPE"||' : '||"CDTYOTH" else "CDTYPE" end as cdtype,
	case when "CDHISTO" = 'Other' then "CDHISTO"||' : '||"CDHISOTH" else "CDHISTO" end as cdhisto
  from tas120_202."CD"
	
union all

select "project" as studyid, "SiteNumber" as siteid,"Subject" as usubjid,
case when "CDTYPE" = 'Other' then "CDTYPE"||' : '||"CDTYOTH" else "CDTYPE" end as cdtype,
case when "CDHISTO1" = 'Other' then "CDHISTO1"||' : '||"CDHISTOTH" else "CDHISTO1" end as cdhisto
from tas120_202."CD1"
)a
left join tas120_202."FGFR" fg
on a."project" = fg."project" and 
a."SiteNumber" = fg."SiteNumber" and
a."Subject" = fg."Subject"
),

data_3681_DOSE_ESC as (
select 
'TAS3681_101_DOSE_ESC' as studyid, "SiteNumber" as siteid,"Subject" as usubjid,
"CDTYPE" as cdtype
,case when "CDHISTO" = 'Other' then "CDHISTO"||' : '||"CDHISOTH" else "CDHISTO" end as cdhisto
, null as fgfrtype
from tas3681_101."CD"
where "Subject" in (select usubjid from cqs.subject where studyid='TAS3681_101_DOSE_ESC')
),

data_3681_DOSE_EXP as (
select 
'TAS3681_101_DOSE_EXP' as studyid, "SiteNumber" as siteid,"Subject" as usubjid,
"CDTYPE" as cdtype
,case when "CDHISTO" = 'Other' then "CDHISTO"||' : '||"CDHISOTH" else "CDHISTO" end as cdhisto
, null as fgfrtype
from tas3681_101."CD"
where "Subject" in (select usubjid from cqs.subject where studyid='TAS3681_101_DOSE_EXP')
),


 data_tas117_201 as (
select 
'TAS117_201' as studyid, 
concat('TAS117_201_',split_part("SiteNumber",'_',2)) as siteid,
"Subject" as usubjid,
case when "CDPTDG" = 'Other' then "CDPTDG"||' : '||"CDPTDGOT" else "CDPTDG" end as cdtype,
case when "CDHISTO" = 'Other' then "CDHISTO"||' : '||"CDHISTOT" else "CDHISTO" end as cdhisto
, null as fgfrtype
from tas117_201."CD"
),

data_120_203 as 
(
select a."project"as studyid, 
concat('TAS120_203_',split_part(a."SiteNumber",'_',2)) as siteid, 
a."Subject" as usubjid,
	case when "CDTUMLOC" = 'Other' then "CDTUMLOC"||' : '||"CDSUBOTH" else "CDTUMLOC" end as cdtype,
	--case when "CDHISTO" = 'Other' then "CDHISTO"||' : '||"CDHISOTH" else "CDHISTO" end as cdhisto
	null as cdhisto,
	fg."FGFRTEST" as fgfrtype
from tas120_203."CD" a
left join tas120_203."FGFR" fg
on a."project" = fg."project" and 
a."SiteNumber" = fg."SiteNumber" and
a."Subject" = fg."Subject"
),

 data_120_204 as (
select 
"project" as studyid, 
concat('TAS120_204_',split_part("SiteNumber",'_',2)) as siteid,
"Subject" as usubjid,
case when "CDTYPE" = 'Other' then "CDTYPE"||' : '||"CDTYOTH" else "CDTYPE" end as cdtype,
case when "CDHISTO" = 'Other' then "CDHISTO"||' : '||"CDHISOTH" else "CDHISTO" end as cdhisto
, null as fgfrtype
from tas120_204."CD"
),

 data_2940_101 as (
select 
"project" as studyid, 
concat('TAS2940_101_',split_part("SiteNumber",'_',2)) as siteid,
"Subject" as usubjid,
case when "CDTYPE" = 'Other' then "CDTYPE"||' : '||"CDTYOTH" else "CDTYPE" end as cdtype,
case when "CDHISTO" = 'Other' then "CDHISTO"||' : '||"CDHISOTH" else "CDHISTO" end as cdhisto
, null as fgfrtype
from tas2940_101."CD"
)

 
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_612
union all
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_201
union all
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_202
union all
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_3681_DOSE_ESC
union all 
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_3681_DOSE_EXP
union all 
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_tas117_201
union all 
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_120_203
union all 
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_120_204
union all 
select studyid,siteid,usubjid,cdtype,cdhisto,fgfrtype  from data_2940_101
 ;

drop table if exists ckpi.cKPI03_CD_orig;

alter table if exists ckpi.cKPI03_CD rename to cKPI03_CD_orig;

alter table if exists ckpi.cKPI03_CD_new rename to cKPI03_CD;	
	
--ALTER TABLE ckpi.cKPI03_CD OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE ckpi.cKPI03_CD_orig OWNER TO "taiho-stage-app-clinical-master-write";	

