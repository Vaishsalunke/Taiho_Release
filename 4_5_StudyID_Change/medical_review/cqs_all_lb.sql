/*
      Study: All Studies
      Table: LB
*/
--DROP TABLE IF EXISTS "medical_review"."cqs_all_lb";

--CREATE TABLE "medical_review"."cqs_all_lb" AS

select
	comprehendid,
	studyid,
	siteid,
	usubjid,
	visit,
	lbdtc,
	lbdy,
	lbseq,
	lbtestcd,
	lbtest,
	lbcat,
	case 
	when lower(lbscat) like '%tas120-201%' then 'TAS120-201'
	when lower(lbscat) like '%fulvestrant%' then 'Fulvestrant'
	when lower(lbscat) like '%tas0612%' then 'TAS0612'
	when lower(lbscat) like '%tas3681%' then 'TAS3681'
	when lower(lbscat) like '%tas120_202%' then 'TAS120_202'
	else lbscat end as lbscat,
	lbspec,
	lbmethod,
	lborres,
	lbstat,
	lbreasnd,
	lbstnrlo,
	lbstnrhi,
	lborresu,
	lbstresn,
	lbstresu,
	lbtm,
	lbblfl,
	lbnrind,
	lbornrhi,
	lbornrlo,
	lbstresc,
	lbenint,
	lbevlint,
	lblat,
	lblloq,
	lbloc,
	lbpos,
	lbstint,
	lbuloq,
	lbclsig,
	objectuniquekey,
	sitecountrycode,
	sitename,
	sitecountry,
	studyname,
	siteregion
from
	cqs.lb
	
--ALTER SCHEMA "medical_review" OWNER TO "taiho-stage-app";

--ALTER TABLE "medical_review"."cqs_all_ae" OWNER TO "taiho-stage-app-clinical-master-write";