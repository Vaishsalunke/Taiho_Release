/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
               
 max_eot as (
 select eot."project",
 concat('TAS0612_101_',split_part(eot."SiteNumber",'_',2)) as siteid,
eot."Subject",
max(eot."EOTDAT") as eotdat
from tas0612_101."EOT" eot
group by 1,2,3
 ),

ds_data AS (

----Disposition Event: All Subjects----

SELECT distinct
'TAS0612_101' ::text AS studyid,
concat('TAS0612_101_',site_id) ::text AS siteid,
subject_number ::text AS usubjid,
1.0::NUMERIC AS dsseq,
'All Subjects'::text AS dscat,
'All Subjects'::text AS dsterm,
null::DATE AS dsstdtc,
null::text AS dsscat
from tas0612_101_irt.subject s


/*
union all

--Disposition Event: Consented

SELECT  dm."project"::text AS studyid,
--dm."SiteNumber"::text AS siteid,
concat('TAS0612_101_',split_part(dm."SiteNumber",'_',2))::text AS siteid,
dm."Subject"::text AS usubjid,
2.0::NUMERIC AS dsseq,
'Consent'::text AS dscat,
'Consented'::text AS dsterm,
dm."DMICDAT"::DATE AS dsstdtc,
null::text AS dsscat
from tas0612_101."DM" dm
*/

union all

--Disposition Event: Screened

SELECT  distinct
'TAS0612_101'::text AS studyid,
concat('TAS0612_101_',site_id)::text AS siteid,
subject_number ::text AS usubjid,
1.3::NUMERIC AS dsseq,
'Screened'::text AS dscat,
'Screened'::text AS dsterm,
screening_date ::DATE AS dsstdtc,
null::text AS dsscat
from tas0612_101_irt.subject s4
where nullif (screening_date,'') notnull

union all

--Disposition Event: Failed Screen

select studyid, siteid, usubjid, dsseq, dscat, dsterm, dsstdtc, string_agg(dsscat,';') as dsscat from (
SELECT distinct 'TAS0612_101'::text AS studyid,
concat('TAS0612_101_',site_id)::text AS siteid,
subject_number ::text AS usubjid,
2.1::NUMERIC AS dsseq,
'Enrollment'::text AS dscat,
'Failed Screen'::text AS dsterm,
screen_fail_date ::DATE AS dsstdtc,
concat(i."IECAT",i."IETESTCD") ::text AS dsscat
from tas0612_101_irt.subject s1
left join tas0612_101."IE" i on ('TAS0612_101'=i.project and concat('TAS0612_101_',s1.site_id) = concat(split_part(i."SiteNumber",'_',1),'_101_',split_part(i."SiteNumber",'_',2)) and s1.subject_number = i."Subject")
where subject_status = 'Screen Failed')a
group by 1,2,3,4,5,6,7

union all

--Disposition Event: Enrollment

SELECT  distinct
'TAS0612_101' ::text AS studyid,
concat('TAS0612_101_',site_id)::text AS siteid,
subject_number ::text AS usubjid,
3.0::NUMERIC AS dsseq,
'Enrollment'::text AS dscat,
'Enrolled'::text AS dsterm,
enrollment_date ::DATE AS dsstdtc,
null::text AS dsscat  
from tas0612_101_irt.subject s2
where subject_status = 'Enrolled'

union all

--Disposition Event: Early EOT

SELECT distinct
'TAS0612_101'::text AS studyid,
concat('TAS0612_101_',site_id)::text AS siteid,
subject_number ::text AS usubjid,
4.01::NUMERIC AS dsseq,
'Treatment'::text AS dscat,
'Early EOT'::text AS dsterm,
end_of_treatment_date ::DATE AS dsstdtc,
eot."EOTREAS"::text AS dsscat  
from tas0612_101_irt.subject s3
left join tas0612_101."EOT" eot on ('TAS0612_101'=eot.project and concat('TAS0612_101_',site_id) = concat(eot."SiteNumber",'_',split_part(eot."Subject",'-',1)) and s3.subject_number = concat(split_part(eot."Subject",'-',1),'-',split_part(eot."Subject",'-',2)))
where subject_status = 'Discontinued Treatment'

union all

--Disposition Event: Withdrawn

SELECT  es."project"::text AS studyid,
--es."SiteNumber"::text AS siteid,
concat('TAS0612_101_',split_part(es."SiteNumber",'_',2))::text AS siteid,
es."Subject"::text AS usubjid,
4.4::NUMERIC AS dsseq,
'Completion'::text AS dscat,
'Withdrawn'::text AS dsterm,
es."EOSDAT"::DATE AS dsstdtc,
case when es."EOSREAS" = '' or es."EOSREAS" is null then 'Missing' else es."EOSREAS" end ::text AS dsscat  
from tas0612_101."EOS" es
where es."EOSREAS" != 'Study Completion'

/*
union all

--Disposition Event: Study Completion

SELECT  es."project"::text AS studyid,
--es."SiteNumber"::text AS siteid,
concat('TAS0612_101_',split_part(es."SiteNumber",'_',2))::text AS siteid,
es."Subject"::text AS usubjid,
5.0::NUMERIC AS dsseq,
'Completion'::text AS dscat,
'Completed'::text AS dsterm,
es."EOSDAT"::DATE AS dsstdtc,
es."EOSREAS"::text AS dsscat  
from tas0612_101."EOS" es
where es."EOSREAS" = 'Study Completion'


union all

--Disposition Event: Failed Randomization

SELECT   "project"::TEXT AS studyid,
concat('TAS0612_101_',split_part(ie."SiteNumber",'_',2))::TEXT AS siteid,
ie."Subject"::TEXT AS usubjid,
3.1::NUMERIC AS dsseq,
'Enrollment'::TEXT AS dscat,
'Failed Randomization'::TEXT AS dsterm,
COALESCE(ie."MinCreated",ie."RecordDate")::DATE AS dsstdtc,
null::TEXT AS dsscat
from tas0612_101."IE" ie
WHERE "IEYN" = 'No' and ("project","SiteNumber", "Subject", "serial_id")
in (

select "project","SiteNumber", "Subject", max(serial_id)  as serial_id
from tas0612_101."IE"
group by 1,2,3
)


union all

--Disposition Event: Discontinued before Treatment

SELECT  es."project"::text AS studyid,
concat('TAS0612_101_',split_part(es."SiteNumber",'_',2))::text AS siteid,
es."Subject"::text AS usubjid,
4.2::NUMERIC AS dsseq,
'Randomization'::text AS dscat,
'Discontinued before Treatment'::text AS dsterm,
es."EOSDAT"::DATE AS dsstdtc,
es."EOSREAS"::text AS dsscat  
from tas0612_101."EOS" es
where es."EOSREAS" = 'Death'
*/
)

SELECT
        /*KEY (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid)::TEXT AS comprehendid, KEY*/
        ds.studyid::TEXT AS studyid,
        ds.siteid::TEXT AS siteid,
        ds.usubjid::TEXT AS usubjid,
        ds.dsseq::NUMERIC AS dsseq, --deprecated
        ds.dscat::TEXT AS dscat,
        ds.dsscat::TEXT AS dsscat,
        ds.dsterm::TEXT AS dsterm,
        ds.dsstdtc::DATE AS dsstdtc,
        null::TEXT AS dsgrpid,
        null::TEXT AS dsrefid,
        null::TEXT AS dsspid,
        null::TEXT AS dsdecod,
        null::TEXT AS visit,
        null::NUMERIC AS visitnum,
        null::INTEGER AS visitdy,
        null::TEXT AS epoch,
        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
        null::INTEGER AS dsstdy
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);

