/*
 CCDM DS mapping
 Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

ds_data AS (

----Disposition Event: All Subjects----

SELECT  
'TAS-120-201'::TEXT AS studyid,
dm."SiteNumber"::TEXT AS siteid,
dm."Subject"::TEXT AS usubjid,
1.0::NUMERIC AS dsseq, --deprecated
'All Subjects'::TEXT AS dscat,
null::TEXT AS dsscat,
'All Subjects'::TEXT AS dsterm,
null::DATE AS dsstdtc
from tas120_201."DM" dm

union all

--Disposition Event: Consented
/*
SELECT  "project"::TEXT AS studyid,
enr."SiteNumber"::TEXT AS siteid,
enr."Subject"::TEXT AS usubjid,
2.0::NUMERIC AS dsseq, --deprecated
'Consent'::TEXT AS dscat,
null::TEXT AS dsscat,
'Consented'::TEXT AS dsterm,
enr."DMICDAT"::DATE AS dsstdtc
from tas120_201."ENR" enr

union all
*/
--Disposition Event: Screened

select studyid, siteid, usubjid, dsseq, dscat, dsscat, dsterm, min (dsstdtc) as dsstdtc from (
SELECT  'TAS-120-201'::TEXT AS studyid,
concat('TAS120_201_',study_site)::TEXT AS siteid,
patient ::TEXT AS usubjid,
1.3::NUMERIC AS dsseq, --deprecated
'Enrollment'::TEXT AS dscat,
null::TEXT AS dsscat,
'Screened'::TEXT AS dsterm,
actual_date ::DATE AS dsstdtc
from tas120_201_irt.patient_visit_summary pvs3
where visit_description in ('Prescreening/Screening', 'Screening'))a
group by 1,2,3,4,5,6,7

union all

--Disposition Event: Failed Screen

SELECT  distinct
'TAS-120-201'::TEXT AS studyid,
concat('TAS120_201_',study_site) ::TEXT AS siteid,
patient ::TEXT AS usubjid,
2.1::NUMERIC AS dsseq, --deprecated
'Enrollment'::TEXT AS dscat,
concat(concat("IECAT",' '), "IETESTCD")   ::TEXT AS dsscat,
'Failed Screen'::TEXT AS dsterm,
last_visit_date ::DATE AS dsstdtc
from tas120_201_irt.patient_summary ps
left join tas120_201."IE" i on ('TAS120_201'=i."project" and concat('TAS120_201_',ps.study_site) = i."SiteNumber" and ps.patient = i."Subject")
where status in ('Prescreen Failed', 'Screen Failed', 'Final Screen Failed')

union all

--Disposition Event: Enrollment

select studyid, siteid, usubjid, dsseq, dscat, dsscat, dsterm, min (dsstdtc) as dsstdtc from (
SELECT distinct 'TAS-120-201'::TEXT AS studyid,
concat('TAS120_201_',study_site) ::TEXT AS siteid,
patient ::TEXT AS usubjid,
3.0::NUMERIC AS dsseq, --deprecated
'Enrollment'::TEXT AS dscat,
null::TEXT AS dsscat,
'Enrolled'::TEXT AS dsterm,
case
when visit_description in ('Discontinue', 'Cycle 1', 'Cycle 1 Day 1') then (actual_date)
end ::DATE AS dsstdtc
from tas120_201_irt.patient_visit_summary pvs
where visit_description in ('Discontinue', 'Cycle 1', 'Cycle 1 Day 1'))a
group by 1,2,3,4,5,6,7

union all

--Disposition Event: Early EOT

SELECT  
'TAS-120-201'::TEXT AS studyid,
concat('TAS120_201_',study_site)::TEXT AS siteid,
patient ::TEXT AS usubjid,
4.01::NUMERIC AS dsseq, --deprecated
'Treatment'::TEXT AS dscat,
d."DSREAS"::TEXT AS dsscat,
'Early EOT'::TEXT AS dsterm,
actual_date ::DATE AS dsstdtc
from tas120_201_irt.patient_visit_summary pvs2
left join tas120_201."DS" d on ('TAS120_201' = d.project and concat('TAS120_201_',pvs2.study_site) = d."SiteNumber" and pvs2.patient = d."Subject")
where visit_description in ('Discontinue', 'Permanently Discontinue')
union all
--Disposition Event: Withdrawn

SELECT  'TAS-120-201'::TEXT AS studyid,
eos."SiteNumber"::TEXT AS siteid,
eos."Subject"::TEXT AS usubjid,
4.4::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
case when eos."EOS_RSN" = '' then 'Missing' else eos."EOS_RSN" end ::text AS dsscat,
'Withdrawn'::TEXT AS dsterm,
eos."EOS_DAT"::DATE AS dsstdtc
from tas120_201."EOS" eos
WHERE "EOS_RSN" != 'End of study per protocol'
/*
union all

--Disposition Event: Study Completion

SELECT  "project"::TEXT AS studyid,
eos."SiteNumber"::TEXT AS siteid,
eos."Subject"::TEXT AS usubjid,
5.0::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
null::TEXT AS dsscat,
'Completed'::TEXT AS dsterm,
eos."EOS_DAT"::DATE AS dsstdtc
from tas120_201."EOS" eos
WHERE "EOS_RSN" = 'End of study per protocol'


union all

--Disposition Event: Failed Randomization

SELECT  "project"::TEXT AS studyid,
ie."SiteNumber"::TEXT AS siteid,
ie."Subject"::TEXT AS usubjid,
3.1::NUMERIC AS dsseq,
'Enrollment'::TEXT AS dscat,
null::TEXT AS dsscat,
'Failed Randomization'::TEXT AS dsterm,
COALESCE(ie."MinCreated" ,ie."RecordDate")::DATE AS dsstdtc
from tas120_201."IE" ie
WHERE "IEYN" = 'No'

union all

--Disposition Event: Discontinued before treatment

SELECT  "project"::TEXT AS studyid,
eos."SiteNumber"::TEXT AS siteid,
eos."Subject"::TEXT AS usubjid,
4.2::NUMERIC AS dsseq, --deprecated
'Randomization'::TEXT AS dscat,
null::TEXT AS dsscat,
'Discontinued before Treatment'::TEXT AS dsterm,
eos."EOS_DAT"::DATE AS dsstdtc
from tas120_201."EOS" eos
WHERE "EOS_RSN" = 'Death'
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
        /*KEY, (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);




