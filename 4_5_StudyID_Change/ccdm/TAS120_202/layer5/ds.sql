/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
               
            ie_data as (select "project", "SiteNumber", "Subject", "IECAT", "IETESTCD"  from tas120_202."IE" i),
           
            eot_data as (select "SiteNumber", "Subject", project, "EOTREAS"  from "tas120_202"."EOT" eot),

     ds_data AS (
           select distinct
                'TAS-120-202'::TEXT AS studyid,
                        concat('TAS120_202_',study_site) ::TEXT AS siteid,
                        patient ::TEXT AS usubjid,
                        1.0::NUMERIC AS dsseq, --deprecated
                        'All Subjects'::TEXT AS dscat,
                        'All Subjects'::TEXT AS dsterm,
                        null::DATE AS dsstdtc,
                        null::TEXT AS dsscat
                        from tas120_202_irt.patient_visit_summary pvs

union all

--Disposition Event: Consented

/*SELECT  enr."project"::text AS studyid,
enr."SiteNumber"::text AS siteid,
enr."Subject"::text AS usubjid,
2.0::NUMERIC AS dsseq,
'Consent'::text AS dscat,
'Consented'::text AS dsterm,
enr."MinCreated" ::DATE AS dsstdtc,
null::text AS dsscat
from "tas120_202"."ENR" enr
SELECT  dm."project"::text AS studyid,
dm."SiteNumber"::text AS siteid,
dm."Subject"::text AS usubjid,
2.0::NUMERIC AS dsseq,
'Consent'::text AS dscat,
'Consented'::text AS dsterm,
dm."DMICDAT" ::DATE AS dsstdtc,
null::text AS dsscat
from "tas120_202"."DM" dm
where ("project","SiteNumber", "Subject", "serial_id")
in ( select "project","SiteNumber", "Subject", max(serial_id)  as serial_id
from "tas120_202"."DM"
group by "project","SiteNumber", "Subject")

union all */

--Disposition Event : Screened

select
                      'TAS-120-202'::TEXT AS studyid,
                        concat('TAS120_202_',study_site) ::TEXT AS siteid,
                        patient ::TEXT AS usubjid,
                        1.3::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        'Screened'::TEXT AS dsterm,
                        actual_date ::DATE AS dsstdtc,
                        null::TEXT AS dsscat
                        from tas120_202_irt.patient_visit_summary pvs1
                        where visit_description = 'Screening'
union all

--Disposition Event: Failed Screen

select studyid,siteid,usubjid,dsseq,dscat,dsterm,dsstdtc, string_agg(dsscat,';') from (
select 'TAS-120-202' ::TEXT AS studyid,
                        concat('TAS120_202_',study_site) ::TEXT AS siteid,
                      patient ::TEXT AS usubjid,
                        2.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        'Failed Screen'::TEXT AS dsterm,
                        actual_date ::DATE AS dsstdtc,
                        concat(concat("IECAT",' '), "IETESTCD")  ::TEXT AS dsscat
                        from tas120_202_irt.patient_visit_summary pvs2
                        left join tas120_202."IE" i on ('TAS120_202'=i."project" and concat('TAS120_202_',pvs2.study_site) = i."SiteNumber" and pvs2.patient = i."Subject")
                        where visit_description = 'Screen Failure' and nullif (tas_120_required, '') is null) fs
                        group by 1,2,3,4,5,6,7
union all

--Disposition Event: Enrollment

select distinct
                      'TAS-120-202'::TEXT AS studyid,
                        concat('TAS120_202_',study_site) ::TEXT AS siteid,
                        patient ::TEXT AS usubjid,
                        3.0::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        'Enrolled'::TEXT AS dsterm,
                        actual_date ::DATE AS dsstdtc,
                        null::TEXT AS dsscat
                        from tas120_202_irt.patient_visit_summary pvs3
where visit_description = 'Cycle 1'
union all

--Disposition Event: Early EOT

select
                      'TAS-120-202'::TEXT AS studyid,
                        concat('TAS120_202_',study_site) ::TEXT AS siteid,
                        patient ::TEXT AS usubjid,
                        4.01::NUMERIC AS dsseq, --deprecated
                        'Treatment'::TEXT AS dscat,
                        'Early EOT'::TEXT AS dsterm,
                        actual_date ::DATE AS dsstdtc,
                        eot."EOTREAS" ::TEXT AS dsscat
                        from tas120_202_irt.patient_visit_summary pvs4
                        left join "tas120_202"."EOT" eot on ('TAS120_202' = eot.project and concat('TAS120_202_',pvs4.study_site) = eot."SiteNumber" and pvs4.patient = eot."Subject")
where visit_description like '%Discont%'
union all

--Disposition Event: Withdrawn (EOS)

SELECT  'TAS-120-202'::text AS studyid,
eos."SiteNumber"::text AS siteid,
eos."Subject"::text AS usubjid,
4.1::NUMERIC AS dsseq,
'Completion'::text AS dscat,
'Withdrawn'::text AS dsterm,
eos."EOSDAT"::DATE AS dsstdtc,
case when eos."EOSREAS" = '' then 'Missing' else eos."EOSREAS" end ::text AS dsscat
from "tas120_202"."EOS" eos
where eos."EOSREAS" != 'End of study per protocol'
/*
union all



--Disposition Event: Study Completion

SELECT  eos."project"::text AS studyid,
eos."SiteNumber"::text AS siteid,
eos."Subject"::text AS usubjid,
5.0::NUMERIC AS dsseq,
'Completion'::text AS dscat,
'Completed'::text AS dsterm,
eos."EOSDAT"::DATE AS dsstdtc,
null::text AS dsscat
from "tas120_202"."EOS" eos
where eos."EOSREAS" = 'End of study per protocol'


union all

--Disposition Event: Failed Randomization

SELECT  "project"::TEXT AS studyid,
ie."SiteNumber"::TEXT AS siteid,
ie."Subject"::TEXT AS usubjid,
3.1::NUMERIC AS dsseq,
'Enrollment'::TEXT AS dscat,
'Failed Randomization'::TEXT AS dsterm,
max(COALESCE(ie."MinCreated" ,ie."RecordDate"))::DATE AS dsstdtc,
null::TEXT AS dsscat
from tas120_202."IE" ie
WHERE "IEYN" = 'No'
group by 1,2,3,4,5,6,8


union all

--Disposition Event: Discontinued before Treatment

SELECT  eos."project"::text AS studyid,
eos."SiteNumber"::text AS siteid,
eos."Subject"::text AS usubjid,
4.2::NUMERIC AS dsseq,
'Randomization'::text AS dscat,
'Discontinued before Treatment'::text AS dsterm,
eos."EOSDAT"::DATE AS dsstdtc,
null::text AS dsscat
from "tas120_202"."EOS" eos
where eos."EOSREAS" = 'Death'
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


