/*
 CCDM DS mapping
 Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

ds_data AS (

----Disposition Event: All Subjects----

SELECT  "project"::TEXT AS studyid,
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

--Disposition Event: Screened

SELECT  "project"::TEXT AS studyid,
enr."SiteNumber"::TEXT AS siteid,
enr."Subject"::TEXT AS usubjid,
1.3::NUMERIC AS dsseq, --deprecated
'Screened'::TEXT AS dscat,
null::TEXT AS dsscat,
'Screened'::TEXT AS dsterm,
enr."DMICDAT"::DATE AS dsstdtc
from tas120_201."ENR" enr

union all 

--Disposition Event: Failed Screen

SELECT  "project"::TEXT AS studyid,
ie."SiteNumber"::TEXT AS siteid,
ie."Subject"::TEXT AS usubjid,
2.1::NUMERIC AS dsseq, --deprecated
'Enrollment'::TEXT AS dscat,
case when nullif("IECAT",'') is null and nullif("IETESTCD",'') is null
		then null
	else concat(concat("IECAT",' '), "IETESTCD") 
end::TEXT AS dsscat,
'Failed Screen'::TEXT AS dsterm,
COALESCE(ie."MinCreated" ,ie."RecordDate")::DATE AS dsstdtc
from tas120_201."IE" ie
WHERE ie."IEYN"='No'

union all 

--Disposition Event: Enrollment

SELECT  "project"::TEXT AS studyid,
enr."SiteNumber"::TEXT AS siteid,
enr."Subject"::TEXT AS usubjid,
3.0::NUMERIC AS dsseq, --deprecated
'Enrollment'::TEXT AS dscat,
null::TEXT AS dsscat,
'Enrolled'::TEXT AS dsterm,
enr."ENRDAT"::DATE AS dsstdtc
from tas120_201."ENR" enr
WHERE "ENRYN"='Yes'

union all 

--Disposition Event: Early EOT

SELECT  "project"::TEXT AS studyid,
ds."SiteNumber"::TEXT AS siteid,
ds."Subject"::TEXT AS usubjid,
4.01::NUMERIC AS dsseq, --deprecated
'Treatment'::TEXT AS dscat,
ds."DSREAS"::TEXT AS dsscat,
'Early EOT'::TEXT AS dsterm,
ds."DSDAT"::DATE AS dsstdtc
from tas120_201."DS" ds 

union all 
--Disposition Event: Withdrawn

SELECT  "project"::TEXT AS studyid,
eos."SiteNumber"::TEXT AS siteid,
eos."Subject"::TEXT AS usubjid,
4.4::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
case when eos."EOS_RSN" = '' then 'Missing' else eos."EOS_RSN" end ::text AS dsscat, 
'Withdrawn'::TEXT AS dsterm,
eos."EOS_DAT"::DATE AS dsstdtc
from tas120_201."EOS" eos
WHERE "EOS_RSN" != 'End of study per protocol'

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

/*
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
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);



