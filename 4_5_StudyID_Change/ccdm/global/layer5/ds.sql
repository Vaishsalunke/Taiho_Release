/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
                SELECT  null::TEXT AS studyid,
                        null::TEXT AS siteid,
                        null::TEXT AS usubjid,
                        null::NUMERIC AS dsseq, --deprecated
                        null::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        null::TEXT AS dsterm,
                        null::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy)

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
        ds.dsgrpid::TEXT AS dsgrpid,
        ds.dsrefid::TEXT AS dsrefid,
        ds.dsspid::TEXT AS dsspid,
        ds.dsdecod::TEXT AS dsdecod,
        ds.visit::TEXT AS visit,
        ds.visitnum::NUMERIC AS visitnum,
        ds.visitdy::INTEGER AS visitdy,
        ds.epoch::TEXT AS epoch,
        ds.dsdtc::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
        ds.dsstdy::INTEGER AS dsstdy
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid)
WHERE 1=2;  
