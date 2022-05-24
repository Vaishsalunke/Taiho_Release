/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
                ---Disposition Event: All Subjects										
(SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        1.0::NUMERIC AS dsseq, --deprecated
                        'All Subjects'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'All Subjects'::TEXT AS dsterm,
                        null::DATE AS dsstdtc
                        from tas120_203."DM" d)
                        
 union all 
 
----Disposition Event: Consented
										 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        2.0::NUMERIC AS dsseq, --deprecated
                        'Consent'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Consented'::TEXT AS dsterm,
                        "ICDAT" ::DATE AS dsstdtc
                        from tas120_203."IC" i
                        where "IEYN"='Yes')
                        
 union all 

--Disposition Event: Failed Screen										 
 
 (SELECT  e.project ::TEXT AS studyid,
                        e.project||substring(e."SiteNumber",position ('_' in e."SiteNumber")) ::TEXT AS siteid,
                        e."Subject" ::TEXT AS usubjid,
                        2.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        i."IETESTCD" ::TEXT AS dsscat,
                        'Failed Screen'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas120_203."EOS" e
                        join tas120_203."IC" i
                        on i.project=e.project and i."SiteNumber"=e."SiteNumber" and i."Subject"=e."Subject"
                        and e."EOSREAS" ='Screen Failure'and i."IEYN"='No'
                        )
                        
 union all 

--Disposition Event: Enrollment										 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        3.0::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Enrolled'::TEXT AS dsterm,
                        "ENRDAT" ::DATE AS dsstdtc
                        from tas120_203."ENR" enr 
                        where "ENRYN" ='Yes'
                        )
                        
 union all 

--Disposition Event: Early EOT 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.01::NUMERIC AS dsseq, --deprecated
                        'Treatment'::TEXT AS dscat,
                        "EOTREAS" ::TEXT AS dsscat,
                        'Early EOT'::TEXT AS dsterm,
                        "EOTDAT" ::DATE AS dsstdtc
                        from tas120_203."EOT" eot
                        where "EOTREAS" != 'Treatment Completion' )
                        
 union all 

--Disposition Event: Withdrawn										 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.4::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        case when eos."EOSREAS" = '' or eos."EOSREAS" is null then 'Missing' else eos."EOSREAS" end::TEXT AS dsscat,
                        'Withdrawn'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas120_203."EOS" eos
                        where "EOSREAS" not in ('Study Completion' ,'Screen Failure'))
                        
 union all 

--Disposition Event: Study Completion										 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        5.0::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Completed'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas120_203."EOS" eos
                        where "EOSREAS"='Study Completion')
                        
 union all 

--Disposition Event: Screened										 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        1.3::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Screened'::TEXT AS dsterm,
                        "VISITDAT" ::DATE AS dsstdtc
                        from tas120_203."IC" ic
                        where "IEYN"='Yes')
 
/* 
 union all 
 
--Disposition Event: Failed Randomization										 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        3.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Failed Randomization'::TEXT AS dsterm,
                        "VISITDAT" ::DATE AS dsstdtc
                        from tas120_203."IC" ic
                        where "IEYN"='No')
                        
 union all 

--Disposition Event: Discontinued before Treatment										 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.2::NUMERIC AS dsseq, --deprecated
                        'Randomization'::TEXT AS dscat,
                        "EOSREAS"::TEXT AS dsscat,
                        'Discontinued before Treatment'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas120_203."EOS" eos
                        where "EOSREAS"='Death')
						
						
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

