/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
                ---Disposition Event: All Subjects

(SELECT distinct 'TAS2940_101' ::TEXT AS studyid,
                        'TAS2940_101_'||site_id ::TEXT AS siteid,
                        subject_number ::TEXT AS usubjid,
                        1.0::NUMERIC AS dsseq, --deprecated
                        'All Subjects'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'All Subjects'::TEXT AS dsterm,
                        null::DATE AS dsstdtc
                        from tas2940_101_irt.subject) 
                       
 /*
  union all
    --Disposition Event: Consented

  (select studyid,siteid,usubjid,dsseq,dscat,dsscat,dsterm,max(dsstdtc)  from (SELECT   "project" ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        2.0::NUMERIC AS dsseq, --deprecated
                        'Consent'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Consented'::TEXT AS dsterm,
                         case when "ICRYN" = 'Yes' THEN "ICRDAT" else "ICDAT"
                         end::DATE AS dsstdtc      
                       from tas2940_101."IC" i )a group by studyid,siteid,usubjid,dsseq,dscat,dsscat,dsterm

                        )
   */                    
 union all
 
 (SELECT distinct 'TAS2940_101' ::TEXT AS studyid,
                        'TAS2940_101_'||site_id ::TEXT AS siteid,
                        subject_number ::TEXT AS usubjid,
                        2.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        concat(ie."IECAT", ie."IETESTCD")::TEXT AS dsscat,
                        'Failed Screen'::TEXT AS dsterm,
                        screen_fail_date ::DATE AS dsstdtc
                        from tas2940_101_irt.subject s1
                        left join tas2940_101."IE" ie on ('TAS2940_101' = ie.project and 'TAS2940_101_'||s1.site_id = concat('TAS2940_101_',split_part(ie."SiteNumber",'_',2)) and s1.subject_number = ie."Subject")
                        where subject_status = 'Screen Failed' and screen_fail_date <> ''
                        )
                       
 union all

--Disposition Event: Enrollment
 
 (SELECT distinct 'TAS2940_101' ::TEXT AS studyid,
                        'TAS2940_101_'||site_id ::TEXT AS siteid,
                        subject_number ::TEXT AS usubjid,
                        3.0::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Enrolled'::TEXT AS dsterm,
                        enrollment_date ::DATE AS dsstdtc
                        from tas2940_101_irt.subject s2
                        where subject_status = 'Enrolled'
                        )
                       
 union all

--Disposition Event: Early EOT
 
 (SELECT distinct 'TAS2940_101' ::TEXT AS studyid,
                        'TAS2940_101_'||site_id ::TEXT AS siteid,
                        subject_number ::TEXT AS usubjid,
                        4.01::NUMERIC AS dsseq, --deprecated
                        'Treatment'::TEXT AS dscat,
                        eot."EOTREAS" ::TEXT AS dsscat,
                        'Early EOT'::TEXT AS dsterm,
                        end_of_treatment_date ::DATE AS dsstdtc
                        from tas2940_101_irt.subject s3
                        left join tas2940_101."EOT" eot on ('TAS2940_101' = eot.project and 'TAS2940_101_'||s3.site_id = concat('TAS2940_101_',split_part(eot."SiteNumber",'_',2)) and s3.subject_number = eot."Subject")
                        where subject_status = 'Discontinued Treatment'
                        )
                       
 union all

--Disposition Event: Withdrawn
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.4::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        case when eos."EOSREAS" = '' or eos."EOSREAS" is null then 'Missing' else eos."EOSREAS" end::TEXT AS dsscat,
                        'Withdrawn'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas2940_101."EOS" eos
                        where "EOSREAS" != 'Study Completion' )
                       
 
                       
 union all

--Disposition Event: Screened
 
 (SELECT distinct 'TAS2940_101' ::TEXT AS studyid,
                       'TAS2940_101_'||site_id ::TEXT AS siteid,
                        subject_number  ::TEXT AS usubjid,
                        1.3::NUMERIC AS dsseq, --deprecated
                        'Screened'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Screened'::TEXT AS dsterm,
                        screening_date ::DATE AS dsstdtc
                        from tas2940_101_irt.subject s4)
 
/*
 
 union all

--Disposition Event: Study Completion
 
 (SELECT  distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        5.0::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Completed'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas2940_101."EOS" eos
                        where "EOSREAS"='Study Completion')
 
 union all
 
--Disposition Event: Failed Randomization
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        3.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Failed Randomization'::TEXT AS dsterm,
                        "ENRDAT" ::DATE AS dsstdtc
                       from tas2940_101."ENR" enr
                        where "ENRYN"='No')
                       
 union all

--Disposition Event: Discontinued before Treatment
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.2::NUMERIC AS dsseq, --deprecated
                        'Randomization'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Discontinued before Treatment'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas2940_101."EOS" eos
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
        /*KEY, (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);



