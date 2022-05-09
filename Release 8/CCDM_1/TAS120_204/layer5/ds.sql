/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
                ---Disposition Event: All Subjects										
(SELECT distinct project ::TEXT AS studyid,
                        concat(concat(dm."project",'_'),substring(dm."SiteNumber",8,10)) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        1.0::NUMERIC AS dsseq, --deprecated
                        'All Subjects'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'All Subjects'::TEXT AS dsterm,
                        null::DATE AS dsstdtc
                        from tas120_204."DM" dm)
                        
  
 	union all
    --Disposition Event: Consented
										 
 (SELECT distinct "project" ::TEXT AS studyid,
                        concat(concat("project",'_'),substring("SiteNumber",8,10)) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        2.0::NUMERIC AS dsseq, --deprecated
                        'Consent'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Consented'::TEXT AS dsterm,
                        max(case when "ICRYN" ='Yes' then "ICRDAT" else "ICDAT" end)::DATE AS dsstdtc  
                        from tas120_204."IC" i
						group by 1,2,3,4,5,6,7)					---column does not exists
                        
 union all 

--Disposition Event: Failed Screen										 
 
  (SELECT distinct ie."project" ::TEXT AS studyid,
                        concat(concat(ie."project",'_'),substring(ie."SiteNumber",8,10)) ::TEXT AS siteid,
                        ie."Subject" ::TEXT AS usubjid,
                        2.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        string_agg(case when (nullif("IECAT",'') is null and nullif("IETESTCD",'') is null) then null 
			else concat("IECAT",' ',"IETESTCD") end, ', ')::TEXT AS dsscat,
                        'Failed Screen'::TEXT AS dsterm,
                        COALESCE("MinCreated","RecordDate")::DATE AS dsstdtc
                        from tas120_204."IE" ie
                        where ie."IEYN"='No'
                       group by 1,2,3,8 )
                        
 union all 

--Disposition Event: Enrollment										 
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        3.0::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Enrolled'::TEXT AS dsterm,
                        coalesce("ENRDAT","MinCreated","RecordDate")::DATE AS dsstdtc
                        from tas120_204."ENR" enr 
                        where "ENRYN" ='Yes'
                        )
                        
 union all 

--Disposition Event: Early EOT 
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.01::NUMERIC AS dsseq, --deprecated
                        'Treatment'::TEXT AS dscat,
                        "EOTREAS" ::TEXT AS dsscat,
                        'Early EOT'::TEXT AS dsterm,
                        "EOTDAT" ::DATE AS dsstdtc
                        from tas120_204."EOT" eot
                        where "EOTREAS" not in ('Treatment Completion' , 'Death') )
                        
 union all 

--Disposition Event: Withdrawn										 
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.4::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Withdrawn'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas120_204."EOS" eos
                        where "EOSREAS" not in ('Study Completion' ,'Death'))
                        
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
                        from tas120_204."EOS" eos
                        where "EOSREAS"='Study Completion')
                        
 union all 

--Disposition Event: Screened										 
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        1.3::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Screened'::TEXT AS dsterm,
                        COALESCE(ie."MinCreated",ie."RecordDate") ::DATE AS dsstdtc
                        from tas120_204."IE" ie
                        where "IEYN"='Yes')
                        
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
                        from tas120_204."ENR" enr
                        where "ENRYN"='No')
                        
 union all 

--Disposition Event: Discontinued before Treatment										 
 
 (SELECT distinct project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.2::NUMERIC AS dsseq, --deprecated
                        'Randomization'::TEXT AS dscat,
                        "EOSREAS"::TEXT AS dsscat,
                        'Discontinued before Treatment'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas120_204."EOS" eos
                        where "EOSREAS"='Death'))

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




