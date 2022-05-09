/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
                
--Disposition Event: All Subjects
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        1.0::NUMERIC AS dsseq, --deprecated
                        'All Subjects'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'All Subjects'::TEXT AS dsterm,
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
                        null::INTEGER AS dsstdy
                        from tas117_201."DM" d 
union all                        
                        
--Disposition Event: Consented										
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        2.0::NUMERIC AS dsseq, --deprecated
                        'Consent'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Consented'::TEXT AS dsterm,
                        case when "PATRECON" ='Y' then "ICDT2" else "INFCONDT" 
                        end::DATE AS dsstdtc,  
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
                        from tas117_201."IC" i 

union all                          
--Disposition Event: Failed Screen
						SELECT  	project  ::TEXT AS studyid,
			concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
			"Subject" ::TEXT AS usubjid,
			2.1::NUMERIC AS dsseq, 
			'Enrollment'::TEXT AS dscat,
			string_agg(case when (nullif("IECAT",'') is null and nullif("IETESTCD",'') is null) then null 
			else concat("IECAT",' ',"IETESTCD") end, ', ')::TEXT AS dsscat,
			'Failed Screen'::TEXT AS dsterm,
			coalesce ("MinCreated","RecordDate")::DATE AS dsstdtc,
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
from		tas117_201."IE" i where "IEYN" ='No' 
group by	1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17 

union all                          
--Disposition Event: Enrollment										                        
						SELECT  project  ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        3.0::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Enrolled'::TEXT AS dsterm,
                        "ENRDAT" ::DATE AS dsstdtc,  
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
                        from tas117_201."ENR" e where "ENPERF" ='Yes'
union all                          
--Disposition Event: Early EOT                        
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.01::NUMERIC AS dsseq, --deprecated
                        'Treatment'::TEXT AS dscat,
                        "EOTREAS"::TEXT AS dsscat,
                        'Early EOT'::TEXT AS dsterm,
                        "EOTDAT"::DATE AS dsstdtc,  
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
                        from tas117_201."EOT" e2 where "EOTREAS" not in ('Death','Treatment Completion')
union all  
--Disposition Event: Withdrawn										                        
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.4::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Withdrawn'::TEXT AS dsterm,
                        "EOSDAT"::DATE AS dsstdtc,  
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
                        from tas117_201."EOS" e3 where "EOSREAS" not in ('Death','Study Completion')
union all  
--Disposition Event: Study Completion                        
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        5.0::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Completed'::TEXT AS dsterm,
                        "EOSDAT"::DATE AS dsstdtc,  
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
                        from tas117_201."EOS" e4 where "EOSREAS"  = ('Study Completion')
union all                          
--Disposition Event: Screened                        
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        1.3::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        case when (nullif("IECAT",'') is null and nullif("IETESTCD",'') is null) then null 
                        else concat("IECAT",' ',"IETESTCD") end ::TEXT AS dsscat,
                        'Screened'::TEXT AS dsterm,
                        coalesce ("MinCreated","RecordDate")::DATE AS dsstdtc,  
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
                        from tas117_201."IE" i where "IEYN" ='Yes'
union all   
--Disposition Event: Failed Randomization                        
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        3.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Failed Randomization'::TEXT AS dsterm,
                        "ENRDAT"::DATE AS dsstdtc,  
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
                        from tas117_201."ENR" e5  where "ENPERF" = 'No'      
union all  
--Disposition Event: Discontinued before Treatment										                        
						SELECT  project ::TEXT AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.2::NUMERIC AS dsseq, --deprecated
                        'Randomization'::TEXT AS dscat,
                        "EOSREAS"::TEXT AS dsscat,
                        'Discontinued before Treatment'::TEXT AS dsterm,
                        "EOSDAT"::DATE AS dsstdtc,  
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
                        from tas117_201."EOS" e6 where "EOSREAS" ='Death')

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
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);

