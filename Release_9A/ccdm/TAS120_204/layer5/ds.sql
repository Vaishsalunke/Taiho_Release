/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/
WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
---Disposition Event: All Subjects										
					(SELECT distinct 
						'TAS-120-204' ::TEXT AS studyid,
						concat('TAS120_204','_',split_part("study_site",'_',1)) ::TEXT AS siteid,
						"patient" ::TEXT AS usubjid,
						1.0::NUMERIC AS dsseq, 
						'All Subjects'::TEXT AS dscat,
						null::TEXT AS dsscat,
						'All Subjects'::TEXT AS dsterm,
						null::DATE AS dsstdtc
						from tas120_204_irt.patient_visit_summary d)
                        
 union all 
 
/*----Disposition Event: Consented
										 
                    (SELECT  
						project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        2.0::NUMERIC AS dsseq, --deprecated
                        'Consent'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Consented'::TEXT AS dsterm,
                        "ICDAT" ::DATE AS dsstdtc
                        from tas120_204_irt.patient_visit_summary i
                        where "IEYN"='Yes')
                        
 union all */

--Disposition Event: Failed Screen										 
 

SELECT  				'TAS-120-204' ::TEXT AS studyid,
                        concat('TAS120_204','_',split_part("study_site",'_',1))::TEXT AS siteid,
                        e."patient" ::TEXT AS usubjid,
                        2.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        string_agg(case when (nullif(i."IECAT",'') is null and nullif(i."IETESTCD",'') is null) then null 
			             else concat(i."IECAT",' ',i."IETESTCD") end, ', ') ::TEXT AS dsscat,
                        'Failed Screen'::TEXT AS dsterm,
                        "actual_date" ::DATE AS dsstdtc
                        from tas120_204_irt.patient_visit_summary e
                        left join tas120_204."IE" i
                        on   split_part(i."SiteNumber",'_',2)=e."study_site" and i."Subject"=e."patient"
                        where e."visit_description" ='Screen Failure' and
                        i.project = 'TAS120_204'  and trim(e.binimetinib_dose ) ='' 
                        group by 1,2,3,4,5,7,8
 union all 

--Disposition Event: Enrollment										 
 
 (SELECT  distinct
						'TAS-120-204'::TEXT AS studyid,
                        concat('TAS120_204','_',split_part("study_site",'_',1)) ::TEXT AS siteid,
                        "patient" ::TEXT AS usubjid,
                        3.0::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Enrolled'::TEXT AS dsterm,
                        "actual_date" ::DATE AS dsstdtc
                        from tas120_204_irt.patient_visit_summary
                        where "visit_description" =  'Cycle 1')
                        
 union all 

--Disposition Event: Early EOT 
 
 (SELECT  'TAS-120-204' ::TEXT AS studyid,
                        concat('TAS120_204','_',split_part("study_site",'_',1)) ::TEXT AS siteid,
                        "patient" ::TEXT AS usubjid,
                        4.01::NUMERIC AS dsseq, --deprecated
                        'Treatment'::TEXT AS dscat,
                        i."EOTREAS" ::TEXT AS dsscat,
                        'Early EOT'::TEXT AS dsterm,
                        "actual_date" ::DATE AS dsstdtc
                        from tas120_204_irt.patient_visit_summary b
                        left join tas120_204."EOT" i
                        on  split_part(i."SiteNumber",'_',2)=b."study_site" and i."Subject"=b."patient"
                        where "visit_description" = 'Discontinuation'
                        ) 
                        
                        
 union all 

--Disposition Event: Withdrawn										 
 
 (SELECT  'TAS-120-204' ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        4.4::NUMERIC AS dsseq,
                        'Completion'::TEXT AS dscat,
                        case when eos."EOSREAS" = '' or eos."EOSREAS" is null then 'Missing' else eos."EOSREAS" end::TEXT AS dsscat,
                        'Withdrawn'::TEXT AS dsterm,
                        "EOSDAT" ::DATE AS dsstdtc
                        from tas120_204."EOS" eos
                        where "EOSREAS"!= ('Study Completion'))
                        
 union all 

/*--Disposition Event: Study Completion										 
 
 (SELECT  project ::TEXT AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::TEXT AS siteid,
                        "Subject" ::TEXT AS usubjid,
                        5.0::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Completed'::TEXT AS dsterm,
                        case when "EOSREAS" ='' and "EOSREAS" is null then 'Missing' else "EOSREAS" ::DATE AS dsstdtc
                        from tas120_204."EOS" eos
                        where "EOSREAS"='Study Completion')
                        
 union all */

--Disposition Event: Screened										 
 
 (SELECT   'TAS-120-204' ::TEXT AS studyid,
                       concat('TAS120_204','_',split_part("study_site",'_',1)) ::TEXT AS siteid,
                        "patient" ::TEXT AS usubjid,
                        1.3::NUMERIC AS dsseq, 
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Screened'::TEXT AS dsterm,
                        "actual_date" ::DATE AS dsstdtc
                        from tas120_204_irt.patient_visit_summary 
                        where "visit_description" = 'Screening')
 
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
                        from tas120_204."IC" ic
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
                        from tas120_204."EOS" eos
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






