/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
	            SELECT Distinct dm."project"::text AS studyid,
								dm."SiteNumber"::text AS siteid,
								dm."Subject"::text AS usubjid,
								1.0::NUMERIC AS dsseq, 
								'All Subjects'::text AS dscat,
								'All Subjects'::text AS dsterm,
								null::DATE AS dsstdtc,
								null::text AS dsscat 
				from "tas120_202"."DM" dm
				
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
				from "tas120_202"."ENR" enr*/
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
				in ( select 	"project","SiteNumber", "Subject", max(serial_id)  as serial_id
					 from 		"tas120_202"."DM"
					 group by 	"project","SiteNumber", "Subject")
				
				union all 
				
				--Disposition Event : Screened
				
				SELECT  dm."project"::text AS studyid,
						dm."SiteNumber"::text AS siteid,
						dm."Subject"::text AS usubjid,
						1.3::NUMERIC AS dsseq, 
						'Screened'::text AS dscat,
						'Screened'::text AS dsterm,
						dm."DMICDAT" ::DATE AS dsstdtc,
						null::text AS dsscat 
				from "tas120_202"."DM" dm
				where ("project","SiteNumber", "Subject", "serial_id")
				in ( select 	"project","SiteNumber", "Subject", max(serial_id)  as serial_id
					 from 		"tas120_202"."DM"
					 group by 	"project","SiteNumber", "Subject")
				
				union all
				
				--Disposition Event: Failed Screen
				
				select studyid,
	   				   siteid,
	   				   usubjid,
	   				   dsseq,
	   				   dscat,
	   				   dsterm,
	   				   dsstdtc,
	   				   dsscat
				from (
				select ie."project"::text AS studyid,
						ie."SiteNumber"::text AS siteid,
						ie."Subject"::text AS usubjid,
						2.1::NUMERIC AS dsseq,
						'Enrollment'::text AS dscat,
						'Failed Screen'::text AS dsterm,
				COALESCE(ie."MinCreated" ,ie."RecordDate") ::Date as dsstdtc,
				case when nullif("IECAT",'') is null and nullif("IETESTCD",'') is null
								then null
							else concat(concat("IECAT",' '), "IETESTCD")
						end::TEXT AS dsscat
						,"RecordPosition"
				from "tas120_202"."IE"  ie)a
				where (studyid, siteid, usubjid, dsstdtc,"RecordPosition") in
				(SELECT  ie."project"::text AS studyid,
						ie."SiteNumber"::text AS siteid,
						ie."Subject"::text AS usubjid,
						max(COALESCE(ie."MinCreated" ,ie."RecordDate"))::DATE AS dsstdtc
						,max("RecordPosition")
				from "tas120_202"."IE" as ie
				where ie."IEYN" = 'No'
				group by 1,2,3)					
				union all 
				
				--Disposition Event: Enrollment
				
				SELECT  enr."project"::text AS studyid,
						enr."SiteNumber"::text AS siteid,
						enr."Subject"::text AS usubjid,
						3.0::NUMERIC AS dsseq,
						'Enrollment'::text AS dscat,
						'Enrolled'::text AS dsterm,
						enr."ENRDAT"::DATE AS dsstdtc,
						null::text AS dsscat  
				from "tas120_202"."ENR" enr
				where enr."ENRYN"='Yes'
				
				union all 
				
				--Disposition Event: Withdrawn (EOS)
				
				SELECT  eos."project"::text AS studyid,
						eos."SiteNumber"::text AS siteid,
						eos."Subject"::text AS usubjid,
						4.4::NUMERIC AS dsseq, 
						'Completion'::text AS dscat,
						'Withdrawn'::text AS dsterm,
						eos."EOSDAT"::DATE AS dsstdtc,
						case when eos."EOSREAS" = '' then 'Missing' else eos."EOSREAS" end ::text AS dsscat
				from "tas120_202"."EOS" eos
				where eos."EOSREAS" not in ( 'End of study per protocol','Death')
				
				union all 
				
				--Disposition Event: Withdrawn (EOT)
				
				SELECT  eot."project"::text AS studyid,
						eot."SiteNumber"::text AS siteid,
						eot."Subject"::text AS usubjid,
						4.01::NUMERIC AS dsseq, 
						'Treatment'::text AS dscat,
						'Early EOT'::text AS dsterm,
						eot."EOTDAT"::DATE AS dsstdtc,
						eot."EOTREAS"::text AS dsscat  
				from "tas120_202"."EOT" eot
			--	where eot."EOTREAS" <> 'End of study per 2 protocol'
				
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



