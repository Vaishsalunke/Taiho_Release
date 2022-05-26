/*
CCDM SiteMonitoringVisit mapping
Notes: Standard mapping to CCDM SiteMonitoringVisit table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid from site ),
 
     sitemonitoringvisit_data AS (
     	  select studyid,
				 siteid,
				 visitname||'~'||row_number()over(partition by visitname,siteid ORDER by visitdate,smvvtype ASC) as visitname,
				 visitdate,
				 smvipyn,
				 smvpiyn,
				 smvtrvld,
				 smvtrvlu,
				 smvvtype,
				 smvmethd
		  from 
				(
					SELECT  'TAS120_203'::text AS studyid,
							concat('TAS120_203_',site_number)::text AS siteid,
							visit_type ::text AS visitname,
							--nullif(start_date_of_conducted_visit,'') ::date AS visitdate,
							coalesce(nullif("start_date_of_conducted_visit"::text,''),nullif("planned_visit_date"::text,'')) ::date AS visitdate,
							null::text AS smvipyn,
							null::text AS smvpiyn,
							(TO_DATE(nullif(visit_end_date,''),'DD-Mon-YYYY') - TO_DATE(nullif(start_date_of_conducted_visit,''),'DD-Mon-YYYY')) ::text as smvtrvld,
							'days'::text AS smvtrvlu,
							visit_type::text AS smvvtype,
							null::text AS smvmethd 
					from TAS120_203_ctms.study_visit
					where start_date_of_conducted_visit is not null
				)r                
								)

SELECT 
        /*KEY (smv.studyid || '~' || smv.siteid)::text AS comprehendid, KEY*/
        smv.studyid::text AS studyid,
        smv.siteid::text AS siteid,
        smv.visitname::text AS visitname,
        smv.visitdate::date AS visitdate,
        smv.smvipyn::text AS smvipyn,
        smv.smvpiyn::text AS smvpiyn,
        smv.smvtrvld::text AS smvtrvld,
        smv.smvtrvlu::text AS smvtrvlu,
        smv.smvvtype::text AS smvvtype,
        smv.smvmethd::text AS smvmethd
        /*KEY, (smv.studyid || '~' || smv.siteid || '~' || smv.visitName)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisit_data smv
JOIN included_sites si ON (smv.studyid = si.studyid AND smv.siteid = si.siteid);



