/*
CCDM SiteMonitoringVisit mapping
Notes: Standard mapping to CCDM SiteMonitoringVisit table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
 
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
			from (
						SELECT  'TAS120_204'::text AS studyid,
								concat('TAS120_204_', right(site_number,3)) ::text AS siteid,
								visit_name ::text AS visitname,
								coalesce(nullif(sv."visit_completed_date",''),nullif(sv."planned_visit_date",''),nullif(sv."visit_start_date",''))::date AS visitdate,
								null::text AS smvipyn,
								null::text AS smvpiyn,
								((nullif(visit_completed_date ,'')::date)-nullif(visit_start_date,'')::date)::text as smvtrvld,
								'days'::text AS smvtrvlu,
								visit_type ::text AS smvvtype,
								visit_category ::text AS smvmethd
						from tas120_204_ctms.site_visits sv
						where visit_start_date is not null
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
        /*KEY , (smv.studyid || '~' || smv.siteid || '~' || smv.visitName)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisit_data smv
JOIN included_sites si ON (smv.studyid = si.studyid AND smv.siteid = si.siteid);


