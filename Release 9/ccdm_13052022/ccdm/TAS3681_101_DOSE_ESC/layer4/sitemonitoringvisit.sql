/*
CCDM SiteMonitoringVisit mapping
Notes: Standard mapping to CCDM SiteMonitoringVisit table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
 
     sitemonitoringvisit_data AS (
     select studyid,
     siteid,
     visitname,
     visitdate,
     smvipyn,
     smvpiyn,
     smvtrvld,
     smvtrvlu,
     smvvtype,
     smvmethd from
     (
     select studyid,
     siteid,
     visitname,
     visitdate,
     smvipyn,
     smvpiyn,
     smvtrvld,
     smvtrvlu,
     smvvtype,
     smvmethd, 
     row_number() over (partition by siteid,studyid,visitname order by visitdate desc ) as rank
     from (
              SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
                        sv."site_number"::text AS siteid,
                        sv."visit_name"||'~' || row_number() OVER(partition by "visit_type","visit_name","site_number" ORDER BY "visit_start_date" ASC)::text AS visitname,
                        coalesce(nullif(sv."visit_completed_date",''),nullif(sv."planned_visit_date",''),nullif(sv."visit_start_date",''))::date AS visitdate,
                        null::text AS smvipyn,
                        null::text AS smvpiyn,
                        ((nullif(sv."visit_completed_date",'')::date)-nullif(sv."visit_start_date",'')::date)::text as smvtrvld,
						'days'::text AS smvtrvlu,
                        sv."visit_type"::text AS smvvtype,
                        sv."visit_category"::text AS smvmethd
                       from tas3681_101_ctms.site_visits sv
					   where "visit_status" = 'Complete'
					   )a
                       )sv 
                       where rank = 1
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
