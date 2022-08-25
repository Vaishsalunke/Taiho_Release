/*
CCDM SiteMonitoringVisit mapping
Notes: Standard mapping to CCDM SiteMonitoringVisit table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
 
     sitemonitoringvisit_data AS (
                SELECT  'TAS0612-101'::text AS studyid,
                        concat('TAS0612_101_',"site_id")::text AS siteid,
						/*"visit_name"||'_'||row_number() over (partition by "site_#","visit_name"
						order by convert_to_date(VISIT_START::text))::text  as visitname,*/
						"site_status"::text AS visitname,
						"siv_date"::date AS visitdate,
						null::text AS smvipyn,
						null::text AS smvpiyn,
						extract(day from "siv_visit_end_date"::timestamp  - "siv_visit_start_date"::timestamp ) ::text AS smvtrvld,
						'days':: text AS smvtrvlu,
						siv_actual_or_planned ::text AS smvvtype						
                          	from tas0612_101_ctms."site_closeout"					 
							)


SELECT 
        /*KEY (smv.studyid || '~' || smv.siteid)::text AS comprehendid, KEY*/
        smv.studyid::text AS studyid,
        smv.siteid::text AS siteid,
        smv.visitname::text AS visitname,
        smv.visitdate::date AS visitdate,
        smv.smvipyn:: text as smvipyn,
		smv.smvpiyn::text AS smvpiyn,
		smv.smvtrvld::text AS smvtrvld,
		smv.smvtrvlu:: text AS smvtrvlu,
		smv.smvvtype::text AS smvvtype,
		null::text AS smvmethd
        /*KEY , (smv.studyid || '~' || smv.siteid || '~' || smv.visitName)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisit_data smv
JOIN included_sites si ON (smv.studyid = si.studyid AND smv.siteid = si.siteid);


