/*
CCDM SiteMonitoringVisit mapping
Notes: Standard mapping to CCDM SiteMonitoringVisit table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
 
     sitemonitoringvisit_data AS (
     select studyid,
			siteid,
			 visitname||'~' || row_number() OVER(partition by visitname,siteid ORDER by visitdate,smvvtype ASC)::text AS visitname,
			visitdate,
			smvipyn,
			smvpiyn,
			smvtrvld,
			smvtrvlu,
			smvvtype,
			smvmethd
			from (
                SELECT  'TAS-120-202'::text AS studyid,
 						case when"site_#" is not null 
                        	then concat('TAS120_202_',"site_#")
                        	end::text AS siteid,
						account_name::text AS visitname,
						visit_start::date AS visitdate,
						null::text AS smvipyn,
						null::text AS smvpiyn,
						null ::text AS smvtrvld,
						null:: text AS smvtrvlu,
						visit_type ::text AS smvvtype,
						visit_status ::text as smvmethd
                          	from tas120_202_ctms.monvisit_tracker
                          	where visit_start is not null
							)a)

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
		smv.smvmethd::text AS smvmethd
        /*KEY , (smv.studyid || '~' || smv.siteid || '~' || smv.visitName)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisit_data smv
JOIN included_sites si ON (smv.studyid = si.studyid AND smv.siteid = si.siteid);

