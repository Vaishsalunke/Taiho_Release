/*
CCDM SiteMonitoringVisitSchedule mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitSchedule table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     sitemonitoringvisitschedule_data AS (
             select studyid, 
					siteid,
					 visitname||'~' || row_number() OVER(partition by visitname,siteid ORDER by startdate,smvvtype ASC)::text AS visitname
					, plannedvisitdate
					, smvvtype
					from (     
               SELECT  replace("protocol_#",'-','_')::text AS studyid,
 						case when"site_#" is not null 
                        	then concat('TAS120_202_',"site_#")
                        	end::text AS siteid,
						"account_name"::text AS visitname,
						 "planned_date" ::date AS plannedvisitdate,
						 "visit_type"::text as smvvtype,
						 "visit_start"::date as startdate
			 from tas120_202_ctms.monvisit_tracker)a)

SELECT 
        /*KEY (smvs.studyid || '~' || smvs.siteid)::text AS comprehendid, KEY*/
        smvs.studyid::text AS studyid,
        smvs.siteid::text AS siteid,
        smvs.visitname::text AS visitname,
        smvs.plannedvisitdate::date AS plannedvisitdate
		,smvs.smvvtype::text as smvvtype
        /*KEY , (smvs.studyid || '~' || smvs.siteid || '~' || smvs.visitname)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitschedule_data smvs
JOIN included_sites si ON (smvs.studyid = si.studyid AND smvs.siteid = si.siteid);

