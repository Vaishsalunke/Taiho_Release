/*
CCDM SiteMonitoringVisitSchedule mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitSchedule table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     sitemonitoringvisitschedule_data AS (
				select studyid,
					   siteid,
					   visitname||'~'||row_number() over(partition by visitname,siteid ORDER by startdate,smvvtype ASC) as visitname,
					   plannedvisitdate,
					   smvvtype
				from (
						SELECT distinct 'TAS120_203'::text AS studyid,
										concat('TAS120_203_',"site_number")::text AS siteid,
										visit_type::text AS visitname,
										coalesce(nullif("planned_visit_date"::text,''),nullif("start_date_of_conducted_visit"::text,''))::date AS plannedvisitdate,
										"visit_type"::text as smvvtype,
										start_date_of_conducted_visit as startdate
						from tas120_203_ctms.study_visit
				 
						)r
										)

SELECT 
        /*KEY (smvs.studyid || '~' || smvs.siteid)::text AS comprehendid, KEY*/
        smvs.studyid::text AS studyid,
        smvs.siteid::text AS siteid,
        smvs.visitname::text AS visitname,
        smvs.plannedvisitdate::date AS plannedvisitdate,
		smvs.smvvtype::text as smvvtype
        /*KEY , (smvs.studyid || '~' || smvs.siteid || '~' || smvs.visitname)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitschedule_data smvs
JOIN included_sites si ON (smvs.studyid = si.studyid AND smvs.siteid = si.siteid);



