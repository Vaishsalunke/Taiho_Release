/*
CCDM SiteMonitoringVisitReport mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitReport table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     sitemonitoringvisitreport_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS sitename,
                        null::text AS investigatorname,
                        null::int AS subjectsenrolled,
                        null::text AS visitname,
                        null::date AS visitcompleteddate,
                        null::numeric AS effort,
                        null::text AS effortunit,
                        null::text AS craname,
                        null::text AS comonitor,
                        null::text AS approvername,
                        null::text AS reporttype,
                        null::date AS firstsubmissiondate,
                        null::int AS firstsubmissioncompleteddays,
                        null::int AS revisioncount,
                        null::date AS finalapprovaldate,
                        null::int AS finalapprovalcompleteddays )

SELECT 
        /*KEY (smvr.studyid || '~' || smvr.siteid)::text AS comprehendid, KEY*/
        smvr.studyid::text AS studyid,
        smvr.siteid::text AS siteid,
        smvr.sitename::text AS sitename,
        smvr.investigatorname::text AS investigatorname,
        smvr.subjectsenrolled::int AS subjectsenrolled,
        smvr.visitname::text AS visitname,
        smvr.visitcompleteddate::date AS visitcompleteddate,
        smvr.effort::numeric AS effort,
        smvr.effortunit::text AS effortunit,
        smvr.craname::text AS craname,
        smvr.comonitor::text AS comonitor,
        smvr.approvername::text AS approvername,
        smvr.reporttype::text AS reporttype,
        smvr.firstsubmissiondate::date AS firstsubmissiondate,
        smvr.firstsubmissioncompleteddays::int AS firstsubmissioncompleteddays,
        smvr.revisioncount::int AS revisioncount,
        smvr.finalapprovaldate::date AS finalapprovaldate,
        smvr.finalapprovalcompleteddays::int AS finalapprovalcompleteddays
        /*KEY , (smvr.studyid || '~' || smvr.siteid || '~' || smvr.visitname || '~' || smvr.revisioncount)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitreport_data smvr
JOIN included_sites si ON (smvr.studyid = si.studyid AND smvr.siteid = si.siteid)
WHERE 1=2;
