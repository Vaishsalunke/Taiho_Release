/*
CCDM SiteMonitoringVisitReport mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitReport table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
     sitemonitoringvisitreport_data AS (
	 select studyid,
     		siteid,
     		sitename,
     		investigatorname,
     		subjectsenrolled,
     		visitname,
     		visitcompleteddate,
     		effort,
     		effortunit,
     		craname,
     		comonitor,
     		approvername,
     		reporttype,
     		firstsubmissiondate,
     		firstsubmissioncompleteddays,
     		revisioncount,
     		finalapprovaldate,
     		finalapprovalcompleteddays
			from
			(
     select studyid,
     		siteid,
     		sitename,
     		investigatorname,
     		subjectsenrolled,
     		visitname,
     		visitcompleteddate,
     		effort,
     		effortunit,
     		craname,
     		comonitor,
     		approvername,
     		reporttype,
     		firstsubmissiondate,
     		firstsubmissioncompleteddays,
     		revisioncount,
     		finalapprovaldate,
     		finalapprovalcompleteddays,
			row_number() over (partition by studyid,siteid,visitname order by visitcompleteddate desc) as rank
     		from
     		(
                SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        svr."site_number"::text AS siteid,
                        svr."site_name"::text AS sitename,
                        svr."principal_investigator"::text AS investigatorname,
                        null::int AS subjectsenrolled,
                        svr."visit_name"::text AS visitname,
                        nullif(svr.visit_completed_date,'')::date AS visitcompleteddate,
                        null::numeric AS effort,
                        null::text AS effortunit,
                        svr."cra_first_name"||svr."cra_middle_name"||svr."cra_last_name"::text AS craname,
                        null::text AS comonitor,
                        svr."approver_first_name"||svr."approver_middle_name"||svr."approver_last_name"::text AS approvername,
                        svr."report_type"::text AS reporttype,
                        nullif(svr."report_started_date",'')::date AS firstsubmissiondate,
                        null::int AS firstsubmissioncompleteddays,
                        1::int AS revisioncount,
                        nullif(svr."approved_date",'')::date AS finalapprovaldate,
                        null::int AS finalapprovalcompleteddays
               from   tas3681_101_ctms.site_visit_reports svr )svr
			   ) a where a.rank = 1
			   )
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
         /*KEY, (smvr.studyid || '~' || smvr.siteid || '~' || smvr.visitname || '~' || smvr.revisioncount)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitreport_data smvr
JOIN included_sites si ON (smvr.studyid = si.studyid AND smvr.siteid = si.siteid);

