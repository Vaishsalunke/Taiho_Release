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
     		visitname||'~' || row_number() OVER(partition by visitname,siteid ORDER by visitcompleteddate ASC)::text AS visitname,
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
                SELECT  'TAS-120-202'::text AS studyid,
 						case when"site_#" is not null 
                        	then concat('TAS120_202_',"site_#")
                        	end::text AS siteid,
                        account_name::text AS sitename,
                        pi_name::text AS investigatorname,
                        null::int AS subjectsenrolled,
                        account_name_2::text AS visitname,
                        visit_end::date AS visitcompleteddate,
                        null::numeric AS effort,
                        null::text AS effortunit,
                        monitor::text AS craname,
                        lm_name::text AS comonitor,
                        monitor::text AS approvername,
                        null::text AS reporttype,
                        null::date AS firstsubmissiondate,
                        null::int AS firstsubmissioncompleteddays,
                        1::int AS revisioncount,
                        null::date AS finalapprovaldate,
                        null::int AS finalapprovalcompleteddays
               from   tas120_202_ctms.monvisit_tracker
               where account_name_2 is not null
			   )a
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
        /*KEY , (smvr.studyid || '~' || smvr.siteid || '~' || smvr.visitname || '~' || smvr.revisioncount)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitreport_data smvr
JOIN included_sites si ON (smvr.studyid = si.studyid AND smvr.siteid = si.siteid);

