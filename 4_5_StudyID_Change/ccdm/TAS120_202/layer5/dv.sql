/*
 CCDM DV mapping
 Notes: Standard mapping to CCDM DV table
*/
WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
     dv_data AS (
                  select studyid,
						siteid,
						usubjid,
						visit,
						formid,
						(row_number() over (partition by studyid, siteid, usubjid order by dvstdtc))::integer AS dvseq,
						dvcat,
						dvterm,
						dvstdtc,
						dvendtc,
						dvscat,
						dvid
				from (
					  SELECT  distinct 'TAS-120-202'::text AS studyid,
									   'TAS120_202_'||"site_#"::text AS siteid,
									   "subject_#"::text AS usubjid,
									   null::text AS visit,
									   null::text AS formid,
									   null::integer AS dvseq,
									   coalesce("type",'Missing')::text AS dvcat,
									   "describe_deviation"::text AS dvterm,
									   "date_of_deviation"::date AS dvstdtc,
									   "deviation_record_updated_date"::date AS dvendtc,
									   coalesce(nullif("severity",''),'Missing')::text AS dvscat,
									   "violation/_deviation_id"::text AS dvid
					  FROM 			   tas120_202_ctms.protocol_deviation
					  )a where dvstdtc is not null
					  )
SELECT
        /*KEY (dv.studyid || '~' || dv.siteid || '~' || dv.usubjid)::text AS comprehendid, KEY*/
        dv.studyid::text AS studyid,
        dv.siteid::text AS siteid,
        dv.usubjid::text AS usubjid,
        dv.visit::text AS visit,
        dv.formid::text AS formid,
        dv.dvseq::integer AS dvseq,
        dv.dvcat::text AS dvcat,
        dv.dvterm::text AS dvterm,
        dv.dvstdtc::date AS dvstdtc,
        dv.dvendtc::date AS dvendtc,
        dv.dvscat::text AS dvscat,
        dv.dvid::text AS dvid,
		null::text AS dvcls
          /*KEY , (dv.studyid || '~' || dv.siteid || '~' || dv.usubjid || '~' || dv.dvseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dv_data dv
JOIN included_subjects s ON (dv.studyid = s.studyid AND dv.siteid = s.siteid AND dv.usubjid = s.usubjid);

