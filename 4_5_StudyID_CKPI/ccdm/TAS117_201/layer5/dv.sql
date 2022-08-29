/*
CCDM DV mapping
Notes: Standard mapping to CCDM DV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dv_data AS (
     		 	select 	studyid,
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
						dvid,
						dvcls
				from (  
		                SELECT  'TAS117-201'::text AS studyid,
		                        concat('TAS117_201_', right(site_number,3)) ::text AS siteid,
		                        concat(right(site_number,3),'-',screening_no) ::text AS usubjid,
		                        nullif(visit_reference,'') ::text AS visit,
		                        null::text AS formid,
		                        null::integer AS dvseq,
		                        deviation_type ::text AS dvcat,
		                        deviation_description ::text AS dvterm,
		                        nullif(deviation_start_date,'') ::date AS dvstdtc,
		                        nullif(deviation_end_date,'') ::date AS dvendtc,
		                        coalesce(nullif(severity,''),'Missing') ::text AS dvscat,
		                        deviation_id ::text AS dvid,
		                        null::text AS dvcls
		                 from tas117_201_ctms.deviations d      
				      )as dv
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
        dv.dvcls::text AS dvcls
        /*KEY , (dv.studyid || '~' || dv.siteid || '~' || dv.usubjid || '~' || dv.dvseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dv_data dv
JOIN included_subjects s ON (dv.studyid = s.studyid AND dv.siteid = s.siteid AND dv.usubjid = s.usubjid);



