/*
CCDM FieldData mapping
Client: Regeneron
Notes: Mapping to CCDM FieldData table
*/

WITH included_studies AS (
        SELECT  studyid FROM study),

    included_subjects AS (
        SELECT studyid, siteid, usubjid, siteid AS siteidjoin FROM subject),
                                  
    fielddata_data AS (
        SELECT 'TAS2940-101' AS studyid,
            concat('TAS2940_101_',split_part(f.siteidjoin,'_',2)) AS siteidjoin,
            f.usubjid AS usubjid,
            f.formid AS formid, 
            f.formseq AS formseq,
            f.fieldid AS fieldid,
            f.fieldseq AS fieldseq,
			case when f.visit_cnt>1 then trim(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							(f.visit,'\s\([0-9][0-9]\)','')
										,'\s\([0-9]\)','')
										,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
										,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
							)||'-'||f.InstanceRepeatNumber 
             else trim(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							(f.visit,'\s\([0-9][0-9]\)','')
										,'\s\([0-9]\)','')
										,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
										,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
							) end AS visit,
         --   case when f.visit_cnt>1 then f.visit||'-'||f.InstanceRepeatNumber else f.visit end AS visit,
            f.visitseq AS visitseq,
            f.log_num AS log_num,
            f.datavalue AS datavalue,
            MIN(de.dataentrydate) AS dataentrydate,
            MIN(f.datacollecteddate) AS datacollecteddate,
            MAX(sdv.sdvdate) AS sdvdate, 
            NULL::TEXT AS sourcerecordprimarykey, 
            NULL::TIMESTAMP AS sourcerecorddate,
            NULL::BOOLEAN AS isdeleted  /* Internal Field - leave AS null */ 
            
        FROM fielddatatmp f
        JOIN dataentrydatetmp de ON (de.subj = f.usubjid 
                                    AND de.instanceid = f.instanceid 
                                    AND de.fieldid = f.fieldid
                                    AND ((de.ItemGroupOID NOT LIKE '%_LOG_LINE' AND  de.datapageid= f.datapageid) 
                                    OR (de.recordid=f.recordid)))
        LEFT JOIN sdvdatetmp sdv ON (sdv.subj = f.usubjid 
                                    AND sdv.fieldid = f.fieldid
                                    AND sdv.instanceid = f.instanceid 
                                    AND ((de.ItemGroupOID NOT LIKE '%_LOG_LINE' AND  sdv.datapageid= f.datapageid) 
                                    OR (sdv.recordid=f.recordid)))
        WHERE f.formseq=(CASE WHEN de.ItemGroupOID NOT LIKE '%_LOG_LINE' AND lower(f.visit) NOT LIKE '%unscheduled%' THEN 1 ELSE f.formseq END)
	    GROUP BY f.studyid, f.siteidjoin, f.usubjid, f.formid, f.formseq, f.visit_cnt, f.InstanceRepeatNumber,
            f.fieldid, f.fieldseq, f.visit, f.visitseq, f.log_num, f.datavalue
)

SELECT 
        /*KEY (fd.studyid || '~' || s.siteid || '~' || fd.usubjid)::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        s.siteid::text AS siteid,
        fd.usubjid::text AS usubjid,
        fd.formid::text AS formid,
        fd.formseq::integer AS formseq,
        fd.fieldid::text AS fieldid,
        fd.fieldseq::integer AS fieldseq,
        fd.visit::text AS visit,
        fd.visitseq::integer AS visitseq, 
        fd.log_num::integer AS log_num,
        fd.datavalue::text AS datavalue,
        fd.dataentrydate::date AS dataentrydate,
        fd.datacollecteddate::date AS datacollecteddate,
        fd.sdvdate::date AS sdvdate,
        fd.sourcerecordprimarykey::text AS sourcerecordprimarykey,
        fd.sourcerecorddate::timestamp AS sourcerecorddate,
        fd.isdeleted::boolean AS isdeleted 
        /*KEY , (fd.studyid || '~' || s.siteid || '~' || fd.usubjid || '~' || '~' || fd.visit || '~' || fd.formid || '~' || fd.formseq || '~' || fd.fieldid || '~' || fd.fieldseq || '~' || fd.log_num)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM fielddata_data fd
JOIN included_subjects s ON (fd.studyid = s.studyid AND fd.siteidjoin = s.siteidjoin AND fd.usubjid = s.usubjid);

