/*
CCDM FieldData mapping
Client: Regeneron
Notes: Mapping to CCDM FieldData table
*/

WITH included_studies AS (
        SELECT  studyid FROM study),

    included_subjects AS (
        SELECT studyid, siteid, usubjid, siteid AS siteidjoin FROM subject),
                                  
    fielddata AS (
    SELECT f.studyid AS studyid,
            concat('TAS0612_101_',split_part(f.siteidjoin,'_',2)) AS siteidjoin,
            f.usubjid AS usubjid,
            f.formid AS formid, 
            f.formseq AS formseq,
            f.fieldid AS fieldid,
            f.fieldseq AS fieldseq,
			case when f.visit_cnt>1 then REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.visit,'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')||'-'||f.InstanceRepeatNumber 
             else REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(f.visit,'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','') end AS visit,
           -- case when f.visit_cnt>1 then f.visit||'-'||f.InstanceRepeatNumber else f.visit end AS visit,
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
        WHERE f.formseq=(CASE WHEN de.ItemGroupOID NOT LIKE '%_LOG_LINE' AND lower(f.visit) NOT LIKE '%unscheduled%' THEN 1 ELSE f.formseq END) and f.studyid='TAS0612_101'
        GROUP BY f.studyid, f.siteidjoin, f.usubjid, f.formid, f.formseq, f.visit_cnt, f.InstanceRepeatNumber,
            f.fieldid, f.fieldseq, f.visit, f.visitseq, f.log_num, f.datavalue
)

SELECT 
        /*KEY (fd.studyid || '~' || s.siteid || '~' || fd.usubjid)::text AS comprehendid, KEY*/
        fd.studyid::TEXT AS studyid,
        s.siteid::TEXT AS siteid,
        fd.usubjid::TEXT AS usubjid,
        fd.formid::TEXT AS formid,
        fd.formseq::INTEGER AS formseq,
        fd.fieldid::TEXT AS fieldid,
        fd.fieldseq::INTEGER AS fieldseq,
        fd.visit::TEXT AS visit,
        fd.visitseq::INTEGER AS visitseq, 
        fd.log_num::INTEGER AS log_num,
        fd.datavalue::TEXT AS datavalue,
        fd.dataentrydate::DATE AS dataentrydate,
        fd.datacollecteddate::DATE AS datacollecteddate,
        fd.sdvdate::DATE AS sdvdate,
        fd.sourcerecordprimarykey::TEXT AS sourcerecordprimarykey,
        fd.sourcerecorddate::TIMESTAMP AS sourcerecorddate,
        fd.isdeleted::BOOLEAN AS isdeleted 
        /*KEY , (fd.studyid || '~' || s.siteid || '~' || fd.usubjid || '~' || '~' || fd.visit || '~' || fd.formid || '~' || fd.formseq || '~' || fd.fieldid || '~' || fd.fieldseq || '~' || fd.log_num)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM fielddata fd
JOIN included_subjects s ON (fd.studyid = s.studyid AND fd.siteidjoin = s.siteidjoin AND fd.usubjid = s.usubjid);

