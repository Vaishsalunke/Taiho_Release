/*
CDM get_fielddata function
Client: Regeneron
*/

DROP FUNCTION IF EXISTS get_fielddata(text);

CREATE OR REPLACE FUNCTION get_fielddata (pstudyid TEXT)
RETURNS BOOLEAN
VOLATILE
AS $$
DECLARE
    rec RECORD;
    lSQL TEXT := '';
    b_add_union_next BOOLEAN := FALSE;
    t_ddl TEXT;

BEGIN
FOR rec IN
(	
	
	WITH fd AS 
      ( 
			   SELECT DISTINCT i.formid::TEXT AS table_name,
               i.fieldid::TEXT AS column_name,
               'tas3681_101'::text AS table_schema, 
               'TAS3681_101_DOSE_ESC'::TEXT AS studyid
               FROM fielddef i
               WHERE i.studyid = upper(pStudyID)
		),
        
	inf_schema AS
        ( 		
				SELECT
                table_schema,
                table_name,
                column_name                   
                FROM information_schema.columns
                WHERE upper(table_schema) = 'TAS3681_101'
		)
                 
	SELECT
    fd.table_name,
    fd.column_name,              
    lower(fd.table_schema) as table_schema ,
    fd.studyid
    FROM fd
    JOIN inf_schema tb 
	ON (tb.table_schema = fd.table_schema 
		AND tb.table_name = fd.table_name
        AND tb.column_name = fd.column_name)
)
    LOOP

        IF b_add_union_next THEN
          lSQL := lSQL || ' UNION ALL '; 
        END IF; 
        
        lSQL := lSQL || format('
               ( SELECT 
                    %L::TEXT                        AS studyid,
                    "SiteNumber"::TEXT              AS siteid,                        
                    "Subject"::TEXT                 AS usubjid,
                    "SiteNumber"::TEXT     AS siteidjoin,
                    %L::TEXT                        AS formid,
                    %L::TEXT                        AS fieldid,
                    TRIM(COALESCE(NULLIF("InstanceName", ''''), "DataPageName"))::TEXT  AS visit,
                    count(*)  over (partition by "project", "SiteNumber", "Subject", "FolderName")::BIGINT AS visit_cnt,
                    "instanceId"::INTEGER           AS instanceid,
                    "InstanceRepeatNumber"::INTEGER           AS InstanceRepeatNumber,
                    "DataPageId"::INTEGER           AS datapageid,
                    "RecordId"::INTEGER             AS recordid,
                    1::INTEGER                      AS visitseq,
                    DENSE_RANK() OVER (PARTITION BY "Subject", COALESCE(NULLIF("FolderName", ''''), "DataPageName") ORDER BY "RecordId")::INTEGER AS formseq,
                    ROW_NUMBER() OVER (PARTITION BY "Subject", COALESCE(NULLIF("FolderName", ''''), "DataPageName") ORDER BY "RecordId")::INTEGER AS fieldseq,
                    "FolderSeq"::INTEGER AS log_num,
                    %I::TEXT                        AS dataValue,
                    NULL::DATE                      AS dataentrydate, --populated by fielddata.sql
                    COALESCE("RecordDate"::DATE,"MinCreated"::DATE) AS datacollecteddate,
                    NULL::DATE                      AS sdvdate      -- populated by fielddata.sql
                FROM %I.%I tbl /*LIMIT LIMIT 10 LIMIT*/)
                ', rec.studyid, rec.table_name, rec.column_name, rec.column_name, rec.table_schema, rec.table_name);
                      
        b_add_union_next := TRUE;        

    END LOOP;
    
    t_ddl := 'DROP TABLE IF EXISTS fielddatatmp';
    EXECUTE t_ddl;
        
    IF LENGTH(lSQL) > 0 THEN

        t_ddl := 'CREATE TABLE fielddatatmp AS ' || lSQL;
        EXECUTE t_ddl;

        t_ddl := 'CREATE INDEX ON fielddatatmp(usubjid,instanceid,datapageid,recordid)';    
        EXECUTE t_ddl;
        
        t_ddl := 'ANALYZE fielddatatmp';
        EXECUTE t_ddl;
        
        RETURN TRUE;
    ELSE 
        t_ddl := 'CREATE TABLE fielddatatmp AS 
                  SELECT
                    NULL::TEXT AS studyid,
                    NULL::TEXT AS siteid,
                    NULL::TEXT AS usubjid,
                    NULL::TEXT AS siteidjoin,
                    NULL::TEXT AS formid,
                    NULL::TEXT AS fieldid,
                    NULL::TEXT AS visit,
                    NULL::BIGINT AS visit_cnt,
                    NULL::INTEGER AS instanceid,
                    NULL::INTEGER AS InstanceRepeatNumber,
                    NULL::INTEGER AS datapageid,
                    NULL::INTEGER AS recordid,
                    NULL::INTEGER AS visitseq,
                    NULL::INTEGER AS formseq,
                    NULL::INTEGER AS fieldseq,
                    NULL::INTEGER AS log_num,
                    NULL::TEXT AS datavalue,
                    NULL::DATE AS dataentrydate,
                    NULL::DATE AS datacollecteddate,
                    NULL::DATE AS sdvdate';
        EXECUTE t_ddl;

        RETURN FALSE;
    END IF;

END
$$ LANGUAGE plpgsql;
