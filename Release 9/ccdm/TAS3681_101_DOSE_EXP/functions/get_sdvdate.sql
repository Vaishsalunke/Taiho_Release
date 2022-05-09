/*
CDM get_sdvdate function
Client: Regeneron
*/

DROP FUNCTION IF EXISTS get_sdvdate(text);

CREATE OR REPLACE FUNCTION get_sdvdate(pStudyID TEXT) --by only looking at audittrail we are not including data ourside of RAVE... 
RETURNS BOOLEAN
VOLATILE
AS $$
DECLARE
    rec record;
    lSQL TEXT := '';
    tbl_schema TEXT;
    t_ddl TEXT;
    
BEGIN

        tbl_schema := REPLACE(REPLACE(LOWER('TAS3681_101'), '-', '_'), ' ', '_');

        IF (SELECT EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.schemata WHERE schema_name = tbl_schema)) THEN
                lSQL := FORMAT(
                                        'SELECT 
                                            subj::TEXT AS subj
                                            , instanceid::INTEGER AS instanceid
                                            , datapageid::INTEGER AS datapageid
                                            , recordid::INTEGER AS recordid
                                            , fieldid::TEXT AS fieldid
                                            , MIN(verifytime::TIMESTAMP)::DATE AS sdvdate
                                        FROM 
                                            (
                                            SELECT 
                                                a."SubjectName" AS subj
                                                , a."InstanceId" AS instanceid
                                                , a."DataPageId" AS datapageid
                                                , a."RecordId" as recordid
                                                , mf."Name" AS fieldid
                                                , a."DateTimeStamp"::TIMESTAMP AS verifytime
                                                , b.unverifytime
                                            FROM %I."audit_ItemData" a
                                            JOIN %I."metadata_fields" mf ON (a."ItemOID" = mf."OID")
                                            JOIN %I."metadata_forms" mfr ON (a."FormOID" = mfr."OID")
                                            LEFT JOIN 
                                                (
                                                SELECT   
                                                    b."RecordId" AS recordid
                                                    , b."ItemOID" AS fieldid
                                                    , MAX(b."DateTimeStamp"::TIMESTAMP) unverifytime
                                                FROM  %I."audit_ItemData" b
                                                WHERE b."AuditSubCategoryName" = ''UnVerify''
                                                GROUP BY b."RecordId", b."ItemOID"
                                                ) b ON (a."RecordId" = b.recordid and a."ItemOID" = b.fieldid )
                                                WHERE a."AuditSubCategoryName" = ''Verify''
                                            ) sdv
                                        WHERE unverifytime IS NULL OR verifytime::TIMESTAMP > unverifytime::TIMESTAMP
                                        GROUP BY subj, instanceid, datapageid, recordid, fieldid'
                                , tbl_schema, tbl_schema, tbl_schema, tbl_schema
                                );
        ELSE
                lSQL := NULL;
        END IF;

    t_ddl := 'DROP TABLE IF EXISTS sdvdatetmp';
    EXECUTE t_ddl;

    IF LENGTH(lSQL) > 0 THEN

        t_ddl := 'CREATE TABLE sdvdatetmp AS ' || lSQL;
        EXECUTE t_ddl;

        t_ddl := 'CREATE INDEX ON sdvdatetmp(subj,instanceid,datapageid,recordid)'; 
        EXECUTE t_ddl;
        
        t_ddl := 'ANALYZE sdvdatetmp';
        EXECUTE t_ddl;
          
        RETURN TRUE;
    ELSE 
        t_ddl := 'CREATE TABLE sdvdatetmp AS 
                  SELECT
                    NULL::TEXT AS subj,	
                    NULL::INTEGER AS instanceid,	
                    NULL::INTEGER AS datapageid,	
                    NULL::INTEGER AS recordid,	
                    NULL::TEXT AS fieldid,	
                    NULL::DATE AS sdvdate';
        EXECUTE t_ddl;
        
        RETURN FALSE;
    END IF;

END
$$ LANGUAGE plpgsql;

