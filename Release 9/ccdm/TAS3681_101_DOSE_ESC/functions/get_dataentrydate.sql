/*
CDM get_dataentrydate function
Client: Regeneron
*/

DROP FUNCTION IF EXISTS get_dataentrydate(text);

CREATE OR REPLACE FUNCTION get_dataentrydate(pStudyID TEXT)  --by only looking at audittrail we are not including data ourside of RAVE... 
RETURNS BOOLEAN
VOLATILE
AS $$
DECLARE
    rec RECORD;
    lSQL TEXT := '';
    tbl_schema TEXT;
    t_ddl TEXT;
    
BEGIN

        tbl_schema := REPLACE(REPLACE(LOWER('TAS3681_101'), '-', '_'), ' ', '_');
        
        IF (SELECT EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.schemata WHERE schema_name = tbl_schema)) THEN
                lSQL := format(
                                        'SELECT 
                                            sat."SubjectName" AS subj
                                            , sat."InstanceId"::INTEGER AS instanceid
                                            , sat."DataPageId"::INTEGER AS datapageid
                                            , sat."RecordId"::INTEGER AS recordid
                                            , mf."Name" AS fieldid
                                            , mfr."LogDirection" As logdirection
                                            , sat."ItemGroupOID" AS ItemGroupOID
                                            , MIN("DateTimeStamp"::TIMESTAMP)::DATE AS dataentrydate
                                        FROM %I."audit_ItemData" sat
                                        JOIN %I."metadata_fields" mf ON (sat."ItemOID" = mf."OID")
                                        JOIN %I."metadata_forms" mfr ON (sat."FormOID" = mfr."OID")
                                        WHERE (sat."AuditSubCategoryName" = ''AcceptedDefaultValue'' OR sat."AuditSubCategoryName" ~* ''^Entere.*'')  
                                        GROUP BY sat."SubjectName", sat."InstanceId", sat."DataPageId",  sat."RecordId", mf."Name", mfr."LogDirection", sat."ItemGroupOID"'
                                , tbl_schema, tbl_schema, tbl_schema
                                );
        ELSE
                lSQL := NULL;
        END IF;
        
    t_ddl := 'DROP TABLE IF EXISTS dataentrydatetmp';
    EXECUTE t_ddl;
    
    IF LENGTH(lSQL) > 0 THEN

        t_ddl := 'CREATE TABLE dataentrydatetmp AS ' || lSQL;
        EXECUTE t_ddl;

        t_ddl := 'CREATE INDEX ON dataentrydatetmp(subj,instanceid,datapageid,recordid)';   
        EXECUTE t_ddl;
        
        t_ddl := 'ANALYZE dataentrydatetmp';
        EXECUTE t_ddl;

        RETURN TRUE;
    ELSE 
        t_ddl := 'CREATE TABLE dataentrydatetmp AS 
                  SELECT
                    NULL::TEXT AS subj,
                    NULL::INTEGER AS instanceid,
                    NULL::INTEGER AS datapageid,
                    NULL::INTEGER AS recordid,
                    NULL::TEXT AS fieldid,
                    NULL::TEXT AS logdirection,
                    NULL::TEXT AS itemgroupoid,
                    NULL::DATE AS dataentrydate';
        EXECUTE t_ddl;
        
        RETURN FALSE;
    END IF;

END
$$ LANGUAGE plpgsql;

