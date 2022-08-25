CREATE OR REPLACE FUNCTION get_studymetadata(pStudyID text)  
RETURNS BOOLEAN
VOLATILE
AS $dbvis$
DECLARE

	t_ddl text := '';
	t_schema text;
	t_study text;
	fieldsql text := '';
	formsql text := '';
	formdef_data text := '';
	err_context text := '';
        b_edc_exists BOOLEAN := TRUE;

BEGIN
	t_study  := pStudyID;
        t_schema := lower(replace(replace(pStudyID, '-', '_'), ' ', '_'));

    IF NOT EXISTS(SELECT distinct table_schema
                                     FROM information_schema.tables
                                 WHERE table_schema=t_schema
                                        AND table_name in (
                                                                                'metadata_fields',
                                                                                'metadata_folders',
                                                                                'metadata_forms'
                                                                                )
                                   )
          THEN
                        b_edc_exists := FALSE;
                        -- insert alert message "EDC Data not available"
                raise notice '3';                        
          END IF;            

        If b_edc_exists then
                        --SQL to fetch Fielddef data for given study
                        fieldsql := 'SELECT mf.* FROM (SELECT DISTINCT '''||t_study||'''::text AS studyid,
                                        "FormDefOID"::text AS formid,
                                        substr("OID", strpos("OID", ''.'')+1)::text AS fieldid,
                                        coalesce("SASLabel", "Name")::text AS fieldname,
                                        false::boolean AS isprimaryendpoint,
                                        false::boolean AS issecondaryendpoint,
                                        CASE WHEN "Mandatory"::boolean = true and coalesce("SourceDocument", false)::boolean = true THEN true
                                        ELSE false END ::boolean AS issdv,
                                        "Mandatory"::boolean AS isrequired
                                        FROM "'||t_schema||'"."metadata_fields") mf';
                                raise notice 'fieldsql: %', fieldsql;

                        --Drop fielddef stage table
                        t_ddl := 'DROP TABLE IF EXISTS stg_fielddef';
                        EXECUTE t_ddl;

                        --Create stage table for fielddef and populate it
                        t_ddl := 'CREATE TABLE stg_fielddef AS ( '|| fieldsql||' JOIN (SELECT studyid FROM study) st ON (mf.studyid = st.studyid))';
                        EXECUTE t_ddl;

                --Create temp table to store formdef_data
                t_ddl := 'CREATE TABLE formdef_data_'||t_schema||' AS '||'SELECT '''||t_study||'''::text AS studyid,
                                     "OID"::text AS formid,
                                     "Name"::text AS formname,
                                     false::boolean AS isprimaryendpoint,
                                     false::boolean AS issecondaryendpoint,
                                     false::boolean AS issdv,
                                     false::boolean AS isrequired
                        FROM "'||t_schema||'"."metadata_forms"';
                EXECUTE t_ddl;

                formsql := 'SELECT studyid::text AS studyid,
                                    formid::text AS formid,
                                    formname::text AS formname,
                                    isprimaryendpoint::boolean AS isprimaryendpoint,
                                    issecondaryendpoint::boolean AS issecondaryendpoint,
                                    issdv::boolean AS issdv,
                                    isrequired::boolean AS isrequired
                            FROM formdef_data_'||t_schema  ;
        
                --Drop formdef stage table
                t_ddl := 'DROP TABLE IF EXISTS stg_formdef';
                EXECUTE t_ddl;
        
                --Create stage table for fielddef and populate it
                t_ddl := 'CREATE TABLE stg_formdef AS ' || formsql;
                EXECUTE t_ddl;
                raise notice '%', t_ddl;
        
                RETURN TRUE;

        ELSE

                --Drop formdef stage table
                t_ddl := 'DROP TABLE IF EXISTS stg_formdef';
                EXECUTE t_ddl;
                
                t_ddl := 'CREATE TABLE stg_formdef
                                                        (
                                                            studyid TEXT NOT NULL,
                                                            formid TEXT NOT NULL,
                                                            formname TEXT,
                                                            isprimaryendpoint BOOLEAN,
                                                            issecondaryendpoint BOOLEAN,
                                                            issdv BOOLEAN,
                                                            isrequired BOOLEAN
                                                        )';
                
                 EXECUTE t_ddl;
                 
                t_ddl := 'DROP TABLE IF EXISTS stg_fielddef';
                EXECUTE t_ddl;

		t_ddl := 'CREATE TABLE stg_fielddef
                                                (
                                                    studyid TEXT NOT NULL,
                                                    formid TEXT NOT NULL,
                                                    fieldid TEXT NOT NULL,
						    fieldname TEXT, 
						    isprimaryendpoint BOOLEAN,
						    issecondaryendpoint BOOLEAN,
						    issdv BOOLEAN,
						    isrequired BOOLEAN
                                                ) ';

                EXECUTE t_ddl;
                                 
                RETURN FALSE;

        end if;
	


EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
		raise notice 'Error Name:%',SQLERRM;
		raise notice 'Error State:%', SQLSTATE;
	        raise notice 'Error Context:%', err_context;
		RETURN FALSE;


END
$dbvis$
LANGUAGE plpgsql;
