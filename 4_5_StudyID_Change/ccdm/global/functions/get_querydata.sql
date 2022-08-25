CREATE OR REPLACE FUNCTION get_querydata(pStudyID text)  
RETURNS BOOLEAN
VOLATILE
AS $dbvis$
DECLARE

t_ddl text := '';
t_schema text;
t_study text;
qsql text := '';
err_context text := '';

BEGIN
t_study  := pStudyID;
        t_schema := lower(replace(replace(pStudyID, '-', '_'), ' ', '_'));

        IF EXISTS(SELECT table_name
                  FROM information_schema.tables
                  WHERE table_name = 'stream_query_detail'
                  AND table_schema = t_schema) THEN

         --SQL to fetch query data for given study
qsql := 'SELECT left("study"::text, strpos("study", '' - '') - 1)::text AS studyId,
		left("sitename"::text, strpos("sitename", ''_'') - 1)::text AS siteId,
		"subjectname"::text AS usubjid,
		"id_"::text AS queryid,
		"folder"::text as visit,
		"form"::text AS formid,
		"field"::text AS fieldid,
		"querytext"::text AS querytext,
		"markinggroupname"::text AS querytype,
		"name"::text AS querystatus,
		convert_to_date("qryopendate")::date AS queryopeneddate,
		convert_to_date("qryresponsedate")::date AS queryresponsedate,
		convert_to_date("qrycloseddate")::date AS querycloseddate,
		1::int as formseq,
		"log"::int as log_num
		FROM "'||t_schema||'"."stream_query_detail"
		WHERE lower(left("markinggroupname", 9)) = ''site from''
		OR lower(right(trim("markinggroupname"), 7)) = ''to site''';
        ELSE
        qsql := 'SELECT NULL::text AS studyid,
            NULL::text AS siteid,
            NULL::text AS usubjid,
            NULL::text AS queryid,
            NULL::text    as visit,
            NULL::text AS formid,
            NULL::text AS fieldid,
            NULL::text AS querytext,
            NULL::text AS querytype,
            NULL::text AS querystatus,
            NULL::date AS queryopeneddate,
            NULL::date AS queryresponsedate,
            NULL::date AS querycloseddate,
            NULL::int as formseq,
            NULL::int as log_num
         WHERE 1=2';
         END IF;

--Drop fielddef stage table
        t_ddl := 'DROP TABLE IF EXISTS stg_querydata_tmp';
        EXECUTE t_ddl;

--Create stage table for fielddef and populate it
        t_ddl := 'CREATE TABLE stg_querydata_tmp AS ('|| qsql||' )';
        EXECUTE t_ddl;

RETURN TRUE;

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

