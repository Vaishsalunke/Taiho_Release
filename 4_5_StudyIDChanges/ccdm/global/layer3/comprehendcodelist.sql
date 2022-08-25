/*
CCDM comprehendcodelist mapping
Notes: The Code/value pairs here can be used to configure the client  

Avaiilable Configuration Options
Code                Value                       Description
SUBJECT_DAY_START   DSTERM value                e.g. consented/enrolled Specifies which disposition event to use for the subject start date
VISIT_COMPLIANCE    "hide first"/"show first"   e.g. "hide first"/"show first" Specifies if first visist has to be taken in to consideration for subject days calculation
INCLUDED_TABLES     USDM object                 e.g. comma seperated USDM objects present in this field determines if that object is expected to have data or not
*/

WITH included_tables AS (select array[
                                        'comprehendcodelist',
                                        'dimdate'
                                    ] tab), 
    
        code_value_pairs AS (
                SELECT  'default'::text AS codekey,
                        'VISIT_COMPLIANCE'::text AS codename,
                        'hide first'::text AS codevalue
                UNION ALL
                SELECT 
                        'default'::text AS codekey,
                        'INCLUDED_TABLES'::text AS codename,
                        string_agg(tabstr, ',')::text AS codevalue
                FROM (SELECT unnest(tab) tabstr FROM included_tables) t
                UNION ALL
                SELECT 'default'::text AS codekey,
                        'INCLUDED_TABLES_SOFT_CHECK'::text AS codename,
                        'false'::text AS codevalue)

SELECT cvp.codekey::text AS codekey,
       cvp.codename::text AS codename,
       cvp.codevalue::text AS codevalue 
FROM code_value_pairs cvp;
