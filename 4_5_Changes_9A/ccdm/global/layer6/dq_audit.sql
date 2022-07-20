/*
CCDM DQ Audit data mapping
Notes: Standard mapping to CCDM DQ Audit data table
*/

WITH audit_data AS (
                    SELECT  null::text AS customer_name,
			    null::text AS source_system,
                            null::text AS src_schema,
                            null::text AS src_table,
                            null::text AS studyid,
                            null::text AS record_type,
                            null::bigint AS record_count,                            
                            null::timestamp AS dq_start_timestamp,
			    null::bigint AS dq_run_id )

SELECT 
	/*KEY (a.studyid)::text AS comprehendid, KEY*/
        a.customer_name::text AS customer_name,
        a.source_system::text AS source_system,
        a.src_schema::text AS src_schema,
        a.src_table::text AS src_table,
        a.studyid::text AS studyid,
        a.record_type::text AS record_type,
        a.record_count::bigint AS record_count,
        a.dq_start_timestamp::timestamp AS dq_start_timestamp,
	a.dq_run_id::bigint AS dq_run_id
	/*KEY , (a.customer_name || '~' || a.source_system || '~' || a.src_schema || '~' || a.src_table || '~' || a.studyid || '~' || a.record_type || '~' || a.dq_run_id || '~' || random())::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM audit_data a
WHERE 1=2;