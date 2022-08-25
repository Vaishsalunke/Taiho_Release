/*
CCDM DQ Bad data mapping
Notes: Standard mapping to CCDM DQ Bad data table
*/

WITH bad_data AS (
                    SELECT  null::text AS customer_name,
                            null::text AS source_system,
                            null::text AS src_schema,
                            null::text AS src_table,
                            null::text AS studyid,
                            null::text AS src_col_name,
                            null::text AS src_col_val,
			    null::text AS src_rec,
                            null::text AS error_reason,
                            null::text AS severity,
                            null::text AS rule_type,
                            null::timestamp AS dq_start_timestamp,
			    null::bigint AS dq_run_id  )

SELECT 
	/*KEY (b.studyid)::text AS comprehendid, KEY*/
        b.customer_name::text AS customer_name,
        b.source_system::text AS source_system,
        b.src_schema::text AS src_schema,
        b.src_table::text AS src_table,
        b.studyid::text AS studyid,
        b.src_col_name::text AS src_col_name,
        b.src_col_val::text AS src_col_val,
        b.src_rec::text AS src_rec,
        b.error_reason::text AS error_reason,
        b.severity::text AS severity,
        b.rule_type::text AS rule_type,
        b.dq_start_timestamp::timestamp AS dq_start_timestamp,
	b.dq_run_id::bigint AS dq_run_id
	/*KEY , (b.customer_name ||'~'|| b.source_system ||'~'|| b.src_schema ||'~'|| b.src_table ||'~'|| b.studyid ||'~'|| COALESCE(b.src_col_name,'null') ||'~'|| b.severity ||'~'|| b.rule_type ||'~'|| b.dq_run_id ||'~'|| abs(hashtext(b.src_rec)) ||'~'|| random())::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM bad_data b
WHERE 1=2;