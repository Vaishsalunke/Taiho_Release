WITH included_studies AS (
               SELECT studyid, studyphase FROM study),

     study_prtover AS (
                SELECT DISTINCT studyid, protver from subject),

     tv_data AS (
            SELECT DISTINCT studyid::text as studyid,
                   visitnum::int as visitnum,
                   visit::text as visitname,
                   visit::text as visitid,
                   visitdy::int as visitdy,
                   visitwindowbefore::int as visitwindowbefore,
                   visitwindowafter::int as visitwindowafter,
                   'C1D1'::text as visit_bl,
                   'Not Available'::text as visit_schedule_code,
                   'Not Available'::text as visit_schedule_desc,
                   'Not Available'::text as arm,
                   'Not Available'::text as armcd,
                   true::boolean as compliance_flag
            FROM tv
            WHERE NULLIF(visit::text,'') IS NOT NULL
                  )

SELECT 
        /*KEY tv.studyid::text AS comprehendid, KEY*/
        tv.studyid::text AS studyid,
        tv.visitnum::numeric AS visitnum,
        tv.visitname::text AS visitname,
        tv.visitid::text AS visitid,
        coalesce(tv.visitdy::int,99999)::int AS visitdy,
        coalesce(tv.visitwindowbefore::int,0)::int AS visitwindowbefore,
        coalesce(tv.visitwindowafter::int,0)::int AS visitwindowafter,
        tv.visit_bl::text as visit_bl,
        tv.visit_schedule_code::text as visit_schedule_code,
        tv.visit_schedule_desc::text as visit_schedule_desc,
        tv.armcd::text as armcd,
        tv.arm::text as arm,
        COALESCE(pr.protver,'Original') ::text as protver,
        COALESCE(st.studyphase,'Not Available') ::text as studyphase,
        tv.compliance_flag::boolean as compliance_flag
        /*KEY , (tv.studyid || '~' || tv.armcd || '~' || tv.visitid || '~' || tv.visitnum || '~' || tv.visit_schedule_code)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM tv_data tv
JOIN included_studies st ON (st.studyid = tv.studyid)
LEFT JOIN study_prtover pr ON (tv.studyid = pr.studyid)
where 1=2;
