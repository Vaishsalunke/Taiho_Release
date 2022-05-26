/*
CCDM MH mapping
Notes: Standard mapping to CCDM MH table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     mh_data AS (
				SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid,
                        concat("RecordPosition","InstanceRepeatNumber","PageRepeatNumber")::int AS mhseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [mhstdtc,mhsttm]))::int AS mhseq,*/
                        "MHTERM"::text AS mhterm,
                        "MHTERM_PT"::text AS mhdecod,
                        "MHTERM_SOC"::text AS mhcat,
                        'Medical History'::text AS mhscat,
                        "MHTERM_SOC_CD"::text AS mhbodsys,
                        null::time without time zone AS mhsttm,
                        null::time without time zone AS mhendtm,
                        case when mhstdtc='' or mhstdtc like '%0000%' then null
							else to_date(mhstdtc,'DD Mon YYYY') 
						end ::date AS mhstdtc,
						case when mhendtc='' or mhendtc like '%0000%' then null
							else to_date(mhendtc,'DD Mon YYYY') 
						end ::date AS mhendtc
                        from
                ( select *,case when (length("MHSTDAT_RAW")<>11 and length("MHSTDAT_RAW")<>10) or  length(trim(substring(reverse("MHSTDAT_RAW"):: text, 1, 4 ))) <> 4 then null
else concat(replace(replace(substring(upper("MHSTDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("MHSTDAT_RAW"),3),'UNK','Jan'))
end as mhstdtc,
case when (length("MHENDAT_RAW")<>11 and length("MHENDAT_RAW")<>10) or  length(trim(substring(reverse("MHENDAT_RAW"):: text, 1, 4 ))) <> 4 then null
else concat(replace(replace(substring(upper("MHENDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("MHENDAT_RAW"),3),'UNK','Jan'))
end as mhendtc
from tas120_201."MH"
     )a
     )

SELECT
        /*KEY (mh.studyid || '~' || mh.siteid || '~' || mh.usubjid)::text AS comprehendid, KEY*/
        mh.studyid::text AS studyid,
        mh.siteid::text AS siteid,
        mh.usubjid::text AS usubjid,
        mh.mhseq::int AS mhseq,
        null::text AS mhspid,
        mh.mhterm::text AS mhterm,
        mh.mhdecod::text AS mhdecod,
        mh.mhcat::text AS mhcat,
        mh.mhscat::text AS mhscat,
        mh.mhbodsys::text AS mhbodsys,
        null::text AS mhstdtc_iso,
        mh.mhstdtc::date AS mhstdtc,
        mh.mhsttm::time without time zone AS mhsttm,
        null::text AS mhendtc_iso,
        mh.mhendtc::date AS mhendtc,
        mh.mhendtm::time without time zone AS mhendtm
        /*KEY , (mh.studyid || '~' || mh.siteid || '~' || mh.usubjid || '~' || mh.mhseq)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM mh_data mh
JOIN included_subjects s ON (mh.studyid = s.studyid AND mh.siteid = s.siteid AND mh.usubjid = s.usubjid);




