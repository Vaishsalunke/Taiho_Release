/*
CCDM PC mapping
Notes: Standard mapping to CCDM PC table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     pc_data AS (
	 select studyid,
	 siteid,
	 usubjid,
	 (row_number() over (partition by pc.studyid, pc.siteid, pc.usubjid order by pc.pcdtc))::int as pcseq,
	 pctestcd,
	 pctest,
	 pcorres,
	 pcorresu,
	 pcstresc,
	 pcstresn,
	 pcstresu,
	 pcnam,
	 pcspec,
	 pclloq,
	 visitnum,
	 pcdtc
	 from (
                SELECT  pc."project"::text AS studyid,
                        pc."SiteNumber"::text AS siteid,
                        pc."Subject"::text AS usubjid,
                        null::int AS pcseq,
                        null::text AS pctestcd,
                        null::text AS pctest,
                        null::text AS pcorres,
                        null::text AS pcorresu,
                        null::text AS pcstresc,
                        pc."DOSE_AMT"::numeric AS pcstresn,
                        pc."DOSE_AMT_Units"::text AS pcstresu,
                        null::text AS pcnam,
                        null::text AS pcspec,
                        null::int AS pclloq,
                        null::numeric AS visitnum,
                        pc."PK_DAT"::timestamp without time zone AS pcdtc
						from tas120_201."PK" pc
						)pc
						)

SELECT
        /*KEY (pc.studyid || '~' || pc.siteid || '~' || pc.usubjid)::text AS comprehendid, KEY*/
        pc.studyid::text AS studyid,
        pc.siteid::text AS siteid,
        pc.usubjid::text AS usubjid,
        pc.pcseq::int AS pcseq,
        pc.pctestcd::text AS pctestcd,
        pc.pctest::text AS pctest,
        pc.pcorres::text AS pcorres,
        pc.pcorresu::text AS pcorresu,
        pc.pcstresc::text AS pcstresc,
        pc.pcstresn::int AS pcstresn,
        pc.pcstresu::text AS pcstresu,
        pc.pcnam::text AS pcnam,
        pc.pcspec::text AS pcspec,
        pc.pclloq::int AS pclloq,
        pc.visitnum::numeric AS visitnum,
        pc.pcdtc::timestamp without time zone AS pcdtc
        /*KEY , (pc.studyid || '~' || pc.siteid || '~' || pc.usubjid || '~' || pcseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM pc_data pc
JOIN included_subjects s ON (pc.studyid = s.studyid AND pc.siteid = s.siteid AND pc.usubjid = s.usubjid);


