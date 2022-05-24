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
					(row_number() over (partition by pc4.studyid, pc4.siteid, pc4.usubjid order by pc4.pcdtc))::int as pcseq,
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
	                --PKBLD
                        SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
                        pc."SiteNumber"::text AS siteid,
                        pc."Subject"::text AS usubjid,
                        null::int AS pcseq,
                        pc."PKBTMPT"::text AS pctestcd,
                        pc."PKBTMPT"::text AS pctest,
                        null::text AS pcorres,
                        null::text AS pcorresu,
                        null::text AS pcstresc,
                        null::numeric AS pcstresn,
                        null::text AS pcstresu,
                        pc."FolderName"::text AS pcnam,
                        null::text AS pcspec,
                        null::int AS pclloq,
                        99::numeric AS visitnum,
                        pc."PKBDAT"::timestamp without time zone AS pcdtc,						
						null::timestamp with time zone as comprehend_update_time
						from tas3681_101."PKBLD" pc
						
						UNION ALL
						
					--PKBLD2
						SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
                        pc2."SiteNumber"::text AS siteid,
                        pc2."Subject"::text AS usubjid,
                        null::int AS pcseq,
                        pc2."PKBTMPT"::text AS pctestcd,
                        pc2."PKBTMPT"::text AS pctest,
                        null::text AS pcorres,
                        null::text AS pcorresu,
                        null::text AS pcstresc,
                        null::numeric AS pcstresn,
                        null::text AS pcstresu,
                        pc2."FolderName"::text AS pcnam,
                        null::text AS pcspec,
                        null::int AS pclloq,
                        99::numeric AS visitnum,
                        pc2."PKBDAT"::timestamp without time zone AS pcdtc,						
						null::timestamp with time zone as comprehend_update_time
						from tas3681_101."PKBLD2" pc2
						
						UNION ALL
						
					--PKURIN
						SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
                        pc3."SiteNumber"::text AS siteid,
                        pc3."Subject"::text AS usubjid,
                        null::int AS pcseq,
                        pc3."PKUTMPT"::text AS pctestcd,
                        pc3."PKUTMPT"::text AS pctest,
                        null::text AS pcorres,
                        null::text AS pcorresu,
                        null::text AS pcstresc,
                        pc3."PKUVOL"::numeric AS pcstresn,
                        pc3."PKUVOL_Units"::text AS pcstresu,
                        pc3."FolderName"::text AS pcnam,
                        null::text AS pcspec,
                        null::int AS pclloq,
                        99::numeric AS visitnum,
                        pc3."PKUDAT"::timestamp without time zone AS pcdtc,						
						null::timestamp with time zone as comprehend_update_time
						from tas3681_101."PKURIN" pc3	
						
						)pc4 )

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

