/*
CCDM RS Table mapping
Notes: Standard mapping to CCDM RS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
	ex_data as (
				select studyid,siteid,usubjid,min(exstdtc) ex_mindt
				from ex
				group by 1,2,3
			),
	ex_visit as (
				  select studyid,siteid,usubjid,visit,exstdtc ex_dt
				 from ex
				 where visit like '%Day 1 of Cycle 1' and exdose is not null
				 
				 ),				
				
    rs_data AS (
	            select *,
				case when rsdtc1::date-prev_visit_date::date is not null then concat((rsdtc1::date-prev_visit_date::date)::numeric,' Days')
				end::text AS rsevlint
    			 from(
        select distinct rs.comprehendid,
						replace(rs.studyid ,'TAS120_202','TAS-120-202') as studyid,
						rs.siteid,
						rs.usubjid,
						rsseq,
						rsgrpid,
						rsrefid,
						rsspid,
						rslnkid,
						rslnkgrp,
						rstestcd,
						rstest,
						rscat,
						rsscat,
						rsorres,
						rsorresu,
						rsstresc,
						rsstresn,
						rsstresu,
						rsstat,
						rsreasnd,
						rsnam,
						case when rsdtc::date <= ex_mindt then 'Y' else 'N' end ::text as rslobxfl,
						rsblfl,
						rsdrvfl,
						rseval,
						concat('Unknown ',row_number() over (partition by rs.studyid,rs.siteid,rs.usubjid order by rsdtc))::text as rsevalid,
						rsacptfl,
						rs.visitnum,
						rs.visit,
						visitdy,
						taetord,
						dm.arm::text as epoch,
						to_char(rsdtc::date,'DD-MM-YYYY') as rsdtc,
						(rsdtc::date - ex_dt::date)+1::numeric as rsdy,
						rstpt,
						rstptnum,
						rseltm,
						rstptref,
						rsrftdtc,
						--rsevlint,
						rsevintx,
						rsstrtpt,
						rssttpt,
						rsenrtpt,
						rsentpt
						,rsdtc::date as rsdtc1
						,lag(rsdtc::date)over(partition by rs.siteid,rs.usubjid order by rsdtc::date) as prev_visit_date

		from(
				SELECT  null::text AS comprehendid,
						project::text AS studyid,
						"SiteNumber"::text AS siteid,
						"Subject"::text AS usubjid,
						row_number()over(partition by project,siteid,"Subject" order by "ORDAT")::numeric AS rsseq,
						null::text AS rsgrpid,
						null::text AS rsrefid,
						null::text AS rsspid,
						null::text AS rslnkid,
						null::text AS rslnkgrp,
						rstestcd::text AS rstestcd,
						rstest::text AS rstest,
						'RECIST 1.1'::text AS rscat,
						null::text AS rsscat,
						rsorres::text AS rsorres,
						null::text AS rsorresu,
						rsstresc::text AS rsstresc,
						null::numeric AS rsstresn,
						null::text AS rsstresu,
						rsstat::text AS rsstat,
						null::text AS rsreasnd,
						null::text AS rsnam,
						null::text AS rslobxfl,
						null::text AS rsblfl,
						null::text AS rsdrvfl,
						'Unknown'::text AS rseval,
						null::text AS rsevalid,----------------to be handled in outer query
						null::text AS rsacptfl,
						row_number()over(partition by project,siteid,"Subject" order by "ORDAT")::numeric AS visitnum,
						"FolderName"::text AS visit,
						null::numeric AS visitdy,
						null::numeric AS taetord,
						null::text AS epoch,
						"ORDAT"::text AS rsdtc,
						null::numeric AS rsdy,
						null::text AS rstpt,
						0::numeric AS rstptnum,
						null::text AS rseltm,
						'Unknown'::text AS rstptref,
						null::text AS rsrftdtc,
						--null::text AS rsevlint,
						null::text AS rsevintx,
						null::text AS rsstrtpt,
						null::text AS rssttpt,
						null::text AS rsenrtpt,
						null::text AS rsentpt
				from tas120_202."OR1"

				CROSS JOIN LATERAL(VALUES
									("ORNLYN",'NEWLIND','New Lesion Indicator',case when lower("ORNLYN")='yes' then 'New Lesion' else '' end,
									"ORNLYN_STD",case when lower("ORNLYN")='yes' then 'Completed' else 'Not Completed' end),
									("ORTLRES",'TRGRESP','Target Response',"ORTLRES",
									"ORTLRES_STD",case when nullif("ORTLRES",'') is not null then 'Completed' else 'Not Completed' end),
									("ORNTLRES",'NTRGRESP','Non-Target Response',"ORNTLRES",
									"ORNTLRES_STD",case when lower("ORNTLYN")='yes' then 'Completed' else 'Not Completed' end),
									("ORRES",'OVRLRESP','Overall Response',"ORRES",
									"ORRES_STD",case when nullif("ORRES",'') is not null then 'Completed' else 'Not Completed' end)
					
									) t (rstestcd_1,rstestcd,rstest,rsorres,rsstresc,rsstat)
					
				UNION ALL 

				SELECT  null::text AS comprehendid,
						project::text AS studyid,
						"SiteNumber"::text AS siteid,
						"Subject"::text AS usubjid,
						"RecordId"::numeric AS rsseq,
						null::text AS rsgrpid,
						null::text AS rsrefid,
						null::text AS rsspid,
						null::text AS rslnkid,
						null::text AS rslnkgrp,
						rstestcd::text AS rstestcd,
						rstest::text AS rstest,
						'RECIST 1.1'::text AS rscat,
						null::text AS rsscat,
						rsorres::text AS rsorres,
						null::text AS rsorresu,
						rsstresc::text AS rsstresc,
						null::numeric AS rsstresn,
						null::text AS rsstresu,
						rsstat::text AS rsstat,
						null::text AS rsreasnd,
						null::text AS rsnam,
						null::text AS rslobxfl,
						null::text AS rsblfl,
						null::text AS rsdrvfl,
						'Unknown'::text AS rseval,
						null::text AS rsevalid,----------------to be handled in outer query
						null::text AS rsacptfl,
						"RecordId"::numeric AS visitnum,
						"FolderName"::text AS visit,
						null::numeric AS visitdy,
						null::numeric AS taetord,
						null::text AS epoch,
						null::text AS rsdtc,
						null::numeric AS rsdy,
						null::text AS rstpt,
						0::numeric AS rstptnum,
						null::text AS rseltm,
						'Unknown'::text AS rstptref,
						null::text AS rsrftdtc,
						--null::text AS rsevlint,
						null::text AS rsevintx,
						null::text AS rsstrtpt,
						null::text AS rssttpt,
						null::text AS rsenrtpt,
						null::text AS rsentpt
				from tas120_202."BOR"

				CROSS JOIN LATERAL(VALUES
									("BORTLRES",'BOVRLRESP','Best Overall Response',"BORTLRES",
									"BORTLRES_STD",case when nullif("BORTLRES",'') is not null then 'Completed' else 'Not Completed' end)
									
									) t (rstestcd_1,rstestcd,rstest,rsorres,rsstresc,rsstat)
			) rs
		left join 	ex_data ex
		on rs.studyid=ex.studyid and rs.siteid=ex.siteid and rs.usubjid=ex.usubjid
		left join dm
		on rs.studyid=dm.studyid and rs.siteid=dm.siteid and rs.usubjid=dm.usubjid
		left join ex_visit exv
		on rs.studyid=exv.studyid and rs.siteid=exv.siteid and rs.usubjid=exv.usubjid					
                )rs1)

SELECT
    /*KEY (rs.studyid || '~' || rs.siteid || '~' || rs.usubjid)::text AS comprehendid, KEY*/  
    rs.studyid::text AS studyid,
    rs.siteid::text AS siteid,
    rs.usubjid::text AS usubjid,
    rs.rsseq::numeric AS rsseq,
    rs.rsgrpid::text AS rsgrpid,
    rs.rsrefid::text AS rsrefid,
    rs.rsspid::text AS rsspid,
    rs.rslnkid::text AS rslnkid,
    rs.rslnkgrp::text AS rslnkgrp,
    rs.rstestcd::text AS rstestcd,
    rs.rstest::text AS rstest,
    rs.rscat::text AS rscat,
    rs.rsscat::text AS rsscat,
    rs.rsorres::text AS rsorres,
    rs.rsorresu::text AS rsorresu,
    rs.rsstresc::text AS rsstresc,
    rs.rsstresn::numeric AS rsstresn,
    rs.rsstresu::text AS rsstresu,
    rs.rsstat::text AS rsstat,
    rs.rsreasnd::text AS rsreasnd,
    rs.rsnam::text AS rsnam,
    rs.rslobxfl::text AS rslobxfl,
    rs.rsblfl::text AS rsblfl,
    rs.rsdrvfl::text AS rsdrvfl,
    rs.rseval::text AS rseval,
    rs.rsevalid::text AS rsevalid,
    rs.rsacptfl::text AS rsacptfl,
    rs.visitnum::numeric AS visitnum,
    rs.visit::text AS visit,
    rs.visitdy::numeric AS visitdy,
    rs.taetord::numeric AS taetord,
    rs.epoch::text AS epoch,
    rs.rsdtc::text AS rsdtc,
    rs.rsdy::numeric AS rsdy,
    rs.rstpt::text AS rstpt,
    rs.rstptnum::numeric AS rstptnum,
    rs.rseltm::text AS rseltm,
    rs.rstptref::text AS rstptref,
    rs.rsrftdtc::text AS rsrftdtc,
    rs.rsevlint::text AS rsevlint,
    rs.rsevintx::text AS rsevintx,
    rs.rsstrtpt::text AS rsstrtpt,
    rs.rssttpt::text AS rssttpt,
    rs.rsenrtpt::text AS rsenrtpt,
    rs.rsentpt::text AS rsentpt
    /*KEY , (rs.studyid || '~' || rs.siteid || '~' || rs.usubjid || '~' || rs.rstestcd || '~' || rs.rseval || '~' || rs.rsevalid || '~' || rs.visitnum || '~' || rs.rstptnum || '~' || rs.rstptref )::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM rs_data rs JOIN included_subjects s ON (rs.studyid = s.studyid AND rs.siteid = s.siteid AND rs.usubjid = s.usubjid)
;








