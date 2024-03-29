/*
CCDM RS Table mapping
Notes: Standard mapping to CCDM RS table
*/


WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
                
    ex_data1 as (
				select studyid,siteid,usubjid,min(exstdtc) ex_mindt
				from ex
				group by 1,2,3
				),
                
	ex_data as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_dt
				 from ex
				 where visit like '%Cycle 01' and exdose is not null							 			 
				),
				
    rs_data AS (
	            select *,
				case when rsdtc1::date-prev_visit_date::date is not null then concat((rsdtc1::date-prev_visit_date::date)::numeric,' Days')
				end::text AS rsevlint
    			 from(
        select distinct replace(u.studyid,'TAS120_203','TAS-120-203') as studyid,
                        u.siteid,
						u.usubjid,						
						rsseq,
						null as rsgrpid,
						null as rsrefid,
						null as rsspid,
						null as rslnkid,
						rslnkgrp,
						rstestcd,
						rstest,
						rscat,
						null as rsscat,
						rsorres,
						null as rsorresu,
						rsstresc,
						null as rsstresn,
						null as rsstresu,
						rsstat,
						null as rsreasnd,
						null as rsnam,
						case when u.rsdtc::date <= ex1.ex_mindt then 'Y' else 'N' end::text AS rslobxfl,
						null as rsblfl,
						null as rsdrvfl,
						'Independent Assessor':: text as rseval,
						'Investigator'||ROW_NUMBER()OVER(PARTITION BY u.studyid,u.siteid,u.usubjid ORDER BY rsdtc):: text as rsevalid,
						null as rsacptfl,
						u.visitnum,
						u.visit,
						null as visitdy,
						null as taetord,
						dm.arm::text as epoch,
						to_char(rsdtc::date,'DD-MM-YYYY') as rsdtc,
						(u.rsdtc::date - ex.ex_dt::date)+1::numeric as rsdy,
						null as rstpt,
						rstptnum,
						null as rseltm,
						rstptref,
						null as rsrftdtc,
						--rsevlint,
						null as rsevintx,
						null as rsstrtpt,
						null as rssttpt,
						null as rsenrtpt,
						null as rsentpt
						,rsdtc::date as rsdtc1
						,lag(rsdtc::date)over(partition by u.siteid,u.usubjid order by rsdtc::date) as prev_visit_date
		        from
				(
					select distinct	project::text AS studyid,
									concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
									"Subject"::text AS usubjid,
									(dense_rank () over (partition by project, concat(project,'_',split_part("SiteNumber",'_',2)), "Subject" order by "ORDAT")::numeric +1)::text AS rslnkgrp,
									rstestcd::text AS rstestcd,
									rstest::text AS rstest,
									'RECIST 1.1'::text AS rscat,
									rsorres::text AS rsorres,
									rsstresc::text AS rsstresc,
									rsstat::text AS rsstat,									
									"FolderName"::text AS visit,									
									"ORDAT"::text AS rsdtc,									
									--null::text AS rsevlint,
									'Unknown'::text AS rseval,
									0::numeric AS rstptnum,
									'NA'::text AS rstptref,
									row_number()over(partition by project,or1.siteid,or1."Subject" order by "ORDAT")::numeric AS rsseq,
									--row_number()over(partition by project,siteid,"Subject" order by "ORDAT")::numeric as 
									coalesce(sv.visitnum,0) as visitnum
				 from tas120_203."OR" or1
				  join sv on or1."FolderName" = sv.visit and or1."ORDAT" ::date = sv.svstdtc 
					
					CROSS JOIN LATERAL(VALUES
					("ORRES",'OVRLRESP','Overall Response',"ORRES","ORRES_STD" , case when nullif("ORRES",'') is not null then null else 'Not Done' end ),
					("ORTLRES",'TRGRESP','Target Response',"ORTLRES","ORTLRES_STD",case when "ORTLYN" = 'Yes' then null else 'Not Done' end),
					("ORNTLRES", 'NTRGRESP','Non-Target Response',"ORNTLRES","ORNTLRES_STD", case when "ORNTLYN" = 'Yes' then null else 'Not Done' end)
					--("ORNLYN", 'NEWLIND','New Lesion Indicator',case when "ORNLYN"='Yes' then 'New lesion' else '' end, case when "ORNLYN"='Yes' then 'New lesion' else '' end, case when "ORNLYN" = 'Yes' then 'Completed' else 'Not Completed' end)
					
					) t (rstestcd_1,rstestcd,rstest,rsorres,rsstresc,rsstat)
										
				
					
					) u 
		
		left join dm 
		on u.studyid=dm.studyid and u.siteid=dm.siteid and u.usubjid=dm.usubjid
		left join ex_data ex 
		on u.studyid=ex.studyid and u.siteid=ex.siteid and u.usubjid=ex.usubjid
		left join ex_data1 ex1
		on u.studyid=ex1.studyid and u.siteid=ex1.siteid and u.usubjid=ex1.usubjid
                )rs)

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
    /*KEY, (rs.studyid || '~' || rs.siteid || '~' || rs.usubjid || '~' || rs.rstestcd || '~' || rs.rseval || '~' || rs.rsevalid || '~' || rs.visitnum || '~' || rs.rstptnum || '~' || rs.rstptref || '~' || rs.rslnkgrp )::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM rs_data rs JOIN included_subjects s ON (rs.studyid = s.studyid AND rs.siteid = s.siteid AND rs.usubjid = s.usubjid);



