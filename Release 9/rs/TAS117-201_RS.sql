/*
CCDM RS Table mapping
Notes: Standard mapping to CCDM RS table
*/


WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
                
    ex_data1 as (
				select studyid,siteid,usubjid,min(exstdtc) ex_mindt
				from cqs.ex
				group by 1,2,3
				),
                
	ex_data as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_dt
				 from cqs.ex
				 where visit like '%Cycle 01' and exdose is not null							 			 
				),
				
    rs_data AS (
        select distinct u.studyid,
                        u.siteid,
						u.usubjid,						
						row_number() over(partition by u.studyid, u.siteid,u.usubjid order by rsdtc) as rsseq,
						null as rsgrpid,
						null as rsrefid,
						null as rsspid,
						null as rslnkid,
						null as rslnkgrp,
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
						rseval,
						'Unknown'||ROW_NUMBER()OVER(PARTITION BY u.studyid,u.siteid,u.usubjid ORDER BY rsdtc) as rsevalid,
						null as rsacptfl,
						u.visitnum,
						u.visit,
						null as visitdy,
						null as taetord,
						dm.arm::text as epoch,
						rsdtc,
						(u.rsdtc::date - ex.ex_dt::date)+1::numeric as rsdy,
						null as rstpt,
						rstptnum,
						null as rseltm,
						rstptref,
						null as rsrftdtc,
						rsevlint,
						null as rsevintx,
						null as rsstrtpt,
						null as rssttpt,
						null as rsenrtpt,
						null as rsentpt
		        from
				(
					select distinct	project::text AS studyid,
									concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
									or1."Subject"::text AS usubjid,
									rstestcd::text AS rstestcd,
									rstest::text AS rstest,
									'RECIST 1.1'::text AS rscat,
									'Unknown'::text AS rseval,
									rsorres::text AS rsorres,
									rsstresc::text AS rsstresc,
									rsstat::text AS rsstat,									
									concat("instanceId", "InstanceRepeatNumber", "DataPageId")::numeric AS visitnum,
									"FolderName"::text AS visit,									
									Or1."ORDAT"::text AS rsdtc,									
									cast(or2."ORDAT"::date - or1."ORDAT"::date as int)::text AS rsevlint,
									0::numeric AS rstptnum,
									'Unknown'::text AS rstptref
				 from tas117_201."OR" or1
				 left join 
				 
				(select "Subject","ORDAT",lead("ORDAT",1)over(partition by "Subject" order by "ORDAT") as ORDAT from tas117_201."OR")or2  
			    ON or1."Subject"=or2."Subject" AND or1."ORDAT"=or2."ORDAT"
					
					CROSS JOIN LATERAL(VALUES
					("ORRES",'OVRLRESP','Overall Response', case when "ORNLYN"='Yes' then 'New lesion' else '' end, case when "ORNLYN"='Yes' then 'New lesion' else '' end, case when nullif("ORRES",'') is not null then 'Completed' else 'Not Completed' end ),
					("ORTLRES",'TRGRESP','Target Response',"ORTLRES","ORTLRES_STD",case when nullif("ORTLRES",'') is not null then 'Completed' else 'Not Completed' end),
					("ORNTLRES", 'NTRGRESP','Non-Target Response',"ORNTLRES","ORNTLRES_STD", case when "ORNTLYN" = 'Yes' then 'Completed' else 'Not Completed' end),
					("ORNLYN", 'NEWLIND','New Lesion Indicator',"ORRES","ORRES_STD",case when "ORNLYN" = 'Yes' then 'Completed' else 'Not Completed' end)
					
					) t (rstestcd_1,rstestcd,rstest,rsorres,rsstresc,rsstat)
										
					union 
					
					SELECT distinct project::text AS studyid,
									concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
									"Subject"::text AS usubjid,
									rstestcd::text AS rstestcd,
									rstest::text AS rstest,
									'RECIST 1.1'::text AS rscat,
									'Unknown'::text as rseval,
									rsorres::text AS rsorres,
									rsstresc::text AS rsstresc,
									rsstat::text AS rsstat,									
									concat("instanceId", "InstanceRepeatNumber", "DataPageId")::numeric AS visitnum,
									"FolderName"::text AS visit,									
									null::text AS rsdtc,									
									null::text AS rsevlint,
									0::numeric AS rstptnum,
									'Unknown'::text AS rstptref
									
				 from tas117_201."BOR"
					
					CROSS JOIN LATERAL(VALUES
					("BORESPO",'BOVRLRESP','Best Overall Response', "BORESPO", "BORESPO_STD", case when nullif("BORESPO",'') is not null then 'Completed' else 'Not Completed' end )					
					) t (rstestcd_1,rstestcd,rstest,rsorres,rsstresc,rsstat)
					
					) u 
		
		left join cqs.dm 
		on u.studyid=dm.studyid and u.siteid=dm.siteid and u.usubjid=dm.usubjid
		left join ex_data ex 
		on u.studyid=ex.studyid and u.siteid=ex.siteid and u.usubjid=ex.usubjid
		left join ex_data1 ex1
		on u.studyid=ex1.studyid and u.siteid=ex1.siteid and u.usubjid=ex1.usubjid
                )

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
FROM rs_data rs JOIN included_subjects s ON (rs.studyid = s.studyid AND rs.siteid = s.siteid AND rs.usubjid = s.usubjid);






		