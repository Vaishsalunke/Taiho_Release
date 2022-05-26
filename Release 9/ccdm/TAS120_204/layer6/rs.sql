/*
CCDM RS Table mapping
Notes: Standard mapping to CCDM RS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
				
		ex_date as (
					select studyid,usubjid,siteid, min(exstdtc) ex_mindt 
					from cqs.ex
					group by 1,2,3
				),
		
		 ex_visit as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_mindt_visit
				 from 	cqs.ex
				 where visit like '%Cycle 01' and exdose is not null
				),	
   
	rs_data AS ( select *,case when rsdtc1::date-prev_visit_date::date is not null then concat((rsdtc1::date-prev_visit_date::date)::numeric,' Days') 					   
    					   end::text AS rsevlint
    			 from(	
	
				SELECT  DISTINCT
                project::text AS studyid,
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                row_number()over (partition by project,concat(project,'_',split_part("SiteNumber",'_',2)),"Subject" order by "ORDAT")::numeric AS rsseq,
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
                case when "ORDAT" <= a.ex_mindt then 'Y' else 'N' end::text AS rslobxfl,
                null::text AS rsblfl,
                null::text AS rsdrvfl,
                'Unknown'::text AS rseval,
                'Unknown'||ROW_NUMBER()OVER(PARTITION BY project,concat(project,'_',split_part("SiteNumber",'_',2)),"Subject" order by "ORDAT")::text AS rsevalid,
                null::text AS rsacptfl,
                row_number()over (partition by project,concat(project,'_',split_part("SiteNumber",'_',2)),"Subject" order by "ORDAT")::numeric AS visitnum,
                "FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                dm."arm"::text AS epoch,
                to_char("ORDAT"::date,'DD-MM-YYYY')  AS rsdtc,
                ("ORDAT"::date - b.ex_mindt_visit::date)+1::numeric  AS rsdy,
                null::text AS rstpt,
                0::numeric::numeric AS rstptnum,
                null::text AS rseltm,
                'Unknown'::text AS rstptref,
                null::text AS rsrftdtc,
                --null::text AS rsevlint,
                null::text AS rsevintx,
                null::text AS rsstrtpt,
                null::text AS rssttpt,
                null::text AS rsenrtpt,
                null::text AS rsentpt
                ,"ORDAT"::date AS rsdtc1
                ,lag("ORDAT"::date)over(partition by concat(project,'_',split_part("SiteNumber",'_',2)),"Subject" order by "ORDAT"::date) as prev_visit_date
			from tas120_204."OR"
						
						CROSS JOIN LATERAL(values 
												("ORNLYN",'NEWLIND' ,'New Lesion Indicator',case when "ORNLYN" = 'Yes' then 'New lesion' else '' end ,case when "ORNLYN" = 'Yes' then 'New lesion' else '' end,case when "ORNLYN" ='Yes' then 'Completed' else 'Not Completed' end),
												("ORTLRES" ,'TRGRESP' , 'Target Response',"ORTLRES","ORTLRES_STD",case when "ORTLYN" = 'Yes' then 'Completed' else 'Not Completed' end),
												("ORNTLRES",'NTRGRESP' , 'Non-Target Response',"ORNTLRES","ORNTLRES_STD",case when "ORNTLYN" = 'Yes' then 'Completed' else 'Not Completed' end),
												("ORRES",'OVRLRESP' ,'Overall Response',"ORRES","ORRES_STD",case when nullif("ORRES",'') is not null then 'Completed' else 'Not Completed' end  )
				)as t (cd1,rstestcd,rstest,rsorres,rsstresc,rsstat)
			
		left join cqs.dm
		on project = dm.studyid and concat(project,'_',split_part("SiteNumber",'_',2))::text = dm.siteid and "Subject" = dm.usubjid
		left join ex_date a
		on project = a.studyid and concat(project,'_',split_part("SiteNumber",'_',2))::text = a.siteid and "Subject" = a.usubjid
		left join ex_visit b 
		on project = b.studyid and concat(project,'_',split_part("SiteNumber",'_',2))::text = b.siteid and "Subject" = b.usubjid
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
    /*KEY, (rs.studyid || '~' || rs.siteid || '~' || rs.usubjid || '~' || rs.rstestcd || '~' || rs.rseval || '~' || rs.rsevalid || '~' || rs.visitnum || '~' || rs.rstptnum || '~' || rs.rstptref)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM rs_data rs JOIN included_subjects s ON (rs.studyid = s.studyid AND rs.siteid = s.siteid AND rs.usubjid = s.usubjid)
;

