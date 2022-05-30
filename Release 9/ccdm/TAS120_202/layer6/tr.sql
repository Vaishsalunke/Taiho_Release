/*
CCDM TR Table mapping
Notes: Standard mapping to CCDM TR table
*/


WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
		


	ex_data as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_dt
				 from ex
				 where visit like '%Day 1 of Cycle 1' and exdose is not null
				 			 
				),
				
    tr_data AS (
        select distinct u.comprehendid,
						u.studyid,
						u.siteid,
						u.usubjid,
						row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc) as trseq,
						trgrpid,
						trrefid,
						trspid,
						trlnkid,
						trlnkgrp,
						trtestcd,
						trtest,
						trorres,
						trorresu,
						trstresc,
						trstresn,
						trstresu,
						trstat,
						trreasnd,
						trnam,
						trmethod::text as trmethod,
						trlobxfl,
						trblfl,
						treval,
						concat(trevalid,row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc)) as trevalid,
						tracptfl,
						row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc) as visitnum,
						u.visit,
						u.visitdy,
						u.taetord,
						dm.arm::text as epoch,
						trdtc::date,
						(u.trdtc::date - ex.ex_dt::date)+1::numeric as trdy

		from
				(
					SELECT  			
									null::text AS comprehendid,
									project::text AS studyid,
									"SiteNumber"::text AS siteid,
									"Subject"::text AS usubjid,
									null::numeric AS trseq,
									null::text AS trgrpid,
									null::text AS trrefid,
									null::text AS trspid,
									null::text AS trlnkid,
									null::text AS trlnkgrp,
									trtestcd::text AS trtestcd,---------------1
									'Tumor/Lesion Response'::text AS trtest,
									trorres::text AS trorres,
									null::text AS trorresu,
									trstresc::text AS trstresc,---------------2
									null::numeric AS trstresn,
									null::text AS trstresu,
									trstat::text AS trstat,--------------------3
									null::text AS trreasnd,
									null::text AS trnam,
									null::text AS trmethod,--------------------------to be mapped in outer query
									null::text AS trlobxfl,
									null::text AS trblfl,
									null::text AS treval,
									"RecordId"::text AS trevalid,
									null::text AS tracptfl,
									null::numeric AS visitnum,-----------------------to be mapped in outer query
									"FolderName"::text AS visit,
									null::numeric AS visitdy,
									null::numeric AS taetord,
									null::text AS epoch,------------------------------to be mapped in outer query
									"ORDAT"::text AS trdtc,
									null::numeric AS trdy----------------------------to be mapped in outer query
									,trtestcd_1
					
					from tas120_202."OR1"
					
					CROSS JOIN LATERAL(VALUES
					("ORNLYN",'New Lesion Response',"ORNLYN_STD",case when lower("ORNLYN")='yes' then 'New Lesion Present' else 'New Lesion Absent' end,
					case when lower("ORNLYN")='yes' then 'Completed' else 'Not Completed' end),
					("ORTLRES",'Target Lesion Response',"ORTLRES_STD","ORTLRES",
					case when nullif("ORTLRES",'') is not null then 'Completed' else 'Not Completed' end),
					("ORNTLRES",'Non-Target Lesion Response',"ORNTLRES_STD","ORNTLRES",
					case when lower("ORNTLYN")='yes' then 'Completed' else 'Not Completed' end),
					("ORRES",'Overall RECIST Response',"ORRES_STD","ORRES",
					case when nullif("ORRES",'') is not null then 'Completed' else 'Not Completed' end)
					
					) t (trtestcd_1,trtestcd,trstresc,trorres,trstat)
					) u 
		
		left join dm 
		on u.studyid=dm.studyid and u.siteid=dm.siteid and u.usubjid=dm.usubjid
		left join ex_data ex 
		on u.studyid=ex.studyid and u.siteid=ex.siteid and u.usubjid=ex.usubjid
                )

SELECT
    /*KEY (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid)::text AS comprehendid, KEY*/  
    tr.studyid::text AS studyid,
    tr.siteid::text AS siteid,
    tr.usubjid::text AS usubjid,
    tr.trseq::numeric AS trseq,
    tr.trgrpid::text AS trgrpid,
    tr.trrefid::text AS trrefid,
    tr.trspid::text AS trspid,
    tr.trlnkid::text AS trlnkid,
    tr.trlnkgrp::text AS trlnkgrp,
    tr.trtestcd::text AS trtestcd,
    tr.trtest::text AS trtest,
    tr.trorres::text AS trorres,
    tr.trorresu::text AS trorresu,
    tr.trstresc::text AS trstresc,
    tr.trstresn::numeric AS trstresn,
    tr.trstresu::text AS trstresu,
    tr.trstat::text AS trstat,
    tr.trreasnd::text AS trreasnd,
    tr.trnam::text AS trnam,
    tr.trmethod::text AS trmethod,
    tr.trlobxfl::text AS trlobxfl,
    tr.trblfl::text AS trblfl,
    tr.treval::text AS treval,
    tr.trevalid::text AS trevalid,
    tr.tracptfl::text AS tracptfl,
    tr.visitnum::numeric AS visitnum,
    tr.visit::text AS visit,
    tr.visitdy::numeric AS visitdy,
    tr.taetord::numeric AS taetord,
    tr.epoch::text AS epoch,
    tr.trdtc::text AS trdtc,
    tr.trdy::numeric AS trdy
    /*KEY , (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid || '~' || tr.trtestcd || '~' || tr.trevalid || '~' || tr.visitnum)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tr_data tr JOIN included_subjects s ON (tr.studyid = s.studyid AND tr.siteid = s.siteid AND tr.usubjid = s.usubjid)
;


