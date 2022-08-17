/*
CCDM TR Table mapping
Notes: Standard mapping to CCDM TR table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
	 ex_visit as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_mindt_visit
				 from 	ex
				 where visit like '%Cycle 01' and exdose is not null
				),	
    tr_data AS (
        
		SELECT  DISTINCT
                'TAS2940-101'::text AS studyid,
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                row_number()over (partition by project,concat(project,'_',split_part("SiteNumber",'_',2)),"Subject" order by "ORDAT")::numeric AS trseq,
                null::text AS trgrpid,
                null::text AS trrefid,
                null::text AS trspid,
                null::text AS trlnkid,
                null::text AS trlnkgrp,
                trtestcd::text AS trtestcd,
                'Tumor/Lesion Response'::text AS trtest,
                trorres::text AS trorres,
                null::text AS trorresu,
                trstresc::text AS trstresc,
                null::numeric AS trstresn,
                null::text AS trstresu,
                trstat::text AS trstat,
                null::text AS trreasnd,
                null::text AS trnam,
                null::text AS trmethod,
                null::text AS trlobxfl,
                null::text AS trblfl,
                null::text AS treval,
                "RecordId" || row_number()over (partition by project,concat(project,'_',split_part("SiteNumber",'_',2)),"Subject" order by "ORDAT")::text AS trevalid,
                null::text AS tracptfl,
                row_number()over (partition by project,concat(project,'_',split_part("SiteNumber",'_',2)),"Subject" order by "ORDAT")::numeric AS visitnum,
                "FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                dm."arm"::text AS epoch,
                "ORDAT"::text AS trdtc,
                ("ORDAT"::date - a.ex_mindt_visit::date)+1::numeric AS trdy
				 from tas2940_101."OR" or1
                CROSS JOIN LATERAL(values 
												("ORNLYN"::text,'New Lesion Response'::text,
													case when "ORNLYN"='Yes' then 'New Lesion Present'::text
													 	  when "ORNLYN"='No' then 'New Lesion Absent'::text
													      else "ORNLYN"
													end,"ORNLYN_STD",case when "ORNLYN" ='Yes' then 'Completed' else 'Not Completed' end),
												("ORTLRES"::text,'Target Lesion Response'::text,"ORTLRES","ORTLRES_STD",case when "ORTLYN" ='Yes' then 'Completed' else 'Not Completed' end),
												("ORNTLRES"::text,'Non-Target Lesion Response'::text,"ORNTLRES","ORNTLRES_STD",case when "ORNTLYN" ='Yes' then 'Completed' else 'Not Completed' end),
												("ORRES"::text,'Overall RECIST Response'::text,"ORRES","ORRES_STD",case when "ORRES"!='' then 'Completed' else 'Not Completed'end)
												)as t (cd1,trtestcd,trorres,trstresc,trstat)
			left join dm								
			on 'TAS2940-101' = dm."studyid" and concat(project,'_',split_part("SiteNumber",'_',2))=dm.siteid and "Subject"= dm."usubjid"
			left join  ex_visit a
			on 'TAS2940-101' = a."studyid" and concat(project,'_',split_part("SiteNumber",'_',2)) = a.siteid and "Subject"= a."usubjid"
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




