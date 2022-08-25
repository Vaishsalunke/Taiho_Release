/*
CCDM TU Table mapping
Notes: Standard mapping to CCDM TU table
*/


WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
	
	ex_data as (
				select studyid,siteid,usubjid,min(exstdtc) ex_mindt
				from ex
				group by 1,2,3
				),
	sv_visit as (
				 select studyid,siteid,usubjid,visit,svstdtc
				 from sv
				 where visit like '%Day 1 Cycle 01' 
				 or visit like '%Day 01 Cycle 01'
				 or visit like 'Cycle 01'
				 ),		
    tu_data AS (
    
        SELECT  distinct replace(Study,'TAS2940_101','TAS2940-101') ::text AS studyid,
        		SiteNumber::text AS siteid,
        		Subject::text AS usubjid,
                ROW_NUMBER() OVER (PARTITION BY 'TAS2940-101', SiteNumber, Subject, tu.tuseq ORDER BY tu.tuseq)::numeric AS tuseq,
                tugrpid::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                tulnkid::text AS tulnkid,
                tulnkgrp::text AS tulnkgrp,
                tutestcd::text AS tutestcd,
                tutest::text AS tutest,
                tuorres::text AS tuorres,
                tustresc::text AS tustresc,
                null::text AS tunam,
                tuloc::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                tumethod::text AS tumethod,
                case when tudtc <= e1.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                tublfl::text AS tublfl,
                'Independent Assessor'::text AS tueval,
                concat('Investigator',ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc))::text as tuevalid,
                null::text AS tuacptfl,
                --ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc)::numeric AS 
                coalesce (sv.visitnum,0) as visitnum,
                tu.visit::text AS visit,
                Null::numeric AS visitdy,
                null::numeric AS taetord,
                epoch::text AS epoch,
                tudtc::text AS tudtc
               ,(tu.tudtc::date-svv.svstdtc::date)::numeric AS tudy
		FROM	(
					Select 		"project":: text as Study, 
								concat('TAS2940_101_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								'NL-' || "NLNUM"::text as tuseq,
								null::text as tugrpid,
								'NL'||"NLNUM":: text as tulnkid,
								(row_number () over (partition by "project", concat('TAS2940_101_',split_part("SiteNumber",'_',2)), "Subject" order by "NLDAT")::numeric + 1)::text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'NEW':: text as tuorres,
								'NEW':: text as tustresc,
								"NLSITE":: text as tuloc,
								case when "NLMETH"='Other' then "NLOTH" else "NLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								null:: text as tueval,
								null:: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NLDAT":: date as tudtc,
								null::text as  tudy
					From 		tas2940_101."NL" nl
					left join 	dm
					on 			nl."project"=dm.studyid and concat('TAS2940_101_',split_part("SiteNumber",'_',2))=dm.siteid and nl."Subject"=dm.usubjid
										
					union all
					
					Select 		"project":: text as Study, 
								concat('TAS2940_101_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								'NTL-'||"NTLNUM" ::text as tuseq,
								null::text as tugrpid,
								'NTL'||"NTLNUM":: text as tulnkid,
								'1'::text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'NON-TARGET':: text as tuorres,
								'NON-TARGET':: text as tustresc,
								"NTLBSITE":: text as tuloc,
								case when "NTLBMETH"='Other' then "NTLBOTH" else "NTLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								null:: text as tueval,
								null:: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NTLBDAT":: date as tudtc,
								null::text as  tudy
					From 		tas2940_101."NTLB" ntlb
					left join 	dm
					on 			ntlb."project"=dm.studyid and concat('TAS2940_101_',split_part("SiteNumber",'_',2))=dm.siteid and ntlb."Subject"=dm.usubjid
										
					union all
					
					Select 		"project":: text as Study, 
								concat('TAS2940_101_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								'NTL-'||"NTLNUM" ::text as tuseq,
								null::text as tugrpid,
								'NTL'||"NTLNUM":: text as tulnkid,
								(row_number () over (partition by "project", concat('TAS2940_101_',split_part("SiteNumber",'_',2)), "Subject" order by "NTLDAT")::numeric + 1)::text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'NON-TARGET':: text as tuorres,
								'NON-TARGET':: text as tustresc,
								"NTLSITE":: text as tuloc,
								case when "NTLMETH"='Other' then "NTLOTH" else "NTLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								null:: text as tueval,
								null:: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NTLDAT":: date as tudtc,
								null::text as  tudy
					From 		tas2940_101."NTL" ntl
					left join 	dm
					on 			ntl."project"=dm.studyid and concat('TAS2940_101_',split_part("SiteNumber",'_',2))=dm.siteid and ntl."Subject"=dm.usubjid
									
					union all
					
					Select 		"project":: text as Study, 
								concat('TAS2940_101_',split_part("SiteNumber",'_',2))  :: text as SiteNumber,
								"Subject" :: text as Subject,
								'TL-'||"TLNUM" ::text as tuseq,
								null::text as tugrpid,
								'TL'||"TLNUM":: text as tulnkid,
								'1'::text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'TARGET':: text as tuorres,
								'TARGET':: text as tustresc,
								"TLBSITE":: text as tuloc,
								case when "TLBMETH"='Other' then "TLBOTH" else "TLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								null:: text as tueval,
								null:: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLBDAT":: date as tudtc,
								null::text as  tudy
					From 		tas2940_101."TLB" tlb
					left join 	dm
					on 			tlb."project"=dm.studyid and concat('TAS2940_101_',split_part("SiteNumber",'_',2))=dm.siteid and tlb."Subject"=dm.usubjid
									
					union all
					
					Select 		"project":: text as Study, 
								concat('TAS2940_101_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								'TL-'||"TLNUM" ::text as tuseq,
								null::text as tugrpid,
								'TL'||"TLNUM":: text as tulnkid,
								(row_number () over (partition by "project", concat('TAS2940_101_',split_part("SiteNumber",'_',2)), "Subject" order by "TLDAT")::numeric + 1)::text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'TARGET':: text as tuorres,
								'TARGET':: text as tustresc,
								"TLSITE":: text as tuloc,
								case when "TLMETH"='Other' then "TLOTH" else "TLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								null:: text as tueval,
								null:: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLDAT":: date as tudtc,
								null::text as  tudy
					From 		tas2940_101."TL" tl
					left join 	dm
					on 			tl."project"=dm.studyid and concat('TAS2940_101_',split_part("SiteNumber",'_',2))=dm.siteid and tl."Subject"=dm.usubjid
					
		)tu	
		left join 	ex_data e1
		on			'TAS2940_101'=e1.studyid and SiteNumber=e1.siteid and tu.Subject= e1.usubjid
		left join sv_visit svv
on 'TAS2940_101'=svv.studyid and SiteNumber=svv.siteid and tu.Subject=svv.usubjid
		left join sv
on tu.visit = sv.visit and tu.tudtc :: date = sv.svstdtc 
		
		)

SELECT
    /*KEY (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid)::text AS comprehendid, KEY*/  
    tu.studyid::text AS studyid,
    tu.siteid::text AS siteid,
    tu.usubjid::text AS usubjid,
    tu.tuseq::numeric AS tuseq,
    tu.tugrpid::text AS tugrpid,
    tu.turefid::text AS turefid,
    tu.tuspid::text AS tuspid,
    tu.tulnkid::text AS tulnkid,
    tu.tulnkgrp::text AS tulnkgrp,
    tu.tutestcd::text AS tutestcd,
    tu.tutest::text AS tutest,
    tu.tuorres::text AS tuorres,
    tu.tustresc::text AS tustresc,
    tu.tunam::text AS tunam,
    tu.tuloc::text AS tuloc,
    tu.tulat::text AS tulat,
    tu.tudir::text AS tudir,
    tu.tuportot::text AS tuportot,
    tu.tumethod::text AS tumethod,
    tu.tulobxfl::text AS tulobxfl,
    tu.tublfl::text AS tublfl,
    tu.tueval::text AS tueval,
    tu.tuevalid::text AS tuevalid,
    tu.tuacptfl::text AS tuacptfl,
    tu.visitnum::numeric AS visitnum,
    tu.visit::text AS visit,
    tu.visitdy::numeric AS visitdy,
    tu.taetord::numeric AS taetord,
    tu.epoch::text AS epoch,
    tu.tudtc::text AS tudtc
    ,tu.tudy::numeric AS tudy
    /*KEY , (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid)::text  AS objectuniquekey KEY*/ 
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
;


