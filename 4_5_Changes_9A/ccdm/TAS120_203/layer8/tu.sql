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
        SELECT  distinct Study::text AS studyid,
                SiteNumber::text AS siteid,
                Subject::text AS usubjid,
                ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc)::numeric as tuseq,
                tugrpid::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                tulnkid::text as tulnkid,
                tulnkgrp::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                tuorres::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                tuloc::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                tumethod::text AS tumethod,
                case when tudtc <= e1.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                tublfl::text AS tublfl,
                tueval::text AS tueval,
                concat(tu.tuevalid,ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc))::text as tuevalid,
                null::text AS tuacptfl,
                --ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc)::numeric as 
                sv.visitnum,
                tu.visit::text AS visit,
                Null::numeric AS visitdy,
                null::numeric AS taetord,
                epoch::text AS epoch,
                tudtc::text as tudtc,
                (tu.tudtc::date-svv.svstdtc::date)::numeric AS tudy
		FROM	(
					Select 		distinct 'TAS120_203':: text as Study, 
								concat('TAS120_203_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								'New Lesion'::text as tugrpid,								
								coalesce(nullif("NLID",''),"RecordId"::text) :: text as tulnkid,
								"RecordPosition" :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'New Lesion':: text as tuorres,
								'New Lesion'::text as tustresc,
								case when "NLSITE" = 'Other' then "NLSITEOT" else "NLSITE" end:: text as tuloc,
								case when "NLMETH"='Other' then "NLOTH" else "NLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'INDEPENDENT ASSESSOR'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NLDAT":: date as tudtc
								--null::text as  tudy
					From 		TAS120_203."NL" nl
					left join 	dm
					on 			dm.studyid = 'TAS120_203'
					            and concat('TAS120_203_',split_part("SiteNumber",'_',2)) = dm.siteid
					            and nl."Subject"=dm.usubjid					           
										
					union all
					
					Select 		distinct 'TAS120_203':: text as Study, 
								concat('TAS120_203_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
                                 null::numeric as tuseq,		
                                 'Non-Target Lesion'::text as tugrpid,
                                 coalesce(nullif("NTLBID",''),"RecordId"::text):: text as tulnkid,
                                 "RecordPosition" :: text as tulnkgrp,
								 null::text AS tutestcd,
								Null:: text as tutest,
								'Non-Target Lesion':: text as tuorres,
								'Non-Target Lesion'::text as tustresc,
								case when "NTLBSITE" = 'Other' then "NTLBSTOT" else "NTLBSITE" end:: text as tuloc,
								case when "NTLBMETH"='Other' then "NTLBOTH" else "NTLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								'INDEPENDENT ASSESSOR'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NTLBDAT":: date as tudtc
								--null::text as  tudy
					From 		TAS120_203."NTLB" ntlb
					left join 	dm
					on 			dm.studyid = 'TAS120_203'
					            and concat('TAS120_203_',split_part("SiteNumber",'_',2)) = dm.siteid
					            and ntlb."Subject"=dm.usubjid		
					
					union all
					
					Select 	distinct 'TAS120_203':: text as Study, 
								concat('TAS120_203_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								'Non-Target Lesion'::text as tugrpid,
								coalesce(nullif("NTLID",''),"RecordId"::text):: text as tulnkid,
								"RecordPosition" :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'Non-Target Lesion':: text as tuorres,
								'Non-Target Lesion'::text as tustresc,
								case when "NTLSITE" = 'Other' then "NTLSTOT" else "NTLSITE" end:: text as tuloc,
								case when "NTLMETH"='Other' then "NTLOTH" else "NTLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'INDEPENDENT ASSESSOR'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
							    "NTLDAT":: date as tudtc
								--null::text as  tudy
					From 		TAS120_203."NTL" ntl
					left join 	dm
					on 			dm.studyid = 'TAS120_203'
					            and concat('TAS120_203_',split_part("SiteNumber",'_',2)) = dm.siteid
					            and ntl."Subject"=dm.usubjid		
				
					union all
					
					Select 	distinct	'TAS120_203':: text as Study, 
								concat('TAS120_203_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								'Target Lesion'::text as tugrpid,
								coalesce(nullif("TLBID",''),"RecordId"::text):: text as tulnkid,
								"RecordPosition" :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'Target Lesion':: text as tuorres,
								'Target Lesion'::text as tustresc,
								case when "TLBSITE" = 'Other' then "TLBSTOT" else "TLBSITE" end:: text as tuloc,
								case when "TLBMETH"='Other' then "TLBOTH" else "TLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								'INDEPENDENT ASSESSOR'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLBDAT":: date as tudtc
								--null::text as  tudy
					From 		TAS120_203."TLB" tlb
					left join 	dm
					on 			dm.studyid = 'TAS120_203'
					            and concat('TAS120_203_',split_part("SiteNumber",'_',2)) = dm.siteid
					            and tlb."Subject"=dm.usubjid
				
					union all
					
					Select 	distinct 'TAS120_203':: text as Study, 
								concat('TAS120_203_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								'Target Lesion'::text as tugrpid,
								coalesce(nullif("TLID",''),"RecordId"::text):: text as tulnkid,
								"RecordPosition" :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'Target Lesion':: text as tuorres,
								'Target Lesion'::text as tustresc,
								case when "TLSITE" = 'Other' then "TLSITEOT" else "TLSITE" end:: text as tuloc,
								case when "TLMETH"='Other' then "TLOTH" else "TLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'INDEPENDENT ASSESSOR'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLDAT":: date as tudtc
								--null::text as  tudy
					From 		TAS120_203."TL" tl
					left join 	dm
					on 			dm.studyid = 'TAS120_203'
					            and concat('TAS120_203_',split_part("SiteNumber",'_',2)) = dm.siteid
					            and tl."Subject"=dm.usubjid
		)tu	
		left join 	ex_data e1
		on 'TAS120_203'=e1.studyid and SiteNumber=e1.siteid and tu.Subject= e1.usubjid
		
		left join sv_visit svv
on 'TAS120_203'=svv.studyid and SiteNumber=svv.siteid and tu.Subject=svv.usubjid
		left join sv on 'TAS120_203' = sv.studyid and sv.siteid = SiteNumber and sv.usubjid = tu.Subject
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
    tu.tudtc::text as tudtc,
    tu.tudy::numeric AS tudy
    /*KEY, (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid);



