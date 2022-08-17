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
				
	ex_visit as (
				 select 	studyid,siteid,usubjid,visit,exstdtc
				 from 		ex
				 where 		visit in ('Cycle 1 Day 1','Day 01 Cycle 01')
				 and 		exdose is not null	
				),
				
    tu_data AS (
        SELECT  distinct replace(Study ,'TAS120_203','TAS-120-203')::text AS studyid,
                SiteNumber::text AS siteid,
                Subject::text AS usubjid,
                ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc)::numeric as tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                tulnkid::text as tulnkid,
                null::text AS tulnkgrp,
                tutestcd::text AS tutestcd,
                tutest::text AS tutest,
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
                null::text AS tueval,
                --tuevalid::text AS tuevalid,
                concat(tuevalid,ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc)) as tuevalid,
                null::text AS tuacptfl,
                ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc)::numeric as visitnum,
                tu.visit::text AS visit,
                Null::numeric AS visitdy,
                null::numeric AS taetord,
                epoch::text AS epoch,
                tudtc::text AS tudtc,
                (extract(day from tudtc::timestamp  - e2.exstdtc::timestamp)::numeric) +1::numeric AS tudy
		FROM	(
					Select 		distinct 'TAS120_203':: text as Study, 
								concat('TAS120_203_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								coalesce(nullif("NLID",''),"RecordId"::text) :: text as tulnkid,
								case when nullif("NLSITE",'') is not null then concat("NLID",'-',"NLSITE")
				                else 'NA' end::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'Present':: text as tuorres,
								case when "NLSITE" = 'Other' then "NLSITEOT" else "NLSITE" end:: text as tuloc,
								case when "NLMETH"='Other' then "NLOTH" else "NLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'Radiologist':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NLDAT":: date as tudtc,
								null::text as  tudy
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
                                 coalesce(nullif("NTLBID",''),"RecordId"::text):: text as tulnkid,
								 case when nullif("NTLBSITE",'') is not null then concat("NTLBID",'-',"NTLBSITE")
				                else 'NA' end::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								case when "NTLBYN" = 'Yes' then 'Present'
								     when "NTLBYN" = 'No' then 'Absent' end:: text as tuorres,
								case when "NTLBSITE" = 'Other' then "NTLBSTOT" else "NTLBSITE" end:: text as tuloc,
								case when "NTLBMETH"='Other' then "NTLBOTH" else "NTLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								'Radiologist':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NTLBDAT":: date as tudtc,
								null::text as  tudy
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
								coalesce(nullif("NTLID",''),"RecordId"::text):: text as tulnkid,
								case when nullif("NTLSITE",'') is not null then concat("NTLID",'-',"NTLSITE")
				                else 'NA' end::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								"NTLBSTAT":: text as tuorres,
								case when "NTLSITE" = 'Other' then "NTLSTOT" else "NTLSITE" end:: text as tuloc,
								case when "NTLMETH"='Other' then "NTLOTH" else "NTLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'Radiologist':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
							    "NTLDAT":: date as tudtc,
								null::text as  tudy
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
								coalesce(nullif("TLBID",''),"RecordId"::text):: text as tulnkid,
								case when nullif("TLBSITE",'') is not null then concat("TLBID",'-',"TLBSITE")
				                else 'NA' end::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								"TLBDIM":: text as tuorres,
								case when "TLBSITE" = 'Other' then "TLBSTOT" else "TLBSITE" end:: text as tuloc,
								case when "TLBMETH"='Other' then "TLBOTH" else "TLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								'Radiologist':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLBDAT":: date as tudtc,
								null::text as  tudy
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
								coalesce(nullif("TLID",''),"RecordId"::text):: text as tulnkid,
								case when nullif("TLSITE",'') is not null then concat("TLID",'-',"TLSITE")
				                else 'NA' end::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								"TLDIM":: text as tuorres,
								case when "TLSITE" = 'Other' then "TLSITEOT" else "TLSITE" end:: text as tuloc,
								case when "TLMETH"='Other' then "TLOTH" else "TLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'Radiologist':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLDAT":: date as tudtc,
								null::text as  tudy
					From 		TAS120_203."TL" tl
					left join 	dm
					on 			dm.studyid = 'TAS120_203'
					            and concat('TAS120_203_',split_part("SiteNumber",'_',2)) = dm.siteid
					            and tl."Subject"=dm.usubjid
		)tu	
		left join 	ex_data e1
		on 'TAS120_203'=e1.studyid and SiteNumber=e1.siteid and tu.Subject= e1.usubjid
		left join	ex_visit e2
		on 'TAS120_203'=e2.studyid and SiteNumber=e2.siteid and tu.Subject= e2.usubjid
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
    tu.tudtc::text AS tudtc,
    tu.tudy::numeric AS tudy
    /*KEY, (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid);



