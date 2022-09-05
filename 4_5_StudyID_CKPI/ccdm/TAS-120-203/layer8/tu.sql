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

				 tu_raw  as (select *, min(tudtc) OVER (PARTITION BY Study, SiteNumber,Subject, tulnkid)::text AS tudtc_min 
				 
				 FROM	(
					
					Select 		distinct 'TAS120_203':: text as Study, 
								concat('TAS120_203_',split_part("SiteNumber",'_',2)) :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::text as tuseq,
								null::text as tugrpid,								
								"NLID" :: text as tulnkid,
								(rank() over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject", "NLID" order by "NLDAT")) :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'NEW':: text as tuorres,
								'NEW'::text as tustresc,
								case when "NLSITE" = 'Other' then "NLSITEOT" else "NLSITE" end:: text as tuloc,
								case when "NLMETH"='Other' then "NLOTH" else "NLMETH" end:: Text as tumethod,
								'N'::text as tulobxfl,
								'N':: text as tublfl,
								'Independent Assessor'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("NLDAT") over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject" order by "NLID"):: 
								"NLDAT"::date as tudtc
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
                                 null::text as tuseq,	
                                 null::text as tugrpid,								
                                 "NTLBID":: text as tulnkid,
                                 '1' :: text as tulnkgrp,
								 null::text AS tutestcd,
								Null:: text as tutest,
								'NON-TARGET':: text as tuorres,
								'NON-TARGET'::text as tustresc,
								case when "NTLBSITE" = 'Other' then "NTLBSTOT" else "NTLBSITE" end:: text as tuloc,
								case when "NTLBMETH"='Other' then "NTLBOTH" else "NTLBMETH" end:: Text as tumethod,
								'Y'::text as tulobxfl,
								'Y':: text as tublfl,
								'Independent Assessor'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("NTLBDAT") over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject" order by "NTLBID"):: 
								"NTLBDAT"::date as tudtc
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
								null::text as tuseq,	
								null::text as tugrpid,								
								"NTLID":: text as tulnkid,
								(rank() over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject","NTLID" order by "NTLDAT")) :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'NON-TARGET':: text as tuorres,
								'NON-TARGET'::text as tustresc,
								case when "NTLSITE" = 'Other' then "NTLSTOT" else "NTLSITE" end:: text as tuloc,
								case when "NTLMETH"='Other' then "NTLOTH" else "NTLMETH" end:: Text as tumethod,
								'N'::text as tulobxfl,
								'N':: text as tublfl,
								'Independent Assessor'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
							    --min("NTLDAT") over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject" order by "NTLID"):: 
								"NTLDAT"::date as tudtc
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
								null::text as tuseq,	
								null::text as tugrpid,								
								"TLBID":: text as tulnkid,
								'1' :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'TARGET':: text as tuorres,
								'TARGET'::text as tustresc,
								case when "TLBSITE" = 'Other' then "TLBSTOT" else "TLBSITE" end:: text as tuloc,
								case when "TLBMETH"='Other' then "TLBOTH" else "TLBMETH" end:: Text as tumethod,
								'Y'::text as tulobxfl,
								'Y':: text as tublfl,
								'Independent Assessor'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("TLBDAT") over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject" order by "TLBID"):: 
								"TLBDAT"::date as tudtc
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
								null::text as tuseq,	
								null::text as tugrpid,								
								"TLID":: text as tulnkid,
								(rank() over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject", "TLID" order by "TLDAT")) :: text as tulnkgrp,
								null::text AS tutestcd,
								Null:: text as tutest,
								'TARGET':: text as tuorres,
								'TARGET'::text as tustresc,
								case when "TLSITE" = 'Other' then "TLSITEOT" else "TLSITE" end:: text as tuloc,
								case when "TLMETH"='Other' then "TLOTH" else "TLMETH" end:: Text as tumethod,
								'N'::text as tulobxfl,
								'N':: text as tublfl,
								'Independent Assessor'::text AS tueval,
				                'Investigator'::text AS tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("TLDAT") over (partition by 'TAS120_203', concat('TAS120_203_',split_part("SiteNumber",'_',2)), "Subject" order by "TLID"):: 
								"TLDAT"::date as tudtc
								--null::text as  tudy
					From 		TAS120_203."TL" tl
					left join 	dm
					on 			dm.studyid = 'TAS120_203'
					            and concat('TAS120_203_',split_part("SiteNumber",'_',2)) = dm.siteid
					            and tl."Subject"=dm.usubjid
		)a),
				
    tu_data AS (
        SELECT  distinct replace(tu.Study,'TAS120_203','TAS-120-203')::text AS studyid,
                tu.SiteNumber::text AS siteid,
                tu.Subject::text AS usubjid,
                rank() OVER (PARTITION BY tu.Study, tu.SiteNumber, tu.Subject)::numeric as tuseq,
                tugrpid::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                tu.tulnkid::text as tulnkid,
                tulnkgrp::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                tuorres::text AS tuorres,
                tustresc::text AS tustresc,
                null::text AS tunam,
                tuloc::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                tumethod::text AS tumethod,
                --case when tudtc <= e1.ex_mindt then 'Y' else 'N' end::text AS 
				tu.tulobxfl,
                tublfl::text AS tublfl,
                tueval::text AS tueval,
                --concat(tu.tuevalid,ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc))::text as 
				tu.tuevalid,
                null::text AS tuacptfl,
                --ROW_NUMBER() OVER (PARTITION BY Study, SiteNumber, Subject ORDER BY tudtc)::numeric as 
                coalesce (sv.visitnum,0) as visitnum,
                tu.visit::text AS visit,
                Null::numeric AS visitdy,
                null::numeric AS taetord,
                epoch::text AS epoch,
                tudtc::text as tudtc,
                (tu.tudtc::date-svv.svstdtc::date)::numeric AS tudy
				from tu_raw tu
				inner join (select distinct Study, SiteNumber, Subject,tulnkid,tuevalid,visit, tudtc_min,min(tudtc)
from tu_raw
group by 1,2,3,4,5,6,7) tu1
on tu.Study = tu1.Study and tu.SiteNumber=tu1.SiteNumber and tu.Subject =tu1.Subject and tu.tudtc:: date = tu1.tudtc_min:: date
and tu.visit=tu1.visit and  tu.tulnkid=tu1.tulnkid and tu.tuevalid=tu1.tuevalid 
		left join 	ex_data e1
		on 'TAS120_203'=e1.studyid and tu.SiteNumber=e1.siteid and tu.Subject= e1.usubjid
		
		left join sv_visit svv
on 'TAS120_203'=svv.studyid and tu.SiteNumber=svv.siteid and tu.Subject=svv.usubjid
		left join sv on tu.visit = sv.visit and 'TAS-120-203' = sv.studyid  and tu.SiteNumber = sv.siteid and tu.Subject = sv.usubjid
		where tu.tudtc is not null
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



