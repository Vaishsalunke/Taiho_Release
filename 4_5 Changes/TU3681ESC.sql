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
				 select 	studyid,siteid,usubjid,visit, exstdtc--min(exstdtc) ex_mindt_visit
				 from 		ex
				 where 		visit like '%Cycle 1 Day 1' 
				 and 		exdose is not null	
				 --group by 	1,2,3,4
				),
    tu_data AS (
        SELECT  distinct tu.Study::text AS studyid,
                tu.SiteNumber::text AS siteid,
                tu.Subject::text AS usubjid,
                ROW_NUMBER() OVER (PARTITION BY tu.Study, tu.SiteNumber, tu.Subject ORDER BY tu.tudtc)::numeric AS tuseq,
                tugrpid::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                coalesce (concat(tugrpid ,' - ', tulnkgrp), (row_number() over(partition by tu.Study, tu.SiteNumber,tu.Subject order by tudtc))::text) as tulnkid,
                tulnkgrp::text AS tulnkgrp,
                tu.tutestcd::text AS tutestcd,
                tu.tutest::text AS tutest,
                tu.tuorres::text AS tuorres,
                tustresc::text AS tustresc,
                null::text AS tunam,
                tu.tuloc::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                tu.tumethod::text AS tumethod,
                case when tu.tudtc <= e1.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                tu.tublfl::text AS tublfl,
                tueval::text AS tueval,
                --concat(tu.tuevalid,ROW_NUMBER() OVER (PARTITION BY tu.Study, tu.SiteNumber, tu.Subject ORDER BY tu.tudtc))
                tuevalid::text AS tuevalid,
                null::text AS tuacptfl,
                --ROW_NUMBER() OVER (PARTITION BY tu.Study, tu.SiteNumber, tu.Subject ORDER BY tu.tudtc)::numeric AS 
                sv.visitnum,
                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(tu.visit,'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),'<WK[0-9]D[0-9][0-9]/>',''),'<WK[0-9]DA[0-9][0-9]/>','')::text AS visit,
                Null::numeric AS visitdy,
                null::numeric AS taetord,
                tu.epoch::text AS epoch,
                tu.tudtc::text AS tudtc,
                (DATE_PART('day',tu.tudtc::timestamp - e2.exstdtc::timestamp):: numeric) +1::numeric AS tudy
		FROM	(
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								'New Lesion':: text as tugrpid,
								null:: text as tulnkid,
								"RecordPosition":: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'New Lesion':: text as tuorres,
								'New Lesion':: text as tustresc,
								"NLSITE":: text as tuloc,
								case when "NLMETH"='Other' then "NLOTH" else "NLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'INDEPENDENT ASSESSOR':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NLIMGDAT":: date as tudtc,
								null::text as  tudy
					From 		tas3681_101."NL" nl
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and nl."Subject"=dm.usubjid
										
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								NULL::numeric as tuseq,
								'Non-Target Lesion':: text as tugrpid,
								null:: text as tulnkid,
								"RecordPosition":: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'Non-Target Lesion':: text as tuorres,
								'Non-Target Lesion':: text as tustresc,
								"NTLBSITE":: text as tuloc,
								case when "NTLBMETH"='Other' then "NTLBOTH" else "NTLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								'INDEPENDENT ASSESSOR':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NTLBDAT":: date as tudtc,
								null::text as  tudy
					From 		tas3681_101."NTLB" ntlb
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and ntlb."Subject"=dm.usubjid
										
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								NULL::numeric as tuseq,
								'Non-Target Lesion':: text as tugrpid,
								null:: text as tulnkid,
								"RecordPosition":: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'Non-Target Lesion':: text as tuorres,
								'Non-Target Lesion':: text as tustresc,
								"NTLSITE":: text as tuloc,
								case when "NTLMETH"='Other' then "NTLOTH" else "NTLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'INDEPENDENT ASSESSOR':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"NTLDAT":: date as tudtc,
								null::text as  tudy
					From 		tas3681_101."NTL" ntl
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and ntl."Subject"=dm.usubjid
									
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								NULL::numeric as tuseq,
								'Target Lesion':: text as tugrpid,
								null:: text as tulnkid,
								"RecordPosition":: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'Target Lesion':: text as tuorres,
								'Target Lesion':: text as tustresc,
								"TLBSITE":: text as tuloc,
								case when "TLBMETH"='Other' then "TLBOTH" else "TLBMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'Y':: text as tublfl,
								'INDEPENDENT ASSESSOR':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLBDAT":: date as tudtc,
								null::text as  tudy
					From 		tas3681_101."TLB" tlb
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and tlb."Subject"=dm.usubjid
									
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								NULL::numeric as tuseq,
								'Target Lesion':: text as tugrpid,
								null:: text as tulnkid,
								"RecordPosition":: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'Target Lesion':: text as tuorres,
								'Target Lesion':: text as tustresc,
								"TLSITE":: text as tuloc,
								case when "TLMETH"='Other' then "TLOTH" else "TLMETH" end:: Text as tumethod,
								null::text as tulobxfl,
								'N':: text as tublfl,
								'INDEPENDENT ASSESSOR':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								"TLDAT":: date as tudtc,
								null::text as  tudy
					From 		tas3681_101."TL" tl
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and tl."Subject"=dm.usubjid
					
		)tu	
		left join 	ex_data e1
		on			'TAS3681_101_DOSE_ESC'=e1.studyid and SiteNumber=e1.siteid and tu.Subject= e1.usubjid
		left join	ex_visit e2
		on			'TAS3681_101_DOSE_ESC'=e2.studyid and SiteNumber=e2.siteid and tu.Subject= e2.usubjid
				left join sv
on 'TAS3681_101_DOSE_ESC'=sv.studyid and tu.SiteNumber=sv.siteid and tu.Subject=sv.usubjid
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
    /*KEY  , (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid)::text  AS objectuniquekey KEY*/  
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
;


