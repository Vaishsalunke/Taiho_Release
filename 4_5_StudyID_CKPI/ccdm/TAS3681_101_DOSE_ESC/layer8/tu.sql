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

tu_raw as (
	select *, min(tudtc) OVER (PARTITION BY study, SiteNumber,subject, tulnkid)::text AS tudtc_min
	FROM	(
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								null:: text as tugrpid,
								'NL' || "RecordPosition":: text as tulnkid,
								(rank () over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition" order by "NLIMGDAT")):: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'NEW':: text as tuorres,
								'NEW':: text as tustresc,
								"NLSITE":: text as tuloc,
								case when "NLMETH"='Other' then "NLOTH" else "NLMETH" end:: Text as tumethod,
								'N'::text as tulobxfl,
								'N':: text as tublfl,
								'Independent Assessor':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("NLIMGDAT") over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition")
								"NLIMGDAT"::text AS tudtc,
								null::text as  tudy
					From 		tas3681_101."NL" nl
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and nl."Subject"=dm.usubjid
										
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								null:: text as tugrpid,
								'NTL' || "RecordPosition":: text as tulnkid,
								'1':: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'NON-TARGET':: text as tuorres,
								'NON-TARGET':: text as tustresc,
								"NTLBSITE":: text as tuloc,
								case when "NTLBMETH"='Other' then "NTLBOTH" else "NTLBMETH" end:: Text as tumethod,
								'Y'::text as tulobxfl,
								'Y':: text as tublfl,
								'Independent Assessor':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("NTLBDAT") over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition")
								"NTLBDAT"::text AS tudtc,
								null::text as  tudy
					From 		tas3681_101."NTLB" ntlb
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and ntlb."Subject"=dm.usubjid
										
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								null:: text as tugrpid,
								'NTL' || "RecordPosition":: text as tulnkid,
								(rank () over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition" order by "NTLDAT")):: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'NON-TARGET':: text as tuorres,
								'NON-TARGET':: text as tustresc,
								"NTLSITE":: text as tuloc,
								case when "NTLMETH"='Other' then "NTLOTH" else "NTLMETH" end:: Text as tumethod,
								'N'::text as tulobxfl,
								'N':: text as tublfl,
								'Independent Assessor':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("NTLDAT") over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition")
								"NTLDAT"::text AS tudtc,
								null::text as  tudy
					From 		tas3681_101."NTL" ntl
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and ntl."Subject"=dm.usubjid
									
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								null:: text as tugrpid,
								'TL' || "RecordPosition":: text as tulnkid,
								'1':: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'TARGET':: text as tuorres,
								'TARGET':: text as tustresc,
								"TLBSITE":: text as tuloc,
								case when "TLBMETH"='Other' then "TLBOTH" else "TLBMETH" end:: Text as tumethod,
								'Y'::text as tulobxfl,
								'Y':: text as tublfl,
								'Independent Assessor':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("TLBDAT") over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition")
								"TLBDAT"::text AS tudtc,
								null::text as  tudy
					From 		tas3681_101."TLB" tlb
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and tlb."Subject"=dm.usubjid
									
					union all
					
					Select 	distinct	'TAS3681_101_DOSE_ESC':: text as Study, 
								"SiteNumber" :: text as SiteNumber,
								"Subject" :: text as Subject,
								null::numeric as tuseq,
								null:: text as tugrpid,
								'TL' || "RecordPosition":: text as tulnkid,
								(rank () over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition" order by "TLDAT")):: text as tulnkgrp,
								'Lesion ID'::text AS tutestcd,
								'Lesion Identification':: text as tutest,
								'TARGET':: text as tuorres,
								'TARGET':: text as tustresc,
								"TLSITE":: text as tuloc,
								case when "TLMETH"='Other' then "TLOTH" else "TLMETH" end:: Text as tumethod,
								'N'::text as tulobxfl,
								'N':: text as tublfl,
								'Independent Assessor':: text as tueval,
								'Investigator':: text as tuevalid,
								null:: numeric as visitnum,
								"FolderName"::text as visit,
								dm."arm":: text as epoch,
								--min("TLDAT") over (partition by 'TAS3681_101_DOSE_ESC', "SiteNumber", "Subject", "RecordPosition")
								"TLDAT"::text AS tudtc,
								null::text as  tudy
					From 		tas3681_101."TL" tl
					left join 	dm
					on 			'TAS3681_101_DOSE_ESC'=dm.studyid and "SiteNumber"=dm.siteid and tl."Subject"=dm.usubjid
					
		)a),

    tu_data AS (
        SELECT  distinct tu.Study::text AS studyid,
                tu.SiteNumber::text AS siteid,
                tu.Subject::text AS usubjid,
                rank() over (partition by tu.Study, tu.SiteNumber, tu.Subject)::numeric AS tuseq,
                tugrpid::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                --coalesce (concat(tugrpid ,' - ', tulnkgrp), (row_number() over(partition by tu.Study, tu.SiteNumber,tu.Subject order by tudtc))::text) as 
				tulnkid,
                tulnkgrp,
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
                --case when tu.tudtc <= e1.ex_mindt then 'Y' else 'N' end::text AS 
                tulobxfl,
                tu.tublfl::text AS tublfl,
                tueval::text AS tueval,
                --concat(tu.tuevalid,ROW_NUMBER() OVER (PARTITION BY tu.Study, tu.SiteNumber,tu.Subject ORDER BY tudtc))::text as 
				tuevalid,
                null::text AS tuacptfl,
                --ROW_NUMBER() OVER (PARTITION BY tu.Study, tu.SiteNumber, tu.Subject ORDER BY tu.tudtc)::numeric AS 
                coalesce (sv.visitnum,0) as visitnum,
                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(tu.visit,'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),'<WK[0-9]D[0-9][0-9]/>',''),'<WK[0-9]DA[0-9][0-9]/>','')::text AS visit,
                Null::numeric AS visitdy,
                null::numeric AS taetord,
                tu.epoch::text AS epoch,
                tu.tudtc::text AS tudtc,
                (tu.tudtc::date-svv.svstdtc::date)::numeric AS tudy
						from tu_raw tu
		inner join (select distinct study, SiteNumber, Subject, tudtc_min
			from tu_raw) tu1
on tu.study = tu1.study and tu.SiteNumber=tu1.SiteNumber and tu.Subject =tu1.Subject and tu.tudtc:: date = tu1.tudtc_min:: date  
		left join 	ex_data e1
		on			'TAS3681_101_DOSE_ESC'=e1.studyid and tu.SiteNumber=e1.siteid and tu.Subject= e1.usubjid
		left join sv_visit svv
on 'TAS3681_101_DOSE_ESC'=svv.studyid and tu.SiteNumber=svv.siteid and tu.Subject=svv.usubjid
				left join sv
on tu.visit = sv.visit and 'TAS3681_101_DOSE_ESC' = sv.studyid and tu.SiteNumber = sv.siteid and tu.Subject=sv.usubjid
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
    tu.tudtc::text AS tudtc,
    tu.tudy::numeric AS tudy
    /*KEY  , (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid)::text  AS objectuniquekey KEY*/  
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
;


