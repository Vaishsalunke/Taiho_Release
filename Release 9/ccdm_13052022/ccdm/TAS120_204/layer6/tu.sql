/*
CCDM TU Table mapping
Notes: Standard mapping to CCDM TU table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
               
ex_data as (
    select studyid,siteid,usubjid,min(exstdtc) ex_mindt
from cqs.ex
group by studyid,siteid,usubjid
),
ex_visit as (
select studyid,siteid,usubjid,visit,exstdtc ex_mindt_visit
from cqs.ex
where visit like '%Cycle 1 Day 1' and exdose is not null
),
    tu_data AS (
        SELECT  distinct
        study::text AS studyid,
                concat(study,'_',split_part(tu.siteid,'_',2))::text AS siteid,
                tu.usubjid::text AS usubjid,
                (ROW_NUMBER() OVER (PARTITION BY tu.study, tu.siteid, tu.usubjid ORDER BY tu.tudtc))::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                concat(tulnkid,(ROW_NUMBER() OVER (PARTITION BY tu.study, tu.siteid, tu.usubjid ORDER BY tu.tudtc)))::text AS tulnkid,
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
               case when tudtc <= ex1.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                tublfl::text AS tublfl,
                null::text AS tueval,
                tuevalid::text AS tuevalid,
                null::text AS tuacptfl,
                (ROW_NUMBER() OVER (PARTITION BY tu.study, tu.siteid, tu.usubjid ORDER BY tu.tudtc))::numeric AS visitnum,
                tu.visit::text AS visit,
                Null::numeric AS visitdy,
                null::numeric AS taetord,
               dm."arm"::text AS epoch,
               tudtc::text AS tudtc,
             (tudtc::date - ex1.ex_mindt::date)+1::numeric AS tudy
                from (

--NL
Select "project"::text as study,
"Subject" :: text as usubjid,
"SiteNumber" :: text as siteid,
NULL::numeric as tuseq,
NULL::text as tugrpid,
NULL::text as turefid,
NULL::text as tuspid,
"RecordPosition":: text as tulnkid,
NULL::text as tulnkgrp,
case when nullif("NLSITE",'') is not null then concat("RecordPosition",'-',"NLSITE") 
else 'NA'
end:: text as tutestcd,
'Lesion Identification':: text as tutest,
'Present':: text as tuorres,
NULL::text as tustresc,
NULL::text as tunam,
"NLSITE":: text as tuloc,
NULL:: text as tulat,
NULL:: text as tudir,
NULL:: text as tuportot,
case when "NLMETH"='Other' then "NLOTH" else "NLMETH" end:: Text as tumethod,
null::text as tulobxfl,
'N':: text as tublfl,
NULL:: text as tueval,
'Radiologist':: text as tuevalid,
NULL:: text as tuacptfl,
null:: numeric as visitnum,
"FolderName"::text as visit,
NULL:: text as visitdy,
NULL:: text as taetord,
null:: text as epoch,
"NLDAT":: date as tudtc,
null::text as  tudy
From tas120_204."NL" n1

union all 
---NTLB

Select
"project"::text as study,
"Subject" :: text as usubjid,
"SiteNumber" :: text as siteid,
NULL::numeric as tuseq,
NULL::text as tugrpid,
NULL::text as turefid,
NULL::text as tuspid,
"RecordPosition":: text as tulnkid,
NULL::text as tulnkgrp,
case when nullif("NTLBSITE",'') is not null then concat("RecordPosition",'-',"NTLBSITE") 
else 'NA'
end:: text as tutestcd,
'Lesion Identification':: text as tutest,
'Present':: text as tuorres,
NULL::text as tustresc,
NULL::text as tunam,
"NTLBSITE":: text as tuloc,
NULL:: text as tulat,
NULL:: text as tudir,
null:: text as tuportot,
case when "NTLBMETH"='Other' then "NTLBOTH" else "NTLBMETH" end:: Text as tumethod,
null::text as tulobxfl,
'Y':: text as tublfl,
NULL:: text as tueval,
'Radiologist':: text as tuevalid,
null:: text as tuacptfl,
null:: numeric as visitnum,
"FolderName"::text as visit,
NULL:: text as visitdy,
NULL:: text as taetord,
null:: text as epoch,
"NTLBDAT":: date as tudtc,
null::text as  tudy
From tas120_204."NTLB" ntlb

union all
--NTL
Select
"project"::text as study,
"Subject" :: text as usubjid,
"SiteNumber" :: text as siteid,
NULL::numeric::numeric as tuseq,
NULL::text as tugrpid,
NULL::text as turefid,
NULL::text as tuspid,
"RecordPosition":: text as tulnkid,
NULL::text as tulnkgrp,
case when nullif("NTLSITE",'') is not null then concat("RecordPosition",'-',"NTLSITE") 
else 'NA'
end:: text as tutestcd,
'Lesion Identification':: text as tutest,
"NTLBSTAT":: text as tuorres,
NULL::text as tustresc,
NULL::text as tunam,
"NTLSITE":: text as tuloc,
NULL:: text as tulat,
NULL:: text as tudir,
NULL:: text as tuportot,
case when "NTLMETH"='Other' then "NTLOTH" else "NTLMETH" end:: Text as tumethod,
null::text as tulobxfl,
'N':: text as tublfl,
NULL:: text as tueval,
'Radiologist':: text as tuevalid,
NULL:: text as tuacptfl,
null:: numeric as visitnum,
"FolderName"::text as visit,
NULL:: text as visitdy,
NULL:: text as taetord,
null:: text as epoch,
"NTLDAT":: date as tudtc,
null::text as  tudy
From tas120_204."NTL" ntl

union all
--TLB
Select "project"::text as study,
"Subject" :: text as usubjid,
"SiteNumber" :: text as siteid,
NULL::numeric::numeric as tuseq,
NULL::text as tugrpid,
NULL::text as turefid,
NULL::text as tuspid,
"RecordPosition":: text as tulnkid,
NULL::text as tulnkgrp,
case when nullif("TLBSITE",'') is not null then concat("RecordPosition",'-',"TLBSITE") 
else 'NA'
end:: text as tutestcd,
'Lesion Identification':: text as tutest,
"TLBDIM":: text as tuorres,
NULL::text as tustresc,
NULL::text as tunam,
"TLBSITE":: text as tuloc,
NULL:: text as tulat,
NULL:: text as tudir,
NULL:: text as tuportot,
case when "TLBMETH"='Other' then "TLBOTH" else "TLBMETH" end:: Text as tumethod,
null::text as tulobxfl,
'Y':: text as tublfl,
NULL:: text as tueval,
'Radiologist':: text as tuevalid,
NULL:: text as tuacptfl,
null:: numeric as visitnum,
"FolderName"::text as visit,
NULL:: text as visitdy,
NULL:: text as taetord,
null:: text as epoch,
"TLBDAT":: date as tudtc,
null::text as  tudy
From tas120_204."TLB" tlb

union all
---TL
Select
"project"::text as study,
"Subject" :: text as usubjid,
"SiteNumber" :: text as siteid,
null::numeric as tuseq,
NULL::text as tugrpid,
NULL::text as turefid,
NULL::text as tuspid,
"RecordPosition":: text as tulnkid,
NULL::text as tulnkgrp,
case when nullif("TLSITE",'') is not null then concat("RecordPosition",'-',"TLSITE") 
else 'NA'
end:: text as tutestcd,
'Lesion Identification':: text as tutest,
"TLDIM":: text as tuorres,
NULL::text as tustresc,
NULL::text as tunam,
"TLSITE":: text as tuloc,
NULL:: text as tulat,
NULL:: text as tudir,
NULL:: text as tuportot,
case when "TLMETH"='Other' then "TLOTH" else "TLMETH" end:: Text as tumethod,
null::text as tulobxfl,
'N':: text as tublfl,
NULL:: text as tueval,
'Radiologist':: text as tuevalid,
NULL:: text as tuacptfl,
null:: numeric as visitnum,
"FolderName"::text as visit,
NULL:: text as visitdy,
NULL:: text as taetord,
null:: text as epoch,
"TLDAT":: date as tudtc,
null::text as  tudy
From tas120_204."TL" tl
)tu
					left join 	ex_data ex1
					on			tu."study" = ex1."studyid" and concat(study,'_',split_part(tu.siteid,'_',2))=ex1.siteid and tu.usubjid= ex1."usubjid"
					left join	ex_visit ex2
					on			tu."study"=ex2."studyid" and concat(study,'_',split_part(tu.siteid,'_',2)) = ex2.siteid and tu.usubjid= ex2."usubjid" 
					left join 	cqs.dm
					on 		tu."study" = dm."studyid" and concat(study,'_',split_part(tu.siteid,'_',2)) = dm.siteid and tu.usubjid=dm."usubjid"
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
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
;


