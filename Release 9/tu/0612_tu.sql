/*
CCDM TU Table mapping
Notes: Standard mapping to CCDM TU table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject
                ),
				
	ex_data as (
				select studyid,siteid,usubjid,min(exstdtc) ex_mindt
				from cqs.ex
				group by 1,2,3
				),
	ex_visit as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_mindt_visit
				 from cqs.ex
				 where visit like '%Cycle 01' and exdose is not null
				 
				 ),		
    tu_data AS (
        SELECT distinct
		tu.comprehendid,
        tu.studyid,
        tu.siteid,
        tu.usubjid,
        ROW_NUMBER() OVER (PARTITION BY tu.studyid, tu.siteid, tu.usubjid ORDER BY tudtc) as tuseq,
        tu.tugrpid,
        tu.turefid,
        tu.tuspid,
        tu.tulnkid,
        tu.tulnkgrp,
        tu.tutestcd,
        tu.tutest,
        tu.tuorres,
        tu.tustresc,
        tu.tunam,
        tu.tuloc,
        tu.tulat,
        tu.tudir,
        tu.tuportot,
        tu.tumethod,
        case when tu.tudtc::date <= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
        tu.tublfl,
        tu.tueval,
        tu.tuevalid,
        tu.tuacptfl,
        tu.visitnum,
        tu.visit,
        tu.visitdy,
        tu.taetord,
        dm.arm epoch,
        tu.tudtc,
        (tu.tudtc::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
FROM (	

SELECT  	distinct	null::text AS comprehendid,
                project::text AS studyid,
                concat("SiteNumber",'_',split_part("Site",'_',1))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "NLNUM"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("NLSITE",'') is not null then concat("NLNUM",'-',"NLSITE")
				else 'NA' end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'Present'::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                "NLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NLMETH")='other' then "NLOTH" else "NLMETH" end::text AS tumethod,
               -- case when "NLDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                case when "NLNUM" ~ '[A-Z]' then split_part(nullif("NLNUM",''),'0',2) else "NLNUM" end ::numeric AS visitnum,
                nl."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
               -- dm.arm::text AS epoch,
                nl."NLDAT"::text AS tudtc
                --,("NLDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas0612_101."NL" nl

union all

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat("SiteNumber",'_',split_part("Site",'_',1))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "NTLSNUM"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("NTLSITE",'') is not null then concat("NTLSNUM",'-',"NTLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                case when lower("NTLBYN")='yes' then 'Present' 
					 when lower("NTLBYN")='no'  then  'Absent' 
				end::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                "NTLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NTLBMETH")='other' then "NTLBOTH" else "NTLBMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                case when "NTLSNUM" ~ '[A-Z]' then split_part(nullif("NTLSNUM",''),'0',2) else nullif("NTLSNUM",'') end ::numeric AS visitnum,
                ntlb."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                ntlb."NTLBDAT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas0612_101."NTLB" ntlb

union all 

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat("SiteNumber",'_',split_part("Site",'_',1))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "NTLSNUM"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("NTLSITE",'') is not null then concat("NTLSNUM",'-',"NTLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                "NTLBSTAT"::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                "NTLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NTLMETH")='other' then "NTLOTH" else "NTLMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                case when "NTLSNUM" ~ '[A-Z]' then split_part(nullif("NTLSNUM",''),'0',2) else nullif("NTLSNUM",'') end::numeric AS visitnum,
                ntl."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                ntl."NTLDAT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas0612_101."NTL" ntl

union all 

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat("SiteNumber",'_',split_part("Site",'_',1))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "LSNUM"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("TLSITE",'') is not null then concat("LSNUM",'-',"TLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                "TLBDIM"::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                "TLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TLBMETH")='other' then "TLBOTH" else "TLBMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                case when "LSNUM" ~ '[A-Z]' then split_part(nullif("LSNUM",''),'0',2) else nullif("LSNUM",'') end ::numeric AS visitnum,
                TLB."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                TLB."TLBDAT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas0612_101."TLB" TLB

union all 

select   distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat("SiteNumber",'_',split_part("Site",'_',1))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "LSNUM"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("TLSITE",'') is not null then concat("LSNUM",'-',"TLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                "TLDIM"::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                "TLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TLMETH")='other' then "TLOTH" else "TLMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                case when "LSNUM" ~ '[A-Z]' then split_part(nullif("LSNUM",''),'0',2) else nullif("LSNUM",'') end ::numeric AS visitnum,
                TL."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                TL."TLDAT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas0612_101."TL" TL
)tu
left join ex_data ex
on tu.studyid=ex.studyid and tu.siteid=ex.siteid and tu.usubjid=ex.usubjid
left join cqs.dm
on tu.studyid=dm.studyid and tu.siteid=dm.siteid and tu.usubjid=dm.usubjid
left join ex_visit exv
on tu.studyid=exv.studyid and tu.siteid=exv.siteid and tu.usubjid=exv.usubjid

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
    /*KEY , (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid|| '~' || tu.tuseq )::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
;


