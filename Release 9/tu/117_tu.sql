/*
CCDM TU Table mapping
Notes: Standard mapping to CCDM TU table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
				
	ex_data as (
				select studyid,siteid,usubjid,min(exstdtc) ex_mindt
				from cqs.ex
				group by 1,2,3
				),
	ex_visit as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_mindt_visit
				 from cqs.ex
				 where visit like '%Day 1 Cycle 01' and exdose is not null
				 
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
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM2"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("NLSITE",'') is not null then concat("TUNUM2",'-',"NLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                case when "NLNO"=0 then 'Present'
                	 else 'Absent'
                end::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                case when "NLSITE"='29-Other' then "NLOTHST" else "NLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NLMETH")='other' then "NLOTH" else "NLMETH" end::text AS tumethod,
               -- case when "NLDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                "TUNUM2"::numeric AS visitnum,
                nl."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
               -- dm.arm::text AS epoch,
                nl."NLDAT"::text AS tudtc
                --,("NLDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas117_201."NL" nl

union all

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM1"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("NTLSITE",'') is not null then concat("TUNUM1",'-',"NTLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'Present'::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                case when "NTLSITE"='29-Other' then "TLOTHSP" else "NTLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                "TUNUM1"::numeric AS visitnum,
                ntlb."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                "TUSTDT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas117_201."NTLBASE" ntlb

union all 

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM1"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("NTLSITE",'') is not null then concat("TUNUM1",'-',"NTLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                case when "NA"=1 then 'Not Assessed'
                	 when "NA"=0 or "NA" is null then 'Assessed'
                end::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                case when "NTLSITE"='29-Other' then "TLOTHSP" else "NTLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                "TUNUM1"::numeric AS visitnum,
                ntl."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                "TUSTDT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas117_201."NTLPB" ntl

union all 

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("TLSITE",'') is not null then concat("TUNUM",'-',"TLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                "MEASURMT"::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                case when "TLSITE"='27-Other' then "TLOTHSP" else "TLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                "TUNUM"::numeric AS visitnum,
                TL."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                "TUSTDT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas117_201."TLRN2" TL

union all 

select   distinct		null::text AS comprehendid,
                project::text AS studyid,
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM"::text AS tulnkid,
                null::text AS tulnkgrp,
                case when nullif("TLSITE",'') is not null then concat("TUNUM",'-',"TLSITE")
				else 'NA'
                end::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                "MEASURMT"::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                case when "TLSITE"='27-Other' then "TLOTHSP" else "TLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH4")='other' then "EVALOTHSP" else "TUMETH4" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                null::text AS tueval,
                'Radiologist'::text AS tuevalid,
                null::text AS tuacptfl,
                "TUNUM"::numeric AS visitnum,
                TLB."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                "TUSTDT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas117_201."TLPB" TLB
)tu
left join ex_data ex
on tu.usubjid=ex.usubjid
left join cqs.dm
on tu.usubjid=dm.usubjid
left join ex_visit exv
on tu.usubjid=exv.usubjid

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
    /*KEY , (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid ||'~' || tu.tuseq)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
;


