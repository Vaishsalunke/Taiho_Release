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
        SELECT distinct
		tu.comprehendid,
        tu.studyid,
        tu.siteid,
        tu.usubjid,
        ROW_NUMBER() OVER (PARTITION BY tu.studyid, tu.siteid, tu.usubjid ORDER BY tudtc)::numeric as tuseq,
        tu.tugrpid,
        tu.turefid,
        tu.tuspid,
        tulnkid,
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
        --tu.tuevalid,
        concat(tu.tuevalid,ROW_NUMBER() OVER (PARTITION BY tu.studyid, tu.siteid, tu.usubjid ORDER BY tudtc))::text as tuevalid,
        tu.tuacptfl,
        --ROW_NUMBER() OVER (PARTITION BY tu.studyid, tu.siteid, tu.usubjid ORDER BY tudtc) 
        COALESCE(sv.visitnum,0) as visitnum,
        tu.visit,
        tu.visitdy,
        tu.taetord,
        dm.arm epoch,
        tu.tudtc,
        (tu.tudtc::date-svv.svstdtc::date)::numeric AS tudy
FROM (	

SELECT  	distinct	null::text AS comprehendid,
                project::text AS studyid,
                concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
                "Subject"::text AS usubjid,
                null::numeric AS tuseq,
                'New Lesion'::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM2"::text AS tulnkid,
                "RecordPosition" ::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'New Lesion'::text AS tuorres,
                'New Lesion'::text AS tustresc,
                null::text AS tunam,
                case when "NLSITE"='29-Other' then "NLOTHST" else "NLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NLMETH")='other' then "NLOTH" else "NLMETH" end::text AS tumethod,
               -- case when "NLDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                'INDEPENDENT ASSESSOR'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
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
                'Non-Target Lesion'::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM1"::text AS tulnkid,
                "RecordPosition"::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'Non-Target Lesion'::text AS tuorres,
                'Non-Target Lesion'::text AS tustresc,
                null::text AS tunam,
                case when "NTLSITE"='29-Other' then "TLOTHSP" else "NTLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                'INDEPENDENT ASSESSOR'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
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
                'Non-Target Lesion'::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM1"::text AS tulnkid,
                "RecordPosition"::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'Non-Target Lesion'::text AS tuorres,
                'Non-Target Lesion'::text AS tustresc,
                null::text AS tunam,
                case when "NTLSITE"='29-Other' then "TLOTHSP" else "NTLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                'INDEPENDENT ASSESSOR'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
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
                'Target Lesion'::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM"::text AS tulnkid,
                "RecordPosition"::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'Target Lesion'::text AS tuorres,
                'Target Lesion'::text AS tustresc,
                null::text AS tunam,
                case when "TLSITE"='27-Other' then "TLOTHSP" else "TLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                'INDEPENDENT ASSESSOR'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
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
                'Target Lesion'::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                "TUNUM"::text AS tulnkid,
                "RecordPosition"::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'Target Lesion'::text AS tuorres,
                'Target Lesion'::text AS tustresc,
                null::text AS tunam,
                case when "TLSITE"='27-Other' then "TLOTHSP" else "TLSITE" end::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TUMETH4")='other' then "EVALOTHSP" else "TUMETH4" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                'INDEPENDENT ASSESSOR'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
                TLB."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                "TUSTDT"::text AS tudtc
                ---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
from tas117_201."TLPB" TLB
)tu
left join ex_data ex
on tu.studyid=ex.studyid and tu.siteid=ex.siteid and tu.usubjid=ex.usubjid
left join dm
on tu.studyid=dm.studyid and tu.siteid=dm.siteid and tu.usubjid=dm.usubjid
left join sv_visit svv
on tu.studyid=svv.studyid and tu.siteid=svv.siteid and tu.usubjid=svv.usubjid
left join sv on tu.studyid = sv.studyid and sv.siteid = tu.siteid and sv.usubjid = tu.usubjid 

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
    /*KEY , (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
;





