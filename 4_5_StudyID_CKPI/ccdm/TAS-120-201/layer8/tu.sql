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
                 or visit like '%Cycle 01 Day 01'-- done
				 or visit like '%Cycle 1 Day 1'-- done
				 or visit like 'Cycle 01'
				 ),		

tu_raw as ( select *, min(tudtc) OVER (PARTITION BY studyid, siteid,usubjid, tulnkid)::text AS tudtc_min
 FROM (	

SELECT  	distinct	null::text AS comprehendid,
                project::text AS studyid,
                "SiteNumber"::text AS siteid,
                "Subject"::text AS usubjid,
                null ::numeric AS tuseq,
               null::text AS tugrpid, --done
                null::text AS turefid,
                null::text AS tuspid,
                'NL' || "RecordPosition"::text AS tulnkid,
                (rank() over (partition by project, "SiteNumber", "Subject","RecordPosition" order by "NLDAT")) ::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'NEW'::text AS tuorres,
                'NEW'::text AS tustresc,
                null::text AS tunam,
                "NLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NLMETH")='other' then "NLOTH" else "NLMETH" end::text AS tumethod,
               -- case when "NLDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                'N'::text AS tulobxfl,
                'Independent Assessor'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
                nl."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
               -- dm.arm::text AS epoch,
                --min(nl."NLDAT") over (partition by project, "SiteNumber", "Subject", "RecordPosition")
                nl."NLDAT"::text AS tudtc
                --,("NLDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
                --,"RecordId"
from tas120_201."NL" nl

union all

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                "SiteNumber"::text AS siteid,
                "Subject"::text AS usubjid,
                null ::numeric AS tuseq,
                null::text AS tugrpid, --done
                null::text AS turefid,
                null::text AS tuspid,
                'NTL' || "RecordPosition"::text AS tulnkid,
                '1'::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'NON-TARGET'::text AS tuorres,
                'NON-TARGET'::text AS tustresc,
                null::text AS tunam,
                "NTLBSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NTLBMETH")='other' then "NTLBOTH" else "NTLBMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                'Y'::text AS tulobxfl,
                'Independent Assessor'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
                ntlb."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                --min(ntlb."NTLBDAT") over (partition by project, "SiteNumber", "Subject", "RecordPosition")::text AS tudtc
                ntlb."NTLBDAT"::text AS tudtc---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
                --,"RecordId"
from tas120_201."NTLB" ntlb

union all 

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                "SiteNumber"::text AS siteid,
                "Subject"::text AS usubjid,
                null ::numeric AS tuseq,
                null::text AS tugrpid, --done
                null::text AS turefid,
                null::text AS tuspid,
                'NTL' || "RecordPosition"::text AS tulnkid,
                (rank() over (partition by project, "SiteNumber", "Subject","RecordPosition" order by "NTLDAT")) ::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'NON-TARGET'::text AS tuorres,
                'NON-TARGET'::text AS tustresc,
                null::text AS tunam,
                "NTLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("NTLMETH")='other' then "NTLOTH" else "NTLMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                'N'::text AS tulobxfl,
                'Independent Assessor'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
                ntl."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                --min(ntl."NTLDAT")over (partition by project, "SiteNumber", "Subject", "RecordPosition")::text AS tudtc
                ntl."NTLDAT"::text AS tudtc---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
                --,"RecordId"
from tas120_201."NTL" ntl

union all 

SELECT  distinct		null::text AS comprehendid,
                project::text AS studyid,
                "SiteNumber"::text AS siteid,
                "Subject"::text AS usubjid,
                null ::numeric AS tuseq,
                null::text AS tugrpid, --done
                null::text AS turefid,
                null::text AS tuspid,
                'TL' || "RecordPosition"::text AS tulnkid,
                '1'::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'TARGET'::text AS tuorres,
                'TARGET'::text AS tustresc,
                null::text AS tunam,
                "TLBSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TLBMETH")='other' then "TLBOTH" else "TLBMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'Y'::text AS tublfl,
                'Y'::text AS tulobxfl,
                'Independent Assessor'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
                TLB."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                --min(TLB."TLBDAT")over (partition by project, "SiteNumber", "Subject", "RecordPosition")::text AS tudtc
                TLB."TLBDAT"::text AS tudtc---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
                --,"RecordId"
from tas120_201."TLB" TLB

union all 

select   distinct		null::text AS comprehendid,
                project::text AS studyid,
                "SiteNumber"::text AS siteid,
                "Subject"::text AS usubjid,
                null ::numeric AS tuseq,
                null::text AS tugrpid, --done
                null::text AS turefid,
                null::text AS tuspid,
                'TL' || "RecordPosition"::text AS tulnkid,
                (rank() over (partition by project, "SiteNumber", "Subject","RecordPosition" order by "TLDAT")) ::text AS tulnkgrp,
                'Lesion ID'::text AS tutestcd,
                'Lesion Identification'::text AS tutest,
                'TARGET'::text AS tuorres,
                'TARGET'::text AS tustresc,
                null::text AS tunam,
                "TLSITE"::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                case when lower("TLMETH")='other' then "TLOTH" else "TLMETH" end::text AS tumethod,
               -- case when "NTLBDAT"<= ex.ex_mindt then 'Y' else 'N' end::text AS tulobxfl,
                'N'::text AS tublfl,
                'N'::text AS tulobxfl,
                'Independent Assessor'::text AS tueval,
                'Investigator'::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
                TL."FolderName"::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                --dm.arm::text AS epoch,
                --min(TL."TLDAT")over (partition by project, "SiteNumber", "Subject", "RecordPosition")::text AS tudtc
                TL."TLDAT"::text AS tudtc---,("NTLBDAT"::date-exv.ex_mindt_visit::date)+1::numeric AS tudy
                
from tas120_201."TL" TL
)a),


    tu_data AS (
        SELECT distinct
		tu.comprehendid,
        replace (tu.studyid,'TAS120_201','TAS-120-201') as studyid,
        tu.siteid,
        tu.usubjid,
        rank() OVER (PARTITION BY tu.studyid, tu.siteid, tu.usubjid)::numeric as tuseq,
        tu.tugrpid,
        tu.turefid,
        tu.tuspid,
        --coalesce (concat(tugrpid ,' - ', tulnkgrp), (row_number() over(partition by tu.studyid, tu.siteid,tu.usubjid order by tudtc))::text) as 
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
        --case when tu.tudtc::date <= ex.ex_mindt then 'Y' else 'N' end::text AS 
        tulobxfl,
        tu.tublfl,
        tu.tueval,
        --concat(tu.tuevalid,ROW_NUMBER() OVER (PARTITION BY tu.studyid, tu.siteid, tu.usubjid ORDER BY tudtc))::text as 
        tu.tuevalid,-- done
        tu.tuacptfl,
        --ROW_NUMBER() OVER (PARTITION BY tu.studyid, tu.siteid, tu.usubjid ORDER BY tudtc) as visitnum,
        coalesce (sv.visitnum,0) as visitnum,
        tu.visit,
        tu.visitdy,
        tu.taetord,
        dm.arm epoch,
        tu.tudtc,
        (tu.tudtc::date-svv.svstdtc::date)::numeric AS tudy
from tu_raw tu
inner join (select distinct studyid, siteid, usubjid,tulnkid,tuevalid,visit, tudtc_min,min(tudtc)
from tu_raw
group by 1,2,3,4,5,6,7) tu1
on tu.studyid = tu1.studyid and tu.siteid=tu1.siteid and tu.usubjid =tu1.usubjid and tu.tudtc:: date = tu1.tudtc_min:: date
and tu.visit=tu1.visit and  tu.tulnkid=tu1.tulnkid and tu.tuevalid=tu1.tuevalid
left join ex_data ex
on tu.studyid=ex.studyid and tu.siteid=ex.siteid and tu.usubjid=ex.usubjid
left join dm
on tu.studyid=dm.studyid and tu.siteid=dm.siteid and tu.usubjid=dm.usubjid
left join sv_visit svv
on tu.studyid=svv.studyid and tu.siteid=svv.siteid and tu.usubjid=svv.usubjid
left join sv on tu.visit = sv.visit and 'TAS-120-201' = sv.studyid and tu.siteid = sv.siteid and tu.usubjid = sv.usubjid --done
where tu.tudtc is not null --done
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