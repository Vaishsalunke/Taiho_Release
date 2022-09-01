/*
CCDM TR Table mapping
Notes: Standard mapping to CCDM TR table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
		


	/*ex_data as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_dt
				 from ex
				 where visit like '%Cycle 1 Day 1' and exdose is not null
							 			 
				)*/   ex_data as (
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
				
				
    tr_data AS (
        select distinct tr.comprehendid,
						replace (tr.studyid,'TAS120_201','TAS-120-201') as studyid,
						tr.siteid,
						tr.usubjid,
						row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc) as trseq,
						trgrpid,
						trrefid,
						trspid,
						--coalesce (concat(trgrpid ,' - ', trlnkgrp), (row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc))::text) as 
						trlnkid,
						trlnkgrp,
						trtestcd,
						trtest,
						trorres,
						trorresu,
						trstresc,
						trstresn,
						trstresu,
						trstat,
						trreasnd,
						trnam,
						trmethod,
						--case when tr.trdtc::date <= ex.ex_mindt then 'Y' else 'N' end::text AS 
						trlobxfl,
						trblfl,
						treval,
						concat(tr.trevalid,row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc))::text as trevalid,-- done
						tracptfl,
						--row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc) as visitnum,
						coalesce(sv.visitnum,0) as visitnum, 
						tr.visit,
						tr.visitdy,
						tr.taetord,
						dm.arm::text as epoch,
						trdtc::date
						,(tr.trdtc::date-svv.svstdtc::date)::numeric AS trdy

		from
				(
				
				
				SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'NEW LESION' ::text AS trgrpid, --done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'NL' || "RecordPosition" ::text AS trlnkid,
                	row_number() over (partition by nl.project, concat(nl.project,'_',split_part(nl."SiteNumber",'_',2)), nl."Subject" order by nl."NLDAT") ::text AS trlnkgrp,
                	'TUMSTATE'::text AS trtestcd,
                	'Tumor State'::text AS trtest,
                	'Present'::text AS trorres,
	                'mm'::text AS trorresu,
	                'Present' ::text AS trstresc,
	                1::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                null ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("NLMETH")='other' then "NLOTH" else "NLMETH" end::text AS trmethod,
	                'N'::text AS trblfl,
	                'N'::text as trlobxfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                nl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	nl."NLDAT"::text AS trdtc
                	from tas120_201."NL" nl
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'NON-TARGET LESION' ::text AS trgrpid, --done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'NTL' || "RecordPosition" ::text AS trlnkid,
                	'1' ::text AS trlnkgrp,
                	'TUMSTATE'::text AS trtestcd,
                	'Tumor State'::text AS trtest,
                	'Present'::text AS trorres,
	                'mm'::text AS trorresu,
	                'Present' ::text AS trstresc,
	                1::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                case when lower("NTLBYN") = 'yes' then null else 'Not Done' end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("NTLBMETH")='other' then "NTLBOTH" else "NTLBMETH" end::text AS trmethod,
	                'Y'::text AS trblfl,
	                'Y'::text as trlobxfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                ntlb."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	ntlb."NTLBDAT"::text AS trdtc
                	from tas120_201."NTLB" ntlb
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'NON-TARGET LESION' ::text AS trgrpid, --done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'NTL' || "RecordPosition" ::text AS trlnkid,
                	row_number() over (partition by ntl.project, concat(ntl.project,'_',split_part(ntl."SiteNumber",'_',2)), ntl."Subject" order by ntl."NTLDAT") ::text AS trlnkgrp,
                	'TUMSTATE'::text AS trtestcd,
                	'Tumor State'::text AS trtest,
                	ntl."NTLBSTAT" ::text AS trorres,
	                'mm'::text AS trorresu,
	                ntl."NTLBSTAT" ::text AS trstresc,
	                1::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                case when lower("NTLYN") = 'yes' then null else 'Not Done' end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("NTLMETH")='other' then "NTLOTH" else "NTLMETH" end::text AS trmethod,
	                'N'::text AS trblfl,
	                'N'::text as trlobxfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                ntl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	ntl."NTLDAT"::text AS trdtc
                	from tas120_201."NTL" ntl
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'TARGET LESION' ::text AS trgrpid, --done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'TL' || "RecordPosition" ::text AS trlnkid,
                	row_number() over (partition by tl.project, concat(tl.project,'_',split_part(tl."SiteNumber",'_',2)), tl."Subject" order by tl."TLDAT") ::text AS trlnkgrp,
                	'LDIAM'::text AS trtestcd,
                	'Longest Diameter'::text AS trtest,
                	tl."TLDIM"  ::text AS trorres,
	                'mm'::text AS trorresu,
	                tl."TLDIM" ::text AS trstresc,
	                tl."TLDIM"  ::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	               case when lower("TLYN") = 'yes' then null else 'Not Done' end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("TLMETH")='other' then "TLOTH" else "TLMETH" end::text AS trmethod,
	                'N'::text AS trblfl,
	                'N'::text as trlobxfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                tl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	tl."TLDAT"::text AS trdtc
                	from tas120_201."TL" tl
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'TARGET LESION' ::text AS trgrpid, --done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'TL' || "RecordPosition" ::text AS trlnkid,
                	'1' ::text AS trlnkgrp,
                	'LDIAM'::text AS trtestcd,
                	'Longest Diameter'::text AS trtest,
                	tlb."TLBDIM"  ::text AS trorres,
	                'mm'::text AS trorresu,
	                tlb."TLBDIM" ::text AS trstresc,
	                tlb."TLBDIM"  ::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                case when lower("TLBYN") = 'yes' then null else 'Not Done' end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("TLBMETH")='other' then "TLBOTH" else "TLBMETH" end::text AS trmethod,
	                'Y'::text AS trblfl,
	                'Y'::text as trlobxfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                tlb."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	tlb."TLBDAT"::text AS trdtc
                	from tas120_201."TLB" tlb
					
					) tr 
		
		left join dm 
		on tr.studyid=dm.studyid and tr.siteid=dm.siteid and tr.usubjid=dm.usubjid
		left join ex_data ex 
		on tr.studyid=ex.studyid and tr.siteid=ex.siteid and tr.usubjid=ex.usubjid
		left join sv_visit svv
			on tr.studyid=svv.studyid and tr.siteid=svv.siteid and tr.usubjid=svv.usubjid
		left join sv on tr.visit = sv.visit-- done
		where tr.trdtc is not null -- done
                )

SELECT
    /*KEY (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid)::text AS comprehendid, KEY*/  
    tr.studyid::text AS studyid,
    tr.siteid::text AS siteid,
    tr.usubjid::text AS usubjid,
    tr.trseq::numeric AS trseq,
    tr.trgrpid::text AS trgrpid,
    tr.trrefid::text AS trrefid,
    tr.trspid::text AS trspid,
    tr.trlnkid::text AS trlnkid,
    tr.trlnkgrp::text AS trlnkgrp,
    tr.trtestcd::text AS trtestcd,
    tr.trtest::text AS trtest,
    tr.trorres::text AS trorres,
    tr.trorresu::text AS trorresu,
    tr.trstresc::text AS trstresc,
    tr.trstresn::numeric AS trstresn,
    tr.trstresu::text AS trstresu,
    tr.trstat::text AS trstat,
    tr.trreasnd::text AS trreasnd,
    tr.trnam::text AS trnam,
    tr.trmethod::text AS trmethod,
    tr.trlobxfl::text AS trlobxfl,
    tr.trblfl::text AS trblfl,
    tr.treval::text AS treval,
    tr.trevalid::text AS trevalid,
    tr.tracptfl::text AS tracptfl,
    tr.visitnum::numeric AS visitnum,
    tr.visit::text AS visit,
    tr.visitdy::numeric AS visitdy,
    tr.taetord::numeric AS taetord,
    tr.epoch::text AS epoch,
    tr.trdtc::text AS trdtc,
    tr.trdy::numeric AS trdy
    /*KEY , (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid || '~' || tr.trtestcd || '~' || tr.trevalid || '~' || tr.visitnum || '~' || tr.trlnkid || '~' || tr.trlnkgrp)::text  AS objectuniquekey  KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tr_data tr JOIN included_subjects s ON (tr.studyid = s.studyid AND tr.siteid = s.siteid AND tr.usubjid = s.usubjid)
;


