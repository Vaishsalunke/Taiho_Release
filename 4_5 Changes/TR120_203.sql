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
				 or visit like 'Cycle 01'
				 ),	
				
    tr_data AS (
        select distinct tr.comprehendid,
						tr.studyid,
						tr.siteid,
						tr.usubjid,
						row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc) as trseq,
						trgrpid,
						trrefid,
						trspid,
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
						case when tr.trdtc::date <= ex.ex_mindt then 'Y' else 'N' end::text AS trlobxfl,
						trblfl,
						treval,
						concat(tr.trevalid,row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc))::text as trevalid,
						tracptfl,
						--row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc) as 
						sv.visitnum,
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
					'New Lesion'::text AS trgrpid,
                	null::text AS trrefid,
                	null::text AS trspid,
                	nl."NLID" ::text AS trlnkid,
                	nl."RecordPosition" ::text AS trlnkgrp,
                	'TUMSTATE'::text AS trtestcd,
                	'Tumor State'::text AS trtest,
                	'Present'::text AS trorres,
	                'mm'::text AS trorresu,
	                null ::text AS trstresc,
	                1::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                null ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("NLMETH")='other' then "NLOTH" else "NLMETH" end::text AS trmethod,
	                'N'::text AS trblfl,
	                'INDEPENDENT ASSESSOR'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                nl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	nl."NLDAT"::text AS trdtc
                	from tas120_203."NL" nl
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'Non-Target Lesion'::text AS trgrpid,
                	null::text AS trrefid,
                	null::text AS trspid,
                	ntlb."NTLBID" ::text AS trlnkid,
                	ntlb."RecordPosition" ::text AS trlnkgrp,
                	'TUMSTATE'::text AS trtestcd,
                	'Tumor State'::text AS trtest,
                	'Present'::text AS trorres,
	                'mm'::text AS trorresu,
	                null ::text AS trstresc,
	                1::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                null ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("NTLBMETH")='other' then "NTLBOTH" else "NTLBMETH" end::text AS trmethod,
	                'Y'::text AS trblfl,
	                'INDEPENDENT ASSESSOR'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                ntlb."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	ntlb."NTLBDAT"::text AS trdtc
                	from tas120_203."NTLB" ntlb
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'Non-Target Lesion'::text AS trgrpid,
                	null::text AS trrefid,
                	null::text AS trspid,
                	ntl."NTLID" ::text AS trlnkid,
                	ntl."RecordPosition" ::text AS trlnkgrp,
                	'TUMSTATE'::text AS trtestcd,
                	'Tumor State'::text AS trtest,
                	'Present'::text AS trorres,
	                'mm'::text AS trorresu,
	                null ::text AS trstresc,
	                1::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                null ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("NTLMETH")='other' then "NTLOTH" else "NTLMETH" end::text AS trmethod,
	                'N'::text AS trblfl,
	                'INDEPENDENT ASSESSOR'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                ntl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	ntl."NTLDAT"::text AS trdtc
                	from tas120_203."NTL" ntl
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'Target Lesion'::text AS trgrpid,
                	null::text AS trrefid,
                	null::text AS trspid,
                	tl."TLID" ::text AS trlnkid,
                	tl."RecordPosition" ::text AS trlnkgrp,
                	'LDIAM'::text AS trtestcd,
                	'Longest Diameter'::text AS trtest,
                	tl."TLDIM"  ::text AS trorres,
	                'mm'::text AS trorresu,
	                null ::text AS trstresc,
	                tl."TLDIM"  ::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	               case when tl."TLDIM" is null then 'Not Done' else null end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("TLMETH")='other' then "TLOTH" else "TLMETH" end::text AS trmethod,
	                'N'::text AS trblfl,
	                'INDEPENDENT ASSESSOR'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                tl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	tl."TLDAT"::text AS trdtc
                	from tas120_203."TL" tl
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'Target Lesion'::text AS trgrpid,
                	null::text AS trrefid,
                	null::text AS trspid,
                	tlb."TLBID" ::text AS trlnkid,
                	tlb."RecordPosition" ::text AS trlnkgrp,
                	'LDIAM'::text AS trtestcd,
                	'Longest Diameter'::text AS trtest,
                	tlb."TLBDIM"  ::text AS trorres,
	                'mm'::text AS trorresu,
	                null ::text AS trstresc,
	                tlb."TLBDIM"  ::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                case when tlb."TLBDIM" is null then 'Not Done' else null end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("TLBMETH")='other' then "TLBOTH" else "TLBMETH" end::text AS trmethod,
	                'Y'::text AS trblfl,
	                'INDEPENDENT ASSESSOR'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	null::numeric AS visitnum,
	                tlb."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	tlb."TLBDAT"::text AS trdtc
                	from tas120_203."TLB" tlb
                	
					) tr 
		
		left join dm 
		on tr.studyid=dm.studyid and tr.siteid=dm.siteid and tr.usubjid=dm.usubjid
		left join ex_data ex 
		on tr.studyid=ex.studyid and tr.siteid=ex.siteid and tr.usubjid=ex.usubjid
		left join sv_visit svv
			on tr.studyid=svv.studyid and tr.siteid=svv.siteid and tr.usubjid=svv.usubjid
		left join sv on tr.studyid = sv.studyid and sv.siteid = tr.siteid and sv.usubjid = tr.usubjid 
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
    /*KEY , (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid || '~' || tr.trtestcd || '~' || tr.trevalid || '~' || tr.visitnum)::text  AS objectuniquekey KEY*/ 
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tr_data tr JOIN included_subjects s ON (tr.studyid = s.studyid AND tr.siteid = s.siteid AND tr.usubjid = s.usubjid);




