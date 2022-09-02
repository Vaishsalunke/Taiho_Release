/*
CCDM TR Table mapping
Notes: Standard mapping to CCDM TR table
*/


WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
		


	/*ex_data as (
				 select studyid,siteid,usubjid,visit,exstdtc ex_dt
				 from ex
				 where visit like '%Cycle 01' and exdose is not null
							 			 
				)*/
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
				
    tr_data AS (
        select distinct tr.comprehendid,
						replace (tr.studyid,'TAS117_201','TAS117-201') as studyid,
						tr.siteid,
						tr.usubjid,
						row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc) as trseq,
						tr.trgrpid,
						tr.trrefid,
						tr.trspid,
						--COALESCE(trlnkid,row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc)::text)::text as 
						tr.trlnkid,
						--coalesce (tr.trlnkgrp,((ROW_NUMBER() OVER (PARTITION BY tr.studyid, tr.siteid, tr.usubjid ORDER BY trdtc))+1)::text)::numeric as 
						tr.trlnkgrp,
						tr.trtestcd,
						tr.trtest,
						tr.trorres,
						tr.trorresu,
						tr.trstresc,
						tr.trstresn,
						tr.trstresu,
						tr.trstat,
						tr.trreasnd,
						tr.trnam,
						tr.trmethod,
						--case when tr.trdtc::date <= ex.ex_mindt then 'Y' else 'N' end::text AS 
						tr.trlobxfl,
						tr.trblfl,
						tr.treval,
						--concat(tr.trevalid,row_number() over(partition by tr.studyid, tr.siteid,tr.usubjid order by trdtc))::text as 
						trevalid,--done
						tr.tracptfl,
						--row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc) as visitnum,
						coalesce (tr.visitnum,0) as visitnum,
						tr.visit,
						tr.visitdy,
						tr.taetord,
						dm.arm::text as epoch,
						trdtc::date,
						(tr.trdtc::date-svv.svstdtc::date)::numeric AS trdy

		from
				(
				
					SELECT	distinct	null::text AS comprehendid,
							project::text AS studyid,
							concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
							"Subject"::text AS usubjid,
							null::numeric AS trseq,
							'NEW LESION'::text AS trgrpid,--done
							null::text AS trrefid,
							null::text AS trspid,
							'NL'||nl."TUNUM2":: text :: text AS trlnkid,--done
							(row_number() over (partition by project, concat(project,'_',split_part("SiteNumber",'_',2)), "Subject")::numeric+1) ::text AS trlnkgrp,--done
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
							'N'::text AS trlobxfl,
							'N'::text AS trblfl,
							'Independent Assessor'::text AS treval,
							'Investigator'::text AS trevalid,
							null::text AS tracptfl,
							coalesce(sv.visitnum,0) ::numeric AS visitnum,
							nl."FolderName"::text AS visit,
							null::numeric AS visitdy,
							null::numeric AS taetord,
							nl."NLDAT"::text AS trdtc
					from 	tas117_201."NL" nl 
					left join sv on nl."FolderName" = sv.visit and 'TAS117-201' = sv.studyid and concat(project,'_',split_part("SiteNumber",'_',2)) = sv.siteid and "Subject" = sv.usubjid
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'NON-TARGET LESION'::text AS trgrpid,--done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'NTL'||ntlb."TUNUM1"::text AS trlnkid,--done
                	'1' ::text AS trlnkgrp,--done
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
	                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS trmethod,
	                'Y'::text AS trlobxfl,
	                'Y'::text AS trblfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	coalesce(sv.visitnum,0) ::numeric AS visitnum,
	                ntlb."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	ntlb."TUSTDT" ::text AS trdtc
                	from tas117_201."NTLBASE" ntlb
                	left join sv on ntlb."FolderName" = sv.visit and 'TAS117-201' = sv.studyid and concat(project,'_',split_part("SiteNumber",'_',2)) = sv.siteid and "Subject" = sv.usubjid
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'NON-TARGET LESION'::text AS trgrpid,--done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'NTL'||ntl."TUNUM1"::text AS trlnkid,--done
                	(row_number() over (partition by project, concat(project,'_',split_part("SiteNumber",'_',2)), "Subject")::numeric+1) ::text AS trlnkgrp,--done
                	'TUMSTATE'::text AS trtestcd,
                	'Tumor State'::text AS trtest,
                	ntl."STATUS"::text AS trorres,--done
	                'mm'::text AS trorresu,
	                ntl."STATUS"::text AS trstresc,--done
	                1::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                case 
	                	when ntl."NA" = 1 then 'Not Done'
	                	else null
	                end
	                ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS trmethod,
	                'N'::text AS trlobxfl,
	                'N'::text AS trblfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	coalesce(sv.visitnum,0) ::numeric AS visitnum,
	                ntl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	ntl."TUSTDT" ::text AS trdtc
                	from tas117_201."NTLPB" ntl
                	left join sv on ntl."FolderName" = sv.visit and 'TAS117-201' = sv.studyid and concat(project,'_',split_part("SiteNumber",'_',2)) = sv.siteid and "Subject" = sv.usubjid
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'TARGET LESION'::text AS trgrpid,--done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'TL'||tl."TUNUM"::text AS trlnkid,--done
                	'1'::text AS trlnkgrp,--done
                	'LDIAM'::text AS trtestcd,
                	'Longest Diameter'::text AS trtest,
                	tl."MEASURMT" ::text AS trorres,
	                'mm'::text AS trorresu,
	                tl."MEASURMT" ::text AS trstresc,
	                tl."MEASURMT" ::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                case when
	                tl."MEASURMT" is null then 'NOT DONE'
	                else null
	                end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("TUMETH1")='other' then "EVALOTHSP" else "TUMETH1" end::text AS trmethod,
	                'N'::text AS trlobxfl,
	                'N'::text AS trblfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	coalesce(sv.visitnum,0) ::numeric AS visitnum,
	                tl."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	tl."TUSTDT" ::text AS trdtc
                	from tas117_201."TLRN2" tl
                	left join sv on TL."FolderName" = sv.visit and 'TAS117-201' = sv.studyid and concat(project,'_',split_part("SiteNumber",'_',2)) = sv.siteid and "Subject" = sv.usubjid
                	
                	union all
                	
                	SELECT  	distinct	null::text AS comprehendid,
					project::text AS studyid,
					concat(project,'_',split_part("SiteNumber",'_',2))::text AS siteid,
					"Subject"::text AS usubjid,
					null::numeric AS trseq,
					'TARGET LESION'::text AS trgrpid,--done
                	null::text AS trrefid,
                	null::text AS trspid,
                	'TL'||tlb."TUNUM"::text AS trlnkid,--done
                	(row_number() over (partition by project, concat(project,'_',split_part("SiteNumber",'_',2)), "Subject")::numeric+1) ::text AS trlnkgrp,--done
                	'LDIAM'::text AS trtestcd,
                	'Longest Diameter'::text AS trtest,
                	tlb."MEASURMT" ::text AS trorres,
	                'mm'::text AS trorresu,
	                tlb."MEASURMT" ::text AS trstresc,
	                tlb."MEASURMT" ::numeric  AS trstresn,
	                'mm'::text AS trstresu,
	                case when
	                tlb."NA" = 1 then 'NOT DONE'
	                when tlb."TTSMALL" = 1 then 'Too Small to Measure'
	                else null
	                end ::text AS trstat,
	                null ::text AS trreasnd,
	                null::text AS trnam,
	                case when lower("TUMETH4")='other' then "EVALOTHSP" else "TUMETH4" end::text AS trmethod,
	                'Y'::text AS trlobxfl,
	                'Y'::text AS trblfl,
	                'Independent Assessor'::text AS treval,
                	'Investigator'::text AS trevalid,
                	null::text AS tracptfl,
                	coalesce(sv.visitnum,0) ::numeric AS visitnum,
	                tlb."FolderName"::text AS visit,
	                null::numeric AS visitdy,
	                null::numeric AS taetord,
                	tlb."TUSTDT" ::text AS trdtc
                	from tas117_201."TLPB" tlb
                	left join sv on TLB."FolderName" = sv.visit and 'TAS117-201' = sv.studyid and concat(project,'_',split_part("SiteNumber",'_',2)) = sv.siteid and "Subject" = sv.usubjid
				
				) tr 
		
		left join dm 
		on tr.studyid=dm.studyid and tr.siteid=dm.siteid and tr.usubjid=dm.usubjid
		left join ex_data ex 
		on tr.studyid=ex.studyid and tr.siteid=ex.siteid and tr.usubjid=ex.usubjid
		left join sv_visit svv
			on tr.studyid=svv.studyid and tr.siteid=svv.siteid and tr.usubjid=svv.usubjid
		--left join sv on tr.studyid = sv.studyid and sv.siteid = tr.siteid and sv.usubjid = tr.usubjid 
		where tr.trdtc is not null--done
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
FROM tr_data tr JOIN included_subjects s ON (tr.studyid = s.studyid AND tr.siteid = s.siteid AND tr.usubjid = s.usubjid);



