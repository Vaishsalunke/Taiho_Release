/*
CCDM TR Table mapping
Notes: Standard mapping to CCDM TR table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
		


	sv_visit as (
				 select studyid,siteid,usubjid,visit,svstdtc
				 from sv
				 where visit = 'Week 1 Day 1 Cycle 01' 
				 --or visit like '%Day 01 Cycle 01'
				 --or visit like 'Cycle 01'
				 ),	
				
    tr_data AS (
        select distinct u.comprehendid,
						u.studyid,
						u.siteid,
						u.usubjid,
						row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc) as trseq,
						trgrpid,
						trrefid,
						trspid,
						--coalesce (concat(trgrpid ,' - ', trlnkgrp), (row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc))::text) as 
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
						trlobxfl,
						trblfl,
						treval,
						--concat(u.trevalid,row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc))::text as 
						trevalid,
						tracptfl,
						--row_number() over(partition by u.studyid, u.siteid,u.usubjid order by trdtc) as 
						coalesce(sv.visitnum,0) as visitnum,
						u.visit,
						u.visitdy,
						u.taetord,
						dm.arm::text as epoch,
						trdtc,
						(u.trdtc::date-svv.svstdtc::date)::numeric AS trdy

		from
				(
					SELECT  			
									null::text AS comprehendid,
									'TAS3681_101_DOSE_EXP'::text AS studyid,
									"SiteNumber"::text AS siteid,
									"Subject"::text AS usubjid,
									null::numeric AS trseq,
									'NEW LESION' ::text AS trgrpid,
									null::text AS trrefid,
									null::text AS trspid,
									'NL' || "RecordPosition"::text AS trlnkid,
									(row_number() over (partition by 'TAS3681_101_DOSE_EXP', "SiteNumber", "Subject")::numeric + 1)::text AS trlnkgrp,
									'TUMSTATE'::text AS trtestcd,---------------1
									'Tumor State'::text AS trtest,
									'Present'::text AS trorres,
									'mm'::text AS trorresu,
									'Present' ::text AS trstresc,---------------2
									null::numeric AS trstresn,
									'mm'::text AS trstresu,
									null::text AS trstat,--------------------3
									null::text AS trreasnd,
									null::text AS trnam,
									"NLMETH"::text AS trmethod,--------------------------to be mapped in outer query
									'N'::text AS trlobxfl,
									'N'::text AS trblfl,
									'Independent Assessor'::text AS treval,
									'Investigator'::text AS trevalid,
									null::text AS tracptfl,
									null::numeric AS visitnum,-----------------------to be mapped in outer query
									--REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),'<WK[0-9]D[0-9][0-9]/>',''),'<WK[0-9]DA[0-9][0-9]/>','')::text AS visit,
									"FolderName"::text AS visit,
									null::numeric AS visitdy,
									null::numeric AS taetord,
									null::text AS epoch,------------------------------to be mapped in outer query
									"NLIMGDAT"::text AS trdtc,
									null::numeric AS trdy----------------------------to be mapped in outer query

									--,trtestcd_1
					
					From 		tas3681_101."NL" nl
					
					union all 
					
					SELECT  			
									null::text AS comprehendid,
									'TAS3681_101_DOSE_EXP'::text AS studyid,
									"SiteNumber"::text AS siteid,
									"Subject"::text AS usubjid,
									null::numeric AS trseq,
									'NON-TARGET LESION' ::text AS trgrpid,
									null::text AS trrefid,
									null::text AS trspid,
									'NTL' || "RecordPosition"::text AS trlnkid,
									'1'::text AS trlnkgrp,
									'TUMSTATE'::text AS trtestcd,---------------1
									'Tumor State'::text AS trtest,
									'Present'::text AS trorres,
									'mm'::text AS trorresu,
									'Present' ::text AS trstresc,---------------2
									null::numeric AS trstresn,
									'mm'::text AS trstresu,
									case
										when lower("NTLBYN") = 'yes' then NULL
										else 'Not Done'
									end
									::text AS trstat,--------------------3
									null::text AS trreasnd,
									null::text AS trnam,
									"NTLBMETH"::text AS trmethod,--------------------------to be mapped in outer query
									'Y'::text AS trlobxfl,
									'Y'::text AS trblfl,
									'Independent Assessor'::text AS treval,
									'Investigator'::text AS trevalid,
									null::text AS tracptfl,
									null::numeric AS visitnum,-----------------------to be mapped in outer query
									--REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),'<WK[0-9]D[0-9][0-9]/>',''),'<WK[0-9]DA[0-9][0-9]/>','')::text AS visit,
									"FolderName"::text AS visit,
									null::numeric AS visitdy,
									null::numeric AS taetord,
									null::text AS epoch,------------------------------to be mapped in outer query
									"NTLBDAT"::text AS trdtc,
									null::numeric AS trdy----------------------------to be mapped in outer query

									--,trtestcd_1
					
					From 		tas3681_101."NTLB" ntlb
				
					union all 
					
					SELECT  			
									null::text AS comprehendid,
									'TAS3681_101_DOSE_EXP'::text AS studyid,
									"SiteNumber"::text AS siteid,
									"Subject"::text AS usubjid,
									null::numeric AS trseq,
									'NON-TARGET LESION' ::text AS trgrpid,
									null::text AS trrefid,
									null::text AS trspid,
									'NTL' || "RecordPosition"::text AS trlnkid,
									(row_number() over (partition by 'TAS3681_101_DOSE_EXP', "SiteNumber", "Subject")::numeric + 1)::text AS trlnkgrp,
									'TUMSTATE'::text AS trtestcd,---------------1
									'Tumor State'::text AS trtest,
									"NTLSTA"::text AS trorres,
									'mm'::text AS trorresu,
									"NTLSTA" ::text AS trstresc,---------------2
									null::numeric AS trstresn,
									'mm'::text AS trstresu,
									case
										when lower("NTLYN") = 'yes' then NULL
										else 'Not Done'
									end
									::text AS trstat,--------------------3
									null::text AS trreasnd,
									null::text AS trnam,
									"NTLMETH"::text AS trmethod,--------------------------to be mapped in outer query
									'N'::text AS trlobxfl,
									'N'::text AS trblfl,
									'Independent Assessor'::text AS treval,
									'Investigator'::text AS trevalid,
									null::text AS tracptfl,
									null::numeric AS visitnum,-----------------------to be mapped in outer query
									--REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),'<WK[0-9]D[0-9][0-9]/>',''),'<WK[0-9]DA[0-9][0-9]/>','')::text AS visit,
									"FolderName"::text AS visit,
									null::numeric AS visitdy,
									null::numeric AS taetord,
									null::text AS epoch,------------------------------to be mapped in outer query
									"NTLDAT"::text AS trdtc,
									null::numeric AS trdy----------------------------to be mapped in outer query

									--,trtestcd_1
					
					From 		tas3681_101."NTL" ntl
				
					union all 
					
					SELECT  			
									null::text AS comprehendid,
									'TAS3681_101_DOSE_EXP'::text AS studyid,
									"SiteNumber"::text AS siteid,
									"Subject"::text AS usubjid,
									null::numeric AS trseq,
									'TARGET LESION' ::text AS trgrpid,
									null::text AS trrefid,
									null::text AS trspid,
									'TL' || "RecordPosition"::text AS trlnkid,
									'1'::text AS trlnkgrp,
									'LDIAM'::text AS trtestcd,---------------1
									'Longest Diameter'::text AS trtest,
									"TLBDIM"::text AS trorres,
									'mm'::text AS trorresu,
									"TLBDIM" ::text AS trstresc,---------------2
									null::numeric AS trstresn,
									'mm'::text AS trstresu,
									case
										when lower("TLBYN") = 'yes' then NULL
										else 'Not Done'
									end
									::text AS trstat,--------------------3
									null::text AS trreasnd,
									null::text AS trnam,
									"TLBMETH"::text AS trmethod,--------------------------to be mapped in outer query
									'Y'::text AS trlobxfl,
									'Y'::text AS trblfl,
									'Independent Assessor'::text AS treval,
									'Investigator'::text AS trevalid,
									null::text AS tracptfl,
									null::numeric AS visitnum,-----------------------to be mapped in outer query
									--REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),'<WK[0-9]D[0-9][0-9]/>',''),'<WK[0-9]DA[0-9][0-9]/>','')::text AS visit,
									"FolderName"::text AS visit,
									null::numeric AS visitdy,
									null::numeric AS taetord,
									null::text AS epoch,------------------------------to be mapped in outer query
									"TLBDAT"::text AS trdtc,
									null::numeric AS trdy----------------------------to be mapped in outer query

									--,trtestcd_1
					
					From 		tas3681_101."TLB" tlb
				
					union all 
					
					SELECT  			
									null::text AS comprehendid,
									'TAS3681_101_DOSE_EXP'::text AS studyid,
									"SiteNumber"::text AS siteid,
									"Subject"::text AS usubjid,
									null::numeric AS trseq,
									'TARGET LESION' ::text AS trgrpid,
									null::text AS trrefid,
									null::text AS trspid,
									'TL' || "RecordPosition"::text AS trlnkid,
									(row_number() over (partition by 'TAS3681_101_DOSE_EXP', "SiteNumber", "Subject")::numeric + 1)::text AS trlnkgrp,
									'LDIAM'::text AS trtestcd,---------------1
									'Longest Diameter'::text AS trtest,
									"TLDIM"::text AS trorres,
									'mm'::text AS trorresu,
									"TLDIM" ::text AS trstresc,---------------2
									null::numeric AS trstresn,
									'mm'::text AS trstresu,
									case
										when lower("TLTSTM") = 'not assessed' then 'To Small To Measure'
										else "TLYN"
									end
									::text AS trstat,--------------------3
									null::text AS trreasnd,
									null::text AS trnam,
									"TLMETH"::text AS trmethod,--------------------------to be mapped in outer query
									'N'::text AS trlobxfl,
									'N'::text AS trblfl,
									'Independent Assessor'::text AS treval,
									'Investigator'::text AS trevalid,
									null::text AS tracptfl,
									null::numeric AS visitnum,-----------------------to be mapped in outer query
									--REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''),'<WK[0-9]D[0-9][0-9]/>',''),'<WK[0-9]DA[0-9][0-9]/>','')::text AS visit,
									"FolderName"::text AS visit,
									null::numeric AS visitdy,
									null::numeric AS taetord,
									null::text AS epoch,------------------------------to be mapped in outer query
									"TLDAT"::text AS trdtc,
									null::numeric AS trdy----------------------------to be mapped in outer query

									--,trtestcd_1
					
					From 		tas3681_101."TL" tl
				
					
					) u 
		
		left join dm 
		on u.studyid=dm.studyid and u.siteid=dm.siteid and u.usubjid=dm.usubjid
					
		left join sv_visit svv
			on u.studyid=svv.studyid and u.siteid=svv.siteid and u.usubjid=svv.usubjid
		
		left join sv on u.visit = sv.visit and u.studyid = sv.studyid and u.siteid = sv.siteid and u.usubjid = sv.usubjid -- done
		where u.trdtc is not null
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
    /*KEY, (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid || '~' || tr.trtestcd  || '~' || tr.trevalid || '~' || tr.visitnum || '~' || tr.trlnkid || '~' || tr.trlnkgrp)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tr_data tr JOIN included_subjects s ON (tr.studyid = s.studyid AND tr.siteid = s.siteid AND tr.usubjid = s.usubjid)
;


