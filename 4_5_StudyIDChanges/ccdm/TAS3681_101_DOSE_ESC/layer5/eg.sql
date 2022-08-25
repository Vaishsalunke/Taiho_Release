

/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
 eg_data AS
(        
         SELECT   eg.studyid,
                  eg.siteid AS siteid,
 eg.usubjid  AS usubjid,
                  --(Row_number() OVER (partition BY eg.studyid, eg.siteid, eg.usubjid ORDER BY eg.egdtc))::int AS egseq,
 egseq,
                  upper(eg.egtestcd) as egtestcd,
                  eg.egtest,
                  eg.egcat,
                  eg.egscat,
                  eg.egpos,
                  eg.egorres,
                  eg.egorresu,
                  eg.egstresn,
                  eg.egstresu,
                  eg.egstat,
                  eg.egloc,
 eg.egblfl,
                  trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(eg.visit
								,'<WK[0-9]D[0-9]/>\sExpansion','')
								,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'Expansion','')
								
				    ) as visit,
                  eg.egdtc,
                  eg.egtm,
				  eg.egtimpnt,
				  eg.egstnrlo,
				  eg.egstnrhi
         FROM     (          -- TAS3681-101  ECG
         select studyid,siteid,usubjid,
         egseq,
egtestcd,
egtest,
egcat,
egscat,
egpos,
egorres,
egorresu,
egstresn,
egstresu,
egstat,
egloc,
egblfl,
visit,egdtc,egtm,
egtimpnt,
egstnrlo,
egstnrhi from (
                             SELECT     'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                        "SiteNumber":: text      AS siteid,
"Subject":: text      AS usubjid,
                                        egseq ::int        AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
egstresn::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
                                       -- REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
										trim(REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 ("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
									   ,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,' Escalation','')
									   ,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								 ) :: text as visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
"ECGTIM"::time without time zone AS egtm,
"ECGTIM"::text AS egtimpnt,
null::numeric AS egstnrlo,
null::numeric AS egstnrhi
          FROM  tas3681_101."ECG"
                             cross join lateral(
values
(concat(concat("instanceId","RecordPosition"),'12'),'ECGRR' , 'RR Interval' , "ECGRR" ,"ECGRR_Units"  ,"ECGRR" ,"ECGRR_Units"),
(concat(concat("instanceId","RecordPosition"),'23'),'ECQTCF', 'Derived QTcF Interval' , "ECQTCF" ,"ECQTCF_Units"  ,"ECQTCF" ,"ECQTCF_Units"),
(concat(concat("instanceId","RecordPosition"),'34'),'ECGHR' , 'HR' , "ECGHR"   ,"ECGHR_Units"  ,"ECGHR" ,"ECGHR_Units"),
(concat(concat("instanceId","RecordPosition"),'45'),'ECGQT' , 'QT Interval' , "ECGQT" ,"ECGQT_Units"  ,"ECGQT" ,"ECGQT_Units"),
(concat(concat("instanceId","RecordPosition"),'56'),'ECGQTC', 'QTc Interval' , "ECGQTC" ,"ECGQTC_Units"  ,"ECGQTC" ,"ECGQTC_Units")
)as t
(egseq,egtestcd,egtest, egorres, egorresu, egstresn, egstresu)
)a

UNION
-- TAS3681-101  ECG2
select studyid,siteid,usubjid,
          egseq,
egtestcd,
egtest,
egcat,
egscat,
egpos,
egorres,
egorresu,
egstresn,
egstresu,
egstat,
egloc,
egblfl,
visit,egdtc,egtm,
egtimpnt,
egstnrlo,
egstnrhi from (
                             SELECT     'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                        "SiteNumber":: text      AS siteid,
"Subject":: text      AS usubjid,
                                        egseq::int            AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
egstresn::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
                                       -- REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
									   trim(REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 ("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
									   ,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,' Escalation','')
									   ,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								 ) :: text as visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
"ECGTIM"::time without time zone AS egtm,
"ECGTM"::text AS egtimpnt,
null::numeric AS egstnrlo,
null::numeric AS egstnrhi
                             FROM  tas3681_101."ECG2"
                             cross join lateral(
values
(concat(concat("instanceId","RecordPosition"),'13'),'ECGRR' , 'RR Interval' , "ECGRR" ,"ECGRR_Units"  ,"ECGRR" ,"ECGRR_Units"),
(concat(concat("instanceId","RecordPosition"),'24'),'ECQTCF', 'Derived QTcF Interval' , "ECQTCF" ,"ECQTCF_Units"  ,"ECQTCF" ,"ECQTCF_Units"),
(concat(concat("instanceId","RecordPosition"),'35'),'ECGHR' , 'HR' , "ECGHR"   ,"ECGHR_Units"  ,"ECGHR" ,"ECGHR_Units"),
(concat(concat("instanceId","RecordPosition"),'46'),'ECGQT' , 'QT Interval' , "ECGQT" ,"ECGQT_Units"  ,"ECGQT" ,"ECGQT_Units"),
(concat(concat("instanceId","RecordPosition"),'57'),'ECGQTC', 'QTc Interval' , "ECGQTC" ,"ECGQTC_Units"  ,"ECGQTC" ,"ECGQTC_Units")
)as t
(egseq,egtestcd,egtest, egorres, egorresu, egstresn, egstresu)
)a

UNION
-- TAS3681-101  ECG3
select studyid,siteid,usubjid,
         egseq,
egtestcd,
egtest,
egcat,
egscat,
egpos,
egorres,
egorresu,
egstresn,
egstresu,
egstat,
egloc,
egblfl,
visit,egdtc,egtm,
egtimpnt,
egstnrlo,
egstnrhi from (
                             SELECT     'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                        "SiteNumber":: text      AS siteid,
"Subject":: text      AS usubjid,
                                        egseq::int            AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
egstresn::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
--REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
trim(REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 ("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
									   ,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,' Escalation','')
									   ,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								 ) :: text as visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
"ECGTIM"::time without time zone AS egtm,
"ECGTM"::text AS egtimpnt,
null::numeric AS egstnrlo,
null::numeric AS egstnrhi
                             FROM  tas3681_101."ECG3"
                             cross join lateral(
values
(concat(concat("instanceId","RecordPosition"),'14'),'ECGRR' , 'RR Interval' , "ECGRR" ,"ECGRR_Units"  ,"ECGRR" ,"ECGRR_Units"),
(concat(concat("instanceId","RecordPosition"),'25'),'ECQTCF', 'Derived QTcF Interval' , "ECQTCF" ,"ECQTCF_Units"  ,"ECQTCF" ,"ECQTCF_Units"),
(concat(concat("instanceId","RecordPosition"),'36'),'ECGHR' , 'HR' , "ECGHR"   ,"ECGHR_Units"  ,"ECGHR" ,"ECGHR_Units"),
(concat(concat("instanceId","RecordPosition"),'47'),'ECGQT' , 'QT Interval' , "ECGQT" ,"ECGQT_Units"  ,"ECGQT" ,"ECGQT_Units"),
(concat(concat("instanceId","RecordPosition"),'58'),'ECGQTC', 'QTc Interval' , "ECGQTC" ,"ECGQTC_Units"  ,"ECGQTC" ,"ECGQTC_Units")
)as t
(egseq,egtestcd,egtest, egorres, egorresu, egstresn, egstresu)
)a

UNION

-- TAS3681-101  ECG4
select studyid,siteid,usubjid,
          egseq,
egtestcd,
egtest,
egcat,
egscat,
egpos,
egorres,
egorresu,
egstresn,
egstresu,
egstat,
egloc,
egblfl,
visit,egdtc,egtm,
egtimpnt,
egstnrlo,
egstnrhi from (
                             SELECT     'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                        "SiteNumber":: text      AS siteid,
"Subject":: text      AS usubjid,
                                        egseq::int            AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
egstresn::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
                                    --    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
									trim(REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 ("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
									   ,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,' Escalation','')
									   ,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								 ) :: text as visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
"ECGTIM"::time without time zone AS egtm,
"ECGTIM"::text AS egtimpnt,
null::numeric AS egstnrlo,
null::numeric AS egstnrhi
            FROM  tas3681_101."ECG4"
                             cross join lateral(
values
(concat(concat("instanceId","RecordPosition"),'15'),'ECGRR' , 'RR Interval' , "ECGRR" ,"ECGRR_Units"  ,"ECGRR" ,"ECGRR_Units"),
--(concat(concat("instanceId","RecordPosition"),'26'),'ECQTCF', 'Derived QTcF Interval' , "ECQTCF" ,"ECQTCF_Units"  ,"ECQTCF" ,"ECQTCF_Units"),
(concat(concat("instanceId","RecordPosition"),'37'),'ECGHR' , 'HR' , "ECGHR"   ,"ECGHR_Units"  ,"ECGHR" ,"ECGHR_Units"),
(concat(concat("instanceId","RecordPosition"),'48'),'ECGQT' , 'QT Interval' , "ECGQT" ,"ECGQT_Units"  ,"ECGQT" ,"ECGQT_Units"),
   (concat(concat("instanceId","RecordPosition"),'59'),'ECGQTC', 'QTc Interval' , "ECGQTC" ,"ECGQTC_Units"  ,"ECGQTC" ,"ECGQTC_Units")
)as t
(egseq,egtestcd,egtest, egorres, egorresu, egstresn, egstresu)
)a

UNION

-- TAS3681-101  ECG5
select studyid,siteid,usubjid,
         egseq,
egtestcd,
egtest,
egcat,
egscat,
egpos,
egorres,
egorresu,
egstresn,
egstresu,
egstat,
egloc,
egblfl,
visit,egdtc,egtm,
egtimpnt,
egstnrlo,
egstnrhi from (
                             SELECT     'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                        "SiteNumber":: text      AS siteid,
"Subject":: text      AS usubjid,
                                        egseq::int            AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
egstresn::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
                                       -- REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
									   trim(REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 ("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
									   ,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,' Escalation','')
									   ,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								 ) :: text as visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
"ECGTIM"::time without time zone AS egtm,
"ECGTM"::text AS egtimpnt,
null::numeric AS egstnrlo,
null::numeric AS egstnrhi
             FROM  tas3681_101."ECG5"
                             cross join lateral(
values
(concat(concat("instanceId","RecordPosition"),'16'),'ECGRR' , 'RR Interval' , "ECGRR" ,"ECGRR_Units"  ,"ECGRR" ,"ECGRR_Units"),
--(concat(concat("instanceId","RecordPosition"),'27'),'ECQTCF', 'Derived QTcF Interval' , "ECQTCF" ,"ECQTCF_Units"  ,"ECQTCF" ,"ECQTCF_Units"),
(concat(concat("instanceId","RecordPosition"),'38'),'ECGHR' , 'HR' , "ECGHR"   ,"ECGHR_Units"  ,"ECGHR" ,"ECGHR_Units"),
(concat(concat("instanceId","RecordPosition"),'49'),'ECGQT' , 'QT Interval' , "ECGQT" ,"ECGQT_Units"  ,"ECGQT" ,"ECGQT_Units")
, (concat(concat("instanceId","RecordPosition"),'50'),'ECGQTC', 'QTc Interval' , "ECGQTC" ,"ECGQTC_Units"  ,"ECGQTC" ,"ECGQTC_Units")
  )as t
(egseq,egtestcd,egtest, egorres, egorresu, egstresn, egstresu)
)a

UNION

-- TAS3681-101  ECG6
select studyid,siteid,usubjid,
        egseq,
egtestcd,
egtest,
egcat,
egscat,
egpos,
egorres,
egorresu,
egstresn,
egstresu,
egstat,
egloc,
egblfl,
visit,egdtc,egtm,
egtimpnt,
egstnrlo,
egstnrhi from (
                             SELECT     'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                        "SiteNumber":: text      AS siteid,
"Subject":: text      AS usubjid,
                                        egseq::int            AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
egstresn::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
                                       -- REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
									   trim(REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 (REGEXP_REPLACE
								 ("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
									   ,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9]/>\sEscalation','')
									   ,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9]/>\sEscalation','')
									   ,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
									   ,' Escalation','')
									   ,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								 ) :: text as visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
                                        
                           
                                        
"ECGTIM"::time without time zone AS egtm,
"ECGTIM"::text AS egtimpnt,
null::numeric AS egstnrlo,
null::numeric AS egstnrhi
             FROM  tas3681_101."ECG6"
                             cross join lateral(
values
(concat(concat("instanceId","RecordPosition"),'17'),'ECGRR' , 'RR Interval' , "ECGRR" ,"ECGRR_Units"  ,"ECGRR" ,"ECGRR_Units"),
--(concat(concat("instanceId","RecordPosition"),'28'),'ECQTCF', 'Derived QTcF Interval' , "ECQTCF" ,"ECQTCF_Units"  ,"ECQTCF" ,"ECQTCF_Units"),
(concat(concat("instanceId","RecordPosition"),'39'),'ECGHR' , 'HR' , "ECGHR"   ,"ECGHR_Units"  ,"ECGHR" ,"ECGHR_Units"),
(concat(concat("instanceId","RecordPosition"),'40'),'ECGQT' , 'QT Interval' , "ECGQT" ,"ECGQT_Units"  ,"ECGQT" ,"ECGQT_Units")
, (concat(concat("instanceId","RecordPosition"),'51'),'ECGQTC', 'QTc Interval' , "ECGQTC" ,"ECGQTC_Units"  ,"ECGQTC" ,"ECGQTC_Units")
  )as t
(egseq,egtestcd,egtest, egorres, egorresu, egstresn, egstresu)
)a

UNION
-- ECG1_LAB
SELECT     'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                         'TAS3681101_'||left(eg1."SUBJID",3)::text AS siteid,
                    eg1."SUBJID"::text    AS usubjid,
                                        concat("EGSEQ",egseq)::int            AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
coalesce(convert_to_numeric(egstresn),0)::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
                                        eg1."VISIT"::text AS visit,
                                        eg1."EGDTC" ::timestamp without time zone AS egdtc,
case when length(replace("EGDTC",'T',' '))=10 then concat("EGDTC",' 00:00:00')
                when length(replace("EGDTC",'T',' '))=19 then replace("EGDTC",'T',' ')
                end::time without time zone AS egtm,
				"EGTPT"::text AS egtimpnt,
				null::numeric AS egstnrlo,
				null::numeric AS egstnrhi
                FROM  tas3681_101."ECG1_LAB" eg1
cross join lateral(
values
('PRAG','PR Interval',case when "EGTESTCD" = 'PRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'PRAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'PRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'PRAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'PRAG' then 8 end),
('QRSAG','QRS Duration', case when "EGTESTCD" = 'QRSAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QRSAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QRSAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QRSAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QRSAG' then 7 end),
('QTCBAG','QTcB Interval', case when "EGTESTCD" = 'QTCBAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCBAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTCBAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCBAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QTCBAG' then 6 end),
('QTCFAG','QTcF Interval', case when "EGTESTCD" = 'QTCFAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCFAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTCFAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCFAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QTCFAG' then 5 end),
('QTCRRAG','RR Interval', case when "EGTESTCD" = 'QTCRRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCRRAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTCRRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCRRAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QTCRRAG' then 4 end),
('QTAG','QT Interval', case when "EGTESTCD" = 'QTAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QTAG' then 3 end),
('EGHRMN','Mean Heart Rate', case when "EGTESTCD" = 'EGHRMN' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'EGHRMN' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'EGHRMN' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'EGHRMN' then "EGORRESU" else null end,
case when "EGTESTCD" = 'EGHRMN' then 2 end),
('INTP','Interpretation', case when "EGTESTCD" = 'INTP' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'INTP' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'INTP' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'INTP' then "EGORRESU" else null end,
case when "EGTESTCD" = 'INTP' then 1 end)
)as t
(egtestcd,egtest, egorres, egorresu, egstresn, egstresu,egseq)
where (egtestcd is not null and  
egtest is not null and
      egorres is not null and
      egorresu is not null and
      egstresn is not null and
      egstresu is not null)


UNION
-- ECG2_LAB
SELECT    'TAS3681_101_DOSE_ESC'::text   AS studyid,
                                    'TAS3681101_'||left(eg2."SUBJID",3)::text AS siteid,
                    eg2."SUBJID"::text    AS usubjid,
                                        concat("EGSEQ",egseq)::int            AS egseq,
                                        egtestcd::text     AS egtestcd,
                                        egtest::text       AS egtest,
                                        'ECG'::text AS egcat,
                                        'ECG'::text           AS egscat,
                                        NULL::text           AS egpos,
                                        egorres::text      AS egorres,
                                        egorresu::text     AS egorresu,
coalesce(convert_to_numeric(egstresn),0)::numeric AS egstresn,
egstresu::text AS egstresu,
                                        NULL::text  AS egstat,
                                        NULL::text  AS egloc,
NULL::text AS egblfl,
                                        eg2."VISIT"::text AS visit,
                                        eg2."EGDTC" ::timestamp without time zone AS egdtc,
case when length(replace("EGDTC",'T',' '))=10 then concat("EGDTC",' 00:00:00')
                     when length(replace("EGDTC",'T',' '))=19 then replace("EGDTC",'T',' ')
                end::time without time zone AS egtm,
				"EGTPT"::text AS egtimpnt,
				null::numeric AS egstnrlo,
				null::numeric AS egstnrhi
                            FROM  tas3681_101."ECG2_LAB" eg2
 cross join lateral(
values
('PRAG','PR Interval',case when "EGTESTCD" = 'PRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'PRAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'PRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'PRAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'PRAG' then 1112 end),
('QRSAG','QRS Duration', case when "EGTESTCD" = 'QRSAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QRSAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QRSAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QRSAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QRSAG' then 1212 end),
('QTCBAG','QTcB Interval', case when "EGTESTCD" = 'QTCBAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCBAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTCBAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCBAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QTCBAG' then  1312 end),
('QTCFAG','QTcF Interval', case when "EGTESTCD" = 'QTCFAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCFAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTCFAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCFAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTCFAG' then 1412 end),
('QTCRRAG','RR Interval', case when "EGTESTCD" = 'QTCRRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCRRAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTCRRAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTCRRAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QTCRRAG' then 1512 end),
('QTAG','QT Interval', case when "EGTESTCD" = 'QTAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTAG' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'QTAG' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'QTAG' then "EGORRESU" else null end,
case when "EGTESTCD" = 'QTAG' then 1612 end),
('EGHRMN','Mean Heart Rate', case when "EGTESTCD" = 'EGHRMN' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'EGHRMN' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'EGHRMN' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'EGHRMN' then "EGORRESU" else null end,
case when "EGTESTCD" = 'EGHRMN' then 1712 end),
('INTP','Interpretation', case when "EGTESTCD" = 'INTP' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'INTP' then "EGORRESU" else null end ,
case when "EGTESTCD" = 'INTP' then "EGORRES1" else null end ,
case when "EGTESTCD" = 'INTP' then "EGORRESU" else null end,
case when "EGTESTCD" = 'INTP' then 1812 end)
)as t
(egtestcd,egtest, egorres, egorresu, egstresn, egstresu,egseq)
where (egtestcd is not null and  
egtest is not null and
      egorres is not null and
      egorresu is not null and
      egstresn is not null and
      egstresu is not null)

                              ) eg )
SELECT
       /*KEY (eg.studyid::text || '~' || eg.siteid::text || '~' || eg.usubjid::text) AS comprehendid, KEY*/
       eg.studyid::text                                   AS studyid,
       eg.siteid::text                                    AS siteid,
       eg.usubjid::text                                   AS usubjid,
       eg.egseq::int                                      AS egseq,
       eg.egtestcd::text                                  AS egtestcd,
       eg.egtest::text                                    AS egtest,
       eg.egcat::text                                     AS egcat,
       eg.egscat::text                                    AS egscat,
       eg.egpos::text                                     AS egpos,
       eg.egorres::text                                   AS egorres,
       eg.egorresu::text                                  AS egorresu,
       eg.egstresn::numeric                               AS egstresn,
       eg.egstresu::text                                  AS egstresu,
       eg.egstat::text                                    AS egstat,
       eg.egloc::text                                     AS egloc,
       eg.egblfl::text  AS egblfl,
       eg.visit::text                                     AS visit,
       eg.egdtc::timestamp without time zone              AS egdtc,
       eg.egtm:: time without time zone AS egtm,
	   eg.egtimpnt::text AS egtimpnt,
	   eg.egstnrlo::numeric AS egstnrlo,
	   eg.egstnrhi::numeric AS egstnrhi
       /*KEY, (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text AS objectuniquekey KEY*/
       /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM   eg_data eg
JOIN   included_subjects s
ON     (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid)
;



