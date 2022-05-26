/*
CCDM LB mapping
Notes: Standard mapping to CCDM LB table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
                
   ds_enrol AS
   (
   SELECT studyid,siteid,usubjid,dsstdtc FROM ds WHERE dsterm = 'Enrolled'
   ),

     lb_data AS (
	 SELECT 
                lb.studyid,
                lb.siteid,
                lb.usubjid,
                trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(visit,'\s\([0-9][0-9]\)','')
								   ,'\s\([0-9]\)','')
								   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))::text as visit,
                lbdtc,
                extract (days from (lbdtc-dsstdtc)::interval)::numeric as lbdy,
                (row_number() over (partition by lb.studyid, lb.siteid, lb.usubjid order by lb.lbtestcd, lb.lbdtc))::int as lbseq,
                lbtestcd as lbtestcd,
                lbtest as lbtest,
                lbcat,
                lbscat,
                lbspec,
                lbmethod,
                lborres,
                lbstat,
                lbreasnd,
                lbstnrlo,
                lbstnrhi,
                lborresu,
                lbstresn,
                lbstresu,
                lbblfl,
                lbnrind,
                lbornrhi,
                lbornrlo,
                lbstresc,
                lbenint,
                lbevlint,
                lblat,
                lblloq,
                lbloc,
                lbpos,
                lbstint,
                lbuloq,
                lbclsig,
                lbtm 
				,null::text as timpnt
from (
               SELECT  distinct 
                        lb1."project"::text AS studyid,
                        lb1."SiteNumber"::text AS siteid, 
                        lb1."Subject"::text    AS usubjid,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(lb1."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
                        CASE
                                WHEN lb1."DataPageName" like '%Chemistry%' THEN max(chem."LBDAT")
                                WHEN lb1."DataPageName" like '%Hematology%' THEN max(hem."LBDAT")
                                WHEN lb1."DataPageName" like '%Coagulation%' THEN max(coag."LBDAT")
                        END::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        lb1."DataPointId"::integer AS lbseq,
                        lb1."AnalyteName"::text AS lbtestcd,
                        lb1."AnalyteName"::text AS lbtest,
                        lb1."DataPageName"::text AS lbcat,
                        lb1."DataPageName"::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        lb1."AnalyteValue"::text AS lborres,
                        null::text AS lbstat,
                        null::text AS lbreasnd,
                        coalesce(lb1."StdLow",lb1."LabLow") ::numeric AS lbstnrlo,
                        coalesce(lb1."StdHigh",lb1."LabHigh") ::numeric AS lbstnrhi,
                        lb1."LabUnits"::text AS lborresu,
                        coalesce(lb1."StdValue", lb1."NumericValue")::numeric AS  lbstresn,
                        lb1."StdUnits"::text AS  lbstresu,
                        null::time without time zone AS lbtm, 
                        null::text AS  lbblfl,
                        null::text AS  lbnrind,
                        lb1."LabHigh"::text AS  lbornrhi,
                        lb1."LabLow"::text AS  lbornrlo,
                        null::text AS  lbstresc,
                        null::text AS  lbenint,
                        null::text AS  lbevlint,
                        null::text AS  lblat,
                        null::numeric AS  lblloq,
                        null::text AS  lbloc,
                        null::text AS  lbpos,
                        null::text AS  lbstint,
                        null::numeric AS  lbuloq,
                        null::text AS  lbclsig
		From        tas120_201_lab."NormLab" lb1
			LEFT JOIN tas120_201."CHEM" chem on (lb1."project" = chem."project" AND lb1."SiteNumber"= chem."SiteNumber" AND lb1."Subject" = chem."Subject" AND lb1."InstanceName" = chem."InstanceName")
			LEFT JOIN tas120_201."COAG" coag on (lb1."project" = coag."project" AND lb1."SiteNumber" = coag."SiteNumber" AND lb1."Subject" = coag."Subject" AND lb1."InstanceName" = coag."InstanceName")
			LEFT JOIN tas120_201."HEMA" hem on (lb1."project" = hem."project" AND lb1."SiteNumber" = hem."SiteNumber" AND lb1."Subject" = hem."Subject" AND lb1."InstanceName" = hem."InstanceName")
			 group by 1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
		--	)a
			
			  -- vs mapping
                UNION ALL
                SELECT          
                        vs.studyid::text AS studyid,
                        vs.siteid::text AS siteid,
                        vs.usubjid::text AS usubjid,
                        vs.visit::text AS visit,
                        vs.vsdtc::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        vs.vsseq::int AS lbseq, 
                        vs.vstestcd::text AS lbtestcd,
                        vs.vstest::text AS lbtest,
                        vs.vscat::text AS lbcat,
                        vs.vsscat::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        vs.vsorres::text AS lborres,
                        vs.vsstat::text AS lbstat,
                        null::text AS lbreasnd,
                        null::numeric AS lbstnrlo,
                        null::numeric AS lbstnrhi,
                        vs.vsorresu::text AS lborresu,
                        vs.vsstresn::numeric AS lbstresn,
                        vs.vsstresu::text AS lbstresu,
                        vs.vstm::time without time zone AS lbtm,
                        vs.vsblfl::text AS lbblfl,
                        null::text AS  lbnrind,
                        null::text AS  lbornrhi,
                        null::text AS  lbornrlo,
                        null::text AS  lbstresc,
                        null::text AS  lbenint,
                        null::text AS  lbevlint,
                        null::text AS  lblat,
                        null::numeric AS  lblloq,
                        vs.vsloc::text AS lbloc,
                        vs.vspos::text AS lbpos,
                        null::text AS  lbstint,
                        null::numeric AS  lbuloq,
                        null::text AS  lbclsig
                FROM vs
        
                 -- EX mapping
                UNION ALL

                SELECT 
                                ex.studyid::text AS studyid,
                                ex.siteid::text AS siteid,
                                ex.usubjid::text AS usubjid,
                                ex.visit::text AS visit,
                                ex.exstdtc::timestamp without time zone AS lbdtc,
                                null::integer AS lbdy,
                                ex.exseq::int AS lbseq,
                                'EXPOSURE'::text AS lbtestcd,
                                'EXPOSURE'::text AS lbtest,
                                'EXPOSURE'::text AS lbcat,
                                to_json((row(ex.extrt), row('Name of Actual Treatment')))::text AS lbscat,
								--ex.extrt::text AS lbscat,
                                null::text AS lbspec,
                                null::text AS lbmethod,
                                ex.exdose::text AS  lborres,
                                null::text AS lbstat,
                                null::text AS lbreasnd,
                                null::numeric AS lbstnrlo,
                                null::numeric AS lbstnrhi,
                                ex.exdosu::text AS lborresu,
                                ex.exdose::numeric AS  lbstresn,
                                ex.exdosu::text AS lbstresu,
                                ex.exsttm::time without time zone AS lbtm,
                                null::text AS  lbblfl,
                                null::text AS  lbnrind,
                                null::text AS  lbornrhi,
                                null::text AS  lbornrlo,
                                null::text AS  lbstresc,
                                null::text AS  lbenint,
                                null::text AS  lbevlint,
                                null::text AS  lblat,
                                null::numeric AS  lblloq,
                                null::text AS  lbloc,
                                null::text AS  lbpos,
                                null::text AS  lbstint,
                                null::numeric AS  lbuloq,
                                null::text AS  lbclsig
                        from ex	
						
                        --EG mapping
                        UNION ALL
                        Select
                                eg.studyid::text       AS studyid, 
                                eg.siteid::text        AS siteid, 
                                eg.usubjid::text       AS usubjid,
                                eg.visit::text         AS visit, 
                                eg.egdtc::timestamp without time zone              AS lbdtc,
                                null::integer AS lbdy,
                                eg.egseq::int          AS lbseq, 
                                eg.egtestcd::text      AS lbtestcd, 
                                eg.egtest::text        AS lbtest, 
                                eg.egcat::text         AS lbcat, 
                                eg.egscat::text        AS lbscat,
                                null::text AS lbspec,
                                null::text AS lbmethod,										
                                eg.egorres::text       AS lborres,
                                eg.egstat::text        AS lbstat,
                                null::text AS lbreasnd,
                                null::numeric AS lbstnrlo,
                                null::numeric AS lbstnrhi,					
                                eg.egorresu::text      AS lborresu, 
                                eg.egstresn::numeric   AS lbstresn, 
                                eg.egstresu::text      AS lbstresu, 
                                eg.egtm::time without time zone AS lbtm,
                                eg.egblfl::text		 AS lbblfl,
                                null::text AS  lbnrind,
                                null::text AS  lbornrhi,
                                null::text AS  lbornrlo,
                                null::text AS  lbstresc,
                                null::text AS  lbenint,
                                null::text AS  lbevlint,
                                null::text AS  lblat,
                                null::numeric AS  lblloq,
                                eg.egloc::text AS lbloc, 			
                                eg.egpos::text         AS lbpos, 
                                null::text AS  lbstint,
                                null::numeric AS  lbuloq,
                                null::text AS  lbclsig
                        from eg
                        
                --PE mapping
                UNION ALL
                Select
                        pe.studyid::text AS studyid,
                        pe.siteid::text AS siteid,
                        pe.usubjid::text AS usubjid,
                        pe.visit::text AS visit,
                        pe.pedtc::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        pe.peseq::int AS lbseq,
                        pe.petestcd::text AS lbtestcd,
                        pe.petest::text AS lbtest,
                        pe.pecat::text AS lbcat,
                        pe.pescat::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        pe.peorres::text AS lborres,
                        pe.pestat::text AS lbstat,
                        null::text AS lbreasnd,
                        null::numeric AS lbstnrlo,
                        null::numeric AS lbstnrhi,
                        pe.peorresu::text AS lborresu,
                        null::numeric   AS lbstresn, 
                        ''::text      AS lbstresu,
                        pe.petm::time without time zone AS lbtm,
                        null::text		 AS lbblfl,
                        null::text AS  lbnrind,
                        null::text AS  lbornrhi,
                        null::text AS  lbornrlo,
                        null::text AS  lbstresc,
                        null::text AS  lbenint,
                        null::text AS  lbevlint,
                        null::text AS  lblat,
                        null::numeric AS  lblloq,
                        pe.peloc::text AS lbloc,
                        null::text         AS lbpos, 
                        null::text AS  lbstint,
                        null::numeric AS  lbuloq,
                        null::text AS  lbclsig
                from pe	
        
			)lb	left join ds_enrol ds 
			on lb.studyid = ds.studyid
			and lb.siteid = ds.siteid
			and lb.usubjid = ds.usubjid
			where lbdtc is not null
						),
						
			included_sites AS (
                  SELECT DISTINCT studyid,studyname,siteid,sitecountry,sitecountrycode,sitename,siteregion FROM site)			

SELECT 
        /*KEY (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid)::text AS comprehendid, KEY*/
        lb.studyid::text AS studyid,
        null::text AS studyname,
        lb.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.siteregion::text AS siteregion,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        lb.usubjid::text AS usubjid,
        lb.visit::text AS visit,
        lb.lbdtc::timestamp without time zone AS lbdtc,
        lb.lbdy::integer AS lbdy,
        lb.lbseq::integer AS lbseq,
        lb.lbtestcd::text AS lbtestcd,
        lb.lbtest::text AS lbtest,
        lb.lbcat::text AS lbcat,
        lb.lbscat::text AS lbscat,
        lb.lbspec::text AS lbspec,
        lb.lbmethod::text AS lbmethod,
        lb.lborres::text AS lborres,
        lb.lbstat::text AS lbstat,
        lb.lbreasnd::text AS lbreasnd,
        lb.lbstnrlo::numeric AS lbstnrlo,
        lb.lbstnrhi::numeric AS lbstnrhi,
        lb.lborresu::text AS lborresu,
        lb.lbstresn::numeric AS  lbstresn,
        lb.lbstresu::text AS  lbstresu,
        lb.lbtm::time without time zone AS lbtm,
        lb.lbblfl::text AS  lbblfl,
        lb.lbnrind::text AS  lbnrind,
        lb.lbornrhi::text AS  lbornrhi,
        lb.lbornrlo::text AS  lbornrlo,
        lb.lbstresc::text AS  lbstresc,
        lb.lbenint::text AS  lbenint,
        lb.lbevlint::text AS  lbevlint,
        lb.lblat::text AS  lblat,
        lb.lblloq::numeric AS  lblloq,
        lb.lbloc::text AS  lbloc,
        lb.lbpos::text AS  lbpos,
        lb.lbstint::text AS  lbstint,
        lb.lbuloq::numeric AS  lbuloq,
        lb.lbclsig::text AS  lbclsig,
        lb.timpnt::text AS  timpnt
        /*KEY , (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid || '~' || lb.lbseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM lb_data lb
JOIN included_subjects s ON (lb.studyid = s.studyid AND lb.siteid = s.siteid AND lb.usubjid = s.usubjid)
LEFT JOIN included_sites si ON (lb.studyid = si.studyid AND lb.siteid = si.siteid);



