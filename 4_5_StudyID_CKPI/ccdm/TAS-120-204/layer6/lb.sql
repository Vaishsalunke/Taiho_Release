/*
CCDM LB mapping
Notes: Standard mapping to CCDM LB table
*/



WITH included_subjects AS (SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     included_site AS (SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),  

     ds_en as ( SELECT distinct studyid,siteid,usubjid,dsstdtc FROM ds WHERE dsterm = 'Enrolled' ),

    lb_data AS
    (
        select distinct
            lb.studyid,
            lb.siteid,
            lb.usubjid,
            lb.visit,
            lb.lbdtc,
            extract (days from (lb.lbdtc-dsstdtc)::interval)::numeric as lbdy,
            lbseq,
            lb.lbtestcd,
            lb.lbtest,
            lb.lbcat,
            lbscat,
            lbspec,
			folderseq,
            lbmethod,
            lborres,
            lbstat,
            lbreasnd,
            lb.lbstnrlo,
            lb.lbstnrhi,
            lb.lborresu,
            lb.lbstresn,
            lb.lbstresu,
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
            c.lbtox as lbclsig,
            studyname,
            lbtm
        FROM
            (
                SELECT DISTINCT
                    lb1."project"::text    AS studyid,
                    'TAS120_204_' || split_part(lb1."SiteNumber",'_',2)::text AS siteid,
                    lb1."Subject"::text    AS usubjid,
           trim(REGEXP_REPLACE
                        (REGEXP_REPLACE
                        (REGEXP_REPLACE
                        (REGEXP_REPLACE
                        (lb1."InstanceName",'\s\([0-9][0-9]\)','')
                                       ,'\s\([0-9]\)','')
                                       ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
                                       ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) :: text AS visit,
                    CASE
                        WHEN lb1."DataPageName" LIKE '%Chemistry%'
                        THEN MAX(chem."LBDAT")
                        WHEN lb1."DataPageName" LIKE '%Hematology%'
                        THEN MAX(hem."LBDAT")
                        WHEN lb1."DataPageName" LIKE '%Coagulation%'
                        THEN MAX(coag."LBDAT")
                        WHEN lb1."DataPageName" LIKE '%Urinalysis%'
                        THEN MAX(urin."LBDAT")
                    END::TIMESTAMP without TIME zone                AS lbdtc,
                    null::numeric as lbdy,
                    lb1."DataPointId"::INTEGER                      AS lbseq,
					lb1."FolderSeq":: INTEGER						AS folderseq,
                    lb1."AnalyteName"::text                         AS lbtestcd,
                    lb1."AnalyteName"::text                         AS lbtest,
                    lb1."DataPageName"::text                        AS lbcat,
                    null::text                                      AS lbscat,
                    NULL::text                                      AS lbspec,
                    NULL::text                                      AS lbmethod,
                    lb1."AnalyteValue"::text                        AS lborres,
                    NULL::text                                      AS lbstat,
                    NULL::text                                      AS lbreasnd,
                    coalesce(lb1."StdLow",lb1."LabLow") ::numeric   AS lbstnrlo,
                    coalesce(lb1."StdHigh",lb1."LabHigh") ::numeric AS lbstnrhi,
                    lb1."LabUnits"::text                            AS lborresu,
                    coalesce(lb1."StdValue", lb1."NumericValue")::NUMERIC                         AS lbstresn,
                    lb1."StdUnits"::text                            AS lbstresu,
                    NULL::TIME without TIME zone                    AS lbtm,
                    NULL::text                                      AS lbblfl,
                    NULL::text                                      AS lbnrind,
                    lb1."LabHigh"::text                             AS lbornrhi,
                    lb1."LabLow"::text                              AS lbornrlo,
                    NULL::text                                      AS lbstresc,
                    NULL::text                                      AS lbenint,
                    NULL::text                                      AS lbevlint,
                    NULL::text                                      AS lblat,
                    NULL::NUMERIC                                   AS lblloq,
                    NULL::text                                      AS lbloc,
                    NULL::text                                      AS lbpos,
                    NULL::text                                      AS lbstint,
                    NULL::NUMERIC                                   AS lbuloq,
                    NULL::text                                      AS lbclsig,
                    'TAS120_204'::text as studyname
                FROM
                    tas120_204_lab."NormLab" lb1
                LEFT JOIN
                    tas120_204."CHEM" chem
                ON
                    (
                        lb1."project" = chem."project"
                    AND lb1."SiteNumber"= chem."SiteNumber"
                    AND lb1."Subject" = chem."Subject"
                    AND lb1."InstanceName" = chem."InstanceName")
                LEFT JOIN
                    tas120_204."COAG" coag
                ON
                    (
                        lb1."project" = coag."project"
                    AND lb1."SiteNumber" = coag."SiteNumber"
                    AND lb1."Subject" = coag."Subject"
                    AND lb1."InstanceName" = coag."InstanceName")
                LEFT JOIN
                    tas120_204."HEMA" hem
                ON
                    (
                        lb1."project" = hem."project"
                    AND lb1."SiteNumber" = hem."SiteNumber"
                    AND lb1."Subject" = hem."Subject"
                    AND lb1."InstanceName" = hem."InstanceName")
                 
                LEFT JOIN
                  tas120_204."URIN" urin
                ON
                    (
                        lb1."project" = urin."project"
                    AND lb1."SiteNumber" = urin."SiteNumber"
                    AND lb1."Subject" = urin."Subject"
                    AND lb1."InstanceName" = urin."InstanceName")
                GROUP BY
                    1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
                    31,32,33,34,35,36
                
                -- vs mapping
                UNION ALL
                SELECT
                    vs.studyid::text                      AS studyid,
                    vs.siteid::text                       AS siteid,
                    vs.usubjid::text                      AS usubjid,
                    vs.visit::text                        AS visit,
                    vs.vsdtc::TIMESTAMP without TIME zone AS lbdtc,
                    NULL::INTEGER                         AS lbdy,
                    concat(vs.vsseq,0)::INT                       AS lbseq,
					NULL:: INTEGER						AS folderseq,
                    vs.vstestcd::text                     AS lbtestcd,
                    vs.vstest::text                       AS lbtest,
                    vs.vscat::text                        AS lbcat,
                    vs.vsscat::text                       AS lbscat,
                    NULL::text                            AS lbspec,
                    NULL::text                            AS lbmethod,
                    vs.vsorres::text                      AS lborres,
                    vs.vsstat::text                       AS lbstat,
                    NULL::text                            AS lbreasnd,
                    NULL::NUMERIC                         AS lbstnrlo,
                    NULL::NUMERIC                         AS lbstnrhi,
                    vs.vsorresu::text                     AS lborresu,
                    vs.vsstresn::NUMERIC                  AS lbstresn,
                    vs.vsstresu::text                     AS lbstresu,
                    vs.vstm::TIME without TIME zone       AS lbtm,
                    vs.vsblfl::text                       AS lbblfl,
                    NULL::text                            AS lbnrind,
                    NULL::text                            AS lbornrhi,
                    NULL::text                            AS lbornrlo,
                    NULL::text                            AS lbstresc,
                    NULL::text                            AS lbenint,
                    NULL::text                            AS lbevlint,
                    NULL::text                            AS lblat,
                    NULL::NUMERIC                         AS lblloq,
                    vs.vsloc::text                        AS lbloc,
                    vs.vspos::text                        AS lbpos,
                    NULL::text                            AS lbstint,
                    NULL::NUMERIC                         AS lbuloq,
                    NULL::text                            AS lbclsig,
                    'TAS120_204'::text as studyname
                FROM
                    vs
                -- EX mapping
                UNION ALL
                select distinct
                    ex.studyid::text                        AS studyid,
                    ex.siteid::text                         AS siteid,
                    ex.usubjid::text                        AS usubjid,
                    ex.visit::text                          AS visit,
                    ex.exstdtc::TIMESTAMP without TIME zone AS lbdtc,
                    NULL::INTEGER                           AS lbdy,
                    ex.exseq::numeric         AS lbseq,
					NULL:: INTEGER						AS folderseq,
                    'EXPOSURE'::text                        AS lbtestcd,
                    'EXPOSURE'::text                        AS lbtest,
                    'EXPOSURE'::text                        AS lbcat,
                    to_json((row(ex.extrt), row('Name of Actual Treatment')))::text AS lbscat,
                    --ex.extrt::text AS lbscat,
                    NULL::text                              AS lbspec,
                    NULL::text                              AS lbmethod,
                    ex.exdose::text                         AS lborres,
                    NULL::text                              AS lbstat,
                    NULL::text                              AS lbreasnd,
                    NULL::NUMERIC                           AS lbstnrlo,
                    NULL::NUMERIC                           AS lbstnrhi,
                    ex.exdosu::text                         AS lborresu,
                    ex.exdose::NUMERIC                      AS lbstresn,
                    ex.exdosu::text                         AS lbstresu,
                    ex.exsttm::TIME without TIME zone       AS lbtm,
                    NULL::text                              AS lbblfl,
                    NULL::text                              AS lbnrind,
                    NULL::text                              AS lbornrhi,
                    NULL::text                              AS lbornrlo,
                    NULL::text                              AS lbstresc,
                    NULL::text                              AS lbenint,
                    NULL::text                              AS lbevlint,
                    NULL::text                              AS lblat,
                    NULL::NUMERIC                           AS lblloq,
                    NULL::text                              AS lbloc,
                    NULL::text                              AS lbpos,
                    NULL::text                              AS lbstint,
                    NULL::NUMERIC                           AS lbuloq,
                    NULL::text                              AS lbclsig,
                    'TAS120_204'::text as studyname
                FROM
                    ex
                --EG mapping
                UNION 
                SELECT
                    eg.studyid::text                      AS studyid,
                    eg.siteid::text                       AS siteid,
                    eg.usubjid::text                      AS usubjid,
                    eg.visit::text                        AS visit,
                    eg.egdtc::TIMESTAMP without TIME zone AS lbdtc,
                    NULL::INTEGER                         AS lbdy,
                    eg.egseq::INT                         AS lbseq,
					NULL:: INTEGER						AS folderseq,
                    eg.egtestcd::text                     AS lbtestcd,
                    eg.egtest::text                       AS lbtest,
                    eg.egcat::text                        AS lbcat,
                    eg.egscat::text                       AS lbscat,
                    NULL::text                            AS lbspec,
                    NULL::text                            AS lbmethod,
                    eg.egorres::text                      AS lborres,
                    eg.egstat::text                       AS lbstat,
                    NULL::text                            AS lbreasnd,
                    NULL::NUMERIC                         AS lbstnrlo,
                    NULL::NUMERIC                         AS lbstnrhi,
                    eg.egorresu::text                     AS lborresu,
                    eg.egstresn::NUMERIC                  AS lbstresn,
                    eg.egstresu::text                     AS lbstresu,
                    eg.egtm::TIME without TIME zone       AS lbtm,
                    eg.egblfl::text                       AS lbblfl,
                    NULL::text                            AS lbnrind,
                    NULL::text                            AS lbornrhi,
                    NULL::text                            AS lbornrlo,
                    NULL::text                            AS lbstresc,
                    NULL::text                            AS lbenint,
                    NULL::text                            AS lbevlint,
                    NULL::text                            AS lblat,
                    NULL::NUMERIC                         AS lblloq,
                    eg.egloc::text                        AS lbloc,
                    eg.egpos::text                        AS lbpos,
                    NULL::text                            AS lbstint,
                    NULL::NUMERIC                         AS lbuloq,
                    NULL::text                            AS lbclsig,
                    'TAS120_204'::text as studyname
                FROM
                    eg
                --PE mapping
                UNION 
                SELECT
                    pe.studyid::text                      AS studyid,
                    pe.siteid::text                       AS siteid,
                    pe.usubjid::text                      AS usubjid,
                    pe.visit::text                        AS visit,
                    pe.pedtc::TIMESTAMP without TIME zone AS lbdtc,
                    NULL::INTEGER                         AS lbdy,
                    pe.peseq::INT                         AS lbseq,
					NULL:: INTEGER						AS folderseq,
                    pe.petestcd::text                     AS lbtestcd,
                    pe.petest::text                       AS lbtest,
                    pe.pecat::text                        AS lbcat,
                    pe.pescat::text                       AS lbscat,
                    NULL::text                            AS lbspec,
                    NULL::text                            AS lbmethod,
                    pe.peorres::text                      AS lborres,
                    pe.pestat::text                       AS lbstat,
                    NULL::text                            AS lbreasnd,
                    NULL::NUMERIC                         AS lbstnrlo,
                    NULL::NUMERIC                         AS lbstnrhi,
                    pe.peorresu::text                     AS lborresu,
                    NULL::NUMERIC                         AS lbstresn,
                    null::text                              AS lbstresu,
                    pe.petm::TIME without TIME zone       AS lbtm,
                    NULL::text                            AS lbblfl,
                    NULL::text                            AS lbnrind,
                    NULL::text                            AS lbornrhi,
                    NULL::text                            AS lbornrlo,
                    NULL::text                            AS lbstresc,
                    NULL::text                            AS lbenint,
                    NULL::text                            AS lbevlint,
                    NULL::text                            AS lblat,
                    NULL::NUMERIC                         AS lblloq,
                    pe.peloc::text                        AS lbloc,
                    NULL::text                            AS lbpos,
                    NULL::text                            AS lbstint,
                    NULL::NUMERIC                         AS lbuloq,
                    NULL::text                            AS lbclsig,
                    'TAS120_204'::text as studyname
                FROM
                    pe )lb left join ds_en ds 
            on lb.studyid = ds.studyid
            and lb.siteid = ds.siteid
            and lb.usubjid = ds.usubjid
			left join ctable_listing.ctcae_listing c
on lb.studyid = c.studyid
and lb.usubjid = c.usubjid
and lb.lbtest = c.lbtest
and lb.lbcat = c.lbcat
and lb.visit = c.visit
and lb.lbdtc = c.lbdtc
and lb.lbstresn = c.lbstresn
        WHERE
            lb.lbdtc IS NOT NULL
    )
	
, baseline as (       
select ex.studyid,ex.siteid,ex.usubjid,visit,blfl,labtest,seq,
count(blfl) over(partition by ex.studyid,ex.siteid,ex.usubjid,labtest ) as blfl_count
from(
select studyid,siteid,usubjid,labtest,visit,max(min_lbdtc) as blfl,seq
from(
    select lb.studyid,lb.siteid,lb.usubjid,lb.lbtest as labtest,lb.visit,lb.lbdtc as min_lbdtc,folderseq as seq1,max(lb.lbseq) as seq,
rank() over (partition by lb.studyid,lb.usubjid,lb.lbtest order by lb.lbdtc desc, folderseq Desc --,max(lbseq) desc
) as rnk
from ex
left join lb_data lb on lb.studyid=ex.studyid and lb.siteid = ex.siteid and lb.usubjid=ex.usubjid
where lb.lbstresn is not null 
group by lb.studyid,lb.siteid,lb.usubjid,lb.lbtest,lb.lbdtc,lb.visit,folderseq
having lb.lbdtc <= min(exstdtc)
)ex_max
where rnk = 1
group by ex_max.studyid,ex_max.siteid,ex_max.usubjid,labtest,visit,Seq
)ex
),
   
final_lb as
        (
        select  distinct  replace(lb.studyid,'TAS120_204','TAS-120-204') as studyid,
                    lb.siteid,
                    lb.usubjid,
                    lb.visit,
                    lbdtc,
                    lbdy,
                    lbseq,
                    lbtestcd,
                    lbtest,
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
                    case when lbdtc = blfl then 'Yes' else 'No' end as lbblfl,
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
        FROM        lb_data lb
        left join   baseline on baseline.studyid = lb.studyid and lb.siteid = baseline.siteid and lb.usubjid = baseline.usubjid  
        and baseline.labtest = lb.lbtest and lb.visit = baseline.visit and lb.lbseq= baseline.seq and blfl_count = 1
        )
       

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
        null::text AS  timpnt
        /*KEY, (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid || '~' || lb.lbseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM final_lb lb
JOIN included_subjects s ON (lb.studyid = s.studyid AND lb.siteid = s.siteid AND lb.usubjid = s.usubjid)
LEFT JOIN included_site si ON (lb.studyid = si.studyid AND lb.siteid = si.siteid);




