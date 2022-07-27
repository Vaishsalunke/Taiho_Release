/*
CCDM LB mapping
Notes: Standard mapping to CCDM LB table
*/

WITH included_subjects AS (SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     included_sites AS (SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),  

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
            folderseq,
            lb.lbtestcd,
            lb.lbtest,
            lb.lbcat,
            lbscat,
            lbspec,
            lbmethod,
            lborres,
            lbstat,
            lbreasnd,
            lb.lbstnrlo,
            lb.lbstnrhi,
            lborresu,
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
            lbtm
        FROM
            (

SELECT distinct nl.project ::text AS studyid,
                        concat(nl.project,substring(nl."SiteNumber",position('_' in nl."SiteNumber")))::text AS siteid,
                        nl."Subject" ::text AS usubjid,
                        trim(REGEXP_REPLACE
                        (REGEXP_REPLACE
                        (REGEXP_REPLACE
                        (REGEXP_REPLACE
                        (nl."InstanceName",'\s\([0-9][0-9]\)','')
                                       ,'\s\([0-9]\)','')
                                       ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
                                       ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        case when nl."DataPageName" like '%Chemistry%'      then c."LBDAT"
                             when nl."DataPageName" like '%Hematology%'     then h."LBSDTC"
                             when nl."DataPageName" like '%Coagulation%'    then c2."LBDAT"
                             when nl."DataPageName" like '%Urinalysis%'     then u."URNDATE"
                             when nl."DataPageName" like '%Hemoglobin A1C%' then hc."HGBDAT"
                        end ::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        "DataPointId" ::integer AS lbseq,
						nl."FolderSeq" ::integer as folderseq,
                        "AnalyteName" ::text AS lbtestcd,
                        "AnalyteName" ::text AS lbtest,
                        nl."DataPageName" ::text AS lbcat,
                        null::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        "AnalyteValue" ::text AS lborres,
                        null::text AS lbstat,
                        null::text AS lbreasnd,
                        coalesce("StdLow","LabLow") ::numeric AS lbstnrlo,
                        coalesce("StdHigh","LabHigh") ::numeric AS lbstnrhi,
                        "LabUnits" ::text AS lborresu,
                        coalesce("StdValue", "NumericValue") ::numeric AS  lbstresn,
                        "StdUnits" ::text AS  lbstresu,
                        null::time without time zone AS lbtm,
                        null::text AS  lbblfl,
                        null::text AS  lbnrind,
                        "LabHigh" ::text AS  lbornrhi,
                        "LabLow" ::text AS  lbornrlo,
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
                       from tas117_201_lab."NormLab" nl 
                        left join tas117_201."HEMA" h on (nl.project = h.project and nl."Subject" = h."Subject" and nl."SiteNumber" = h."SiteNumber" and nl."InstanceName"=h."InstanceName")
                        left join tas117_201."CHEM" c on (nl.project = c.project and nl."Subject" = c."Subject" and nl."SiteNumber" = c."SiteNumber" and nl."InstanceName"=c."InstanceName")
                        left join tas117_201."COAG" c2 on (nl.project = c2.project and nl."Subject" = c2."Subject" and nl."SiteNumber" = c2."SiteNumber" and nl."InstanceName"=c2."InstanceName")
                        left join tas117_201."URN" u on (nl.project = u.project and nl."Subject" = u."Subject" and nl."SiteNumber" = u."SiteNumber" and nl."InstanceName"=u."InstanceName")
                        left join tas117_201."HGBA1C" hc on  (nl.project = hc.project and nl."Subject" = hc."Subject" and nl."SiteNumber" = hc."SiteNumber" and nl."InstanceName"=hc."InstanceName")
                        
 -- vs mapping
                UNION ALL
                SELECT
                    vs.studyid::text                      AS studyid,
                    vs.siteid::text                       AS siteid,
                    vs.usubjid::text                      AS usubjid,
                    vs.visit::text                        AS visit,
                    vs.vsdtc::TIMESTAMP without TIME zone AS lbdtc,
                    NULL::INTEGER                         AS lbdy,
                    concat(vs.vsseq,0)::INT                         AS lbseq,
					null ::integer as folderseq,
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
                    NULL::text                            AS lbclsig
                FROM
                    vs
                -- EX mapping
                UNION ALL
                SELECT
                    ex.studyid::text                        AS studyid,
                    ex.siteid::text                         AS siteid,
                    ex.usubjid::text                        AS usubjid,
                    ex.visit::text                          AS visit,
                    ex.exstdtc::TIMESTAMP without TIME zone AS lbdtc,
                    NULL::INTEGER                           AS lbdy,
                    ex.exseq::numeric         AS lbseq,
					null ::integer as folderseq,
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
                    NULL::text                              AS lbclsig
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
					null ::integer as folderseq,
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
                    NULL::text                            AS lbclsig
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
					null ::integer as folderseq,
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
                    NULL::text                            AS lbclsig
                FROM
                    pe ) lb 
					left join ds_en ds 
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
            WHERE   lb.lbdtc IS NOT NULL
        )
		
		, baseline as (       
select ex.studyid,ex.siteid,ex.usubjid,visit,blfl,labtest,seq,count(blfl) over(partition by ex.studyid,ex.siteid,ex.usubjid,labtest ) as blfl_count
from(
select studyid,siteid,usubjid,labtest,visit,max(min_lbdtc) as blfl,seq
from(
    select lb.studyid,lb.siteid,lb.usubjid,lb.lbtest as labtest,lb.visit,lb.lbdtc as min_lbdtc,folderseq as seq1,max(lb.lbseq) as seq,
rank() over (partition by lb.studyid,lb.usubjid,lb.lbtest order by lb.lbdtc desc, folderseq Desc --,max(lbseq) desc
) as rnk
from ex
left join lb_data lb on lb.studyid=ex.studyid and lb.siteid = ex.siteid and lb.usubjid=ex.usubjid
--left join normlab nl on nl.studyid=ex.studyid and nl.siteid = ex.siteid and nl.usubjid=ex.usubjid and lb.lbseq = nl.lbseq
where lb.lbstresn is not null --and   lb.usubjid = '305-001' and lb.lbtest='ALB'
group by lb.studyid,lb.siteid,lb.usubjid,lb.lbtest,lb.lbdtc,lb.visit,folderseq
having lb.lbdtc <= min(exstdtc)
)ex_max
where rnk = 1
group by ex_max.studyid,ex_max.siteid,ex_max.usubjid,labtest,visit,Seq
)ex
),
   
final_lb as
        (
        select  distinct  lb.studyid,
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
LEFT JOIN included_sites si ON (lb.studyid = si.studyid AND lb.siteid = si.siteid);



