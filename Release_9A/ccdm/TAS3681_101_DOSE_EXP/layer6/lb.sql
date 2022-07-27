/*
CCDM LB mapping
Notes: Standard mapping to CCDM LB table
*/


WITH    included_subjects AS ( SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

        included_sites AS (SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),  

        ds_en as ( SELECT distinct studyid,siteid,usubjid,dsstdtc FROM ds WHERE dsterm = 'Enrolled' ),

        normlab AS
        (
        SELECT      'TAS3681_101_DOSE_EXP'::text    AS studyid,
                    lb1."SiteNumber"::text AS siteid,
                    lb1."Subject"::text    AS usubjid,
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
                        (lb1."InstanceName",'<WK[0-9]D[0-9]/>\sExpansion','')
                                        ,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
                                        ,'<WK[0-9]DA[0-9]/>\sExpansion','')
                                        ,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
                                        ,'<W[0-9]DA[0-9]/>\sExpansion','')
                                        ,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
                                        ,'Expansion','')
                                        ,'\s\([0-9][0-9]\)','')
                                        ,'\s\([0-9]\)','')
                                        ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
                                        ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
                            ):: text AS visit,
                    CASE
                        WHEN lb1."DataPageName" LIKE '%Chemistry%'
                        THEN MAX(chem."LBDAT")
                        WHEN lb1."DataPageName" LIKE '%Hematology%'
                        THEN MAX(hem."LBDAT")
                        WHEN lb1."DataPageName" LIKE '%Coagulation%'
                        THEN MAX(coag."LBDAT")
                        -- WHEN lb1."DataPageName" LIKE '%Urinalysis%'
                        --THEN MAX(urin."LBDAT")
                        WHEN lb1."DataPageName" LIKE '%PSA Blood Sampling%'
                        THEN MAX(psa."LBDAT")
                    END::TIMESTAMP without TIME zone                AS lbdtc,
                    NULL::INTEGER                                   AS lbdy,
                    lb1."DataPointId":: int   AS lbseq,
                    lb1."FolderSeq" :: int as folderseq,
                    lb1."AnalyteName"::text                         AS lbtestcd,
                    lb1."AnalyteName"::text                         AS lbtest,
                    lb1."DataPageName"::text                        AS lbcat,
                    lb1."DataPageName"::text                        AS lbscat,
                    NULL::text                                      AS lbspec,
                    NULL::text                                      AS lbmethod,
                    lb1."AnalyteValue"::text                        AS lborres,
                    NULL::text                                      AS lbstat,
                    NULL::text                                      AS lbreasnd,
                    coalesce(lb1."StdLow",lb1."LabLow") ::numeric   AS lbstnrlo,
                    coalesce(lb1."StdHigh",lb1."LabHigh") ::numeric AS lbstnrhi,
                    lb1."LabUnits"::text                            AS lborresu,
                    coalesce(lb1."StdValue", lb1."NumericValue")::NUMERIC AS lbstresn,
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
                    NULL::text                                      AS lbclsig
        FROM        tas3681_101_lab."NormLab" lb1
                    LEFT JOIN   tas3681_101."CHEM" chem
                    ON          (lb1."project" = chem."project" AND lb1."SiteNumber"= chem."SiteNumber" AND lb1."Subject" = chem."Subject"
                                AND lb1."InstanceName" = chem."InstanceName")
                    LEFT JOIN   tas3681_101."COAG" coag
                    ON          (lb1."project" = coag."project" AND lb1."SiteNumber" = coag."SiteNumber" AND lb1."Subject" = coag."Subject"
                                AND lb1."InstanceName" = coag."InstanceName")
                    LEFT JOIN   tas3681_101."HEMA" hem
                    ON          (lb1."project" = hem."project" AND lb1."SiteNumber" = hem."SiteNumber" AND lb1."Subject" = hem."Subject"
                                AND lb1."InstanceName" = hem."InstanceName")
                  /*LEFT JOIN   tas3681_101."URIN" urin
                    ON          (lb1."project" = urin."project" AND lb1."SiteNumber" = urin."SiteNumber" AND lb1."Subject" = urin."Subject"
                                AND lb1."InstanceName" = urin."InstanceName")*/
                    LEFT JOIN   tas3681_101."PSA" psa
                    ON          (lb1."project" = psa."project" AND lb1."SiteNumber" = psa."SiteNumber" AND lb1."Subject" = psa."Subject"
                                AND lb1."InstanceName" = psa."InstanceName")
                    where       lb1."DataPageName" not LIKE '%Urinalysis%'
                    GROUP BY    1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
   
        union all

        select      *
        from        (   select distinct     'TAS3681_101_DOSE_EXP'::text                      AS studyid,
                                            "SiteNumber"::text                       AS siteid,
                                            "Subject"::text                      AS usubjid,
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
                                                ("FolderName",'<WK[0-9]D[0-9]/>\sExpansion','')
                                                                ,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
                                                                ,'<WK[0-9]DA[0-9]/>\sExpansion','')
                                                                ,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
                                                                ,'<W[0-9]DA[0-9]/>\sExpansion','')
                                                                ,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
                                                                ,'Expansion','')
                                                                ,'\s\([0-9][0-9]\)','')
                                                                ,'\s\([0-9]\)','')
                                                                ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
                                                                ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
                                                    ) ::text                        AS visit,
                                            "LBDAT"::TIMESTAMP without TIME zone AS lbdtc,
                                            NULL::INTEGER                         AS lbdy,
                                            lbseq::int                         AS lbseq,
                                            null :: int folderseq,
                                            lbtestcd::text                     AS lbtestcd,
                                            lbtest::text                       AS lbtest,
                                            "DataPageName"::text                        AS lbcat,
                                            NULL::text                       AS lbscat,
                                            NULL::text                            AS lbspec,
                                            NULL::text                            AS lbmethod,
                                            lborres::text                      AS lborres,
                                            NULL::text                       AS lbstat,
                                            NULL::text                            AS lbreasnd,
                                            NULL::NUMERIC                         AS lbstnrlo,
                                            NULL::NUMERIC                         AS lbstnrhi,
                                            NULL::text                     AS lborresu,
                                            lbstresn::NUMERIC                  AS lbstresn,
                                            NULL::text                     AS lbstresu,
                                            NULL::TIME without TIME zone       AS lbtm,
                                            NULL::text                       AS lbblfl,
                                            NULL::text                            AS lbnrind,
                                            NULL::text                            AS lbornrhi,
                                            NULL::text                            AS lbornrlo,
                                            NULL::text                            AS lbstresc,
                                            NULL::text                            AS lbenint,
                                            NULL::text                            AS lbevlint,
                                            NULL::text                            AS lblat,
                                            NULL::NUMERIC                         AS lblloq,
                                            NULL::text                        AS lbloc,
                                            NULL::text                        AS lbpos,
                                            NULL::text                            AS lbstint,
                                            NULL::NUMERIC                         AS lbuloq,
                                            NULL::text                            AS lbclsig
                        FROM                tas3681_101."URIN"
                        CROSS JOIN LATERAL  (
VALUES  ('PROT','Urinary Protein',case when UPPER("PROT") = 'OTHER' then "PROTSP" end::text,concat("instanceId",14),null),
        ('PROTSP',null,"PROTSP"::text,concat("instanceId",26),null),
        ('GLUC','Glucose (Sugar)',case when UPPER("GLUC") = 'OTHER' then "GLUCSP" END::text,concat("instanceId",38),null),
        ('GLUCSP',null,"GLUCSP"::text,concat("instanceId",47),null),
        ('UBILI','Urinary Bilirubin',"UBILI"::text,concat("instanceId",54),null),
        ('UKET','Urinary Ketones',"UKET"::text,concat("instanceId",68),null),
        ('ULEK','Urinary Leukocytes',"ULEK"::text,concat("instanceId",799),null),
        ('UNITR','Urinary Nitrite',"UNITR"::text,concat("instanceId",81),null),
        ('UOCCB','Urinary Occult Blood',"UOCCB"::text,concat("instanceId",17),null),
        ('RBC','RBCs (Microscopic)',"RBC"::text,concat("instanceId",25),null),
        ('WBC','WBCs (Microscopic)',"WBC"::text,concat("instanceId",31),null),
        ('USPGRAV','Urine Specific Gravity Density',"USPGRAV"::text,concat("instanceId",41),"USPGRAV"),
        ('PH','PH',"PH"::text,concat("instanceId",599),"PH")
) t (lbtestcd,lbtest,lborres,lbseq,lbstresn)
                        where               lbtestcd is not null and lbtest is not null
)p
),
        lb_data AS
        (
            SELECT  lb.studyid,
                    lb.siteid,
                    lb.usubjid,
                    trim(REGEXP_REPLACE
                            (REGEXP_REPLACE
                            (REGEXP_REPLACE
                            (REGEXP_REPLACE
                            (REGEXP_REPLACE
                            (REGEXP_REPLACE
                            (REGEXP_REPLACE
                            (lb.visit
                                        ,'<WK[0-9]D[0-9]/>\sExpansion','')
                                        ,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
                                        ,'<WK[0-9]DA[0-9]/>\sExpansion','')
                                        ,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
                                        ,'<W[0-9]DA[0-9]/>\sExpansion','')
                                        ,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
                                        ,'Expansion','')
                                        
                            )::text AS visit,
                    lb.lbdtc,
                    extract (days from (lb.lbdtc-dsstdtc)::interval)::numeric as lbdy,
                    lbseq,
                    --(row_number() over (partition BY lb.studyid, lb.siteid, lb.usubjid ORDER BY lb.lbtestcd, lb.lbdtc))::INT AS lbseq,
                    lb.lbtestcd          AS lbtestcd,
                    lb.lbtest            AS lbtest,
                    lb.lbcat,
                    lbscat,
                    lbspec,
                    lbmethod,
                    lborres,
                    lbstat,
                    lbreasnd,
                    lb.lbstnrlo,
                    lb.lbstnrhi,
                    lb.lborresu,
                    lb.lbstresn,
                    lb.lbstresu,
                    lbtm,
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
                    c.lbtox as lbclsig
            FROM
            (
               --Normlab
                    SELECT  studyid,
                            siteid,
                            usubjid,
                            visit,
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
                            lbtm,
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
                            lbclsig
                    FROM    Normlab
               
                    UNION ALL
                     -- vs mapping
                    SELECT  vs.studyid::text                      AS studyid,
                            vs.siteid::text                       AS siteid,
                            vs.usubjid::text                      AS usubjid,
                            vs.visit::text                        AS visit,
                            vs.vsdtc::TIMESTAMP without TIME zone AS lbdtc,
                            NULL::INTEGER                         AS lbdy,
                            concat(vs.vsseq,0)::int                         AS lbseq,
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
                    FROM    vs
                
                    UNION ALL
                    
                    -- EX mapping
                    SELECT  ex.studyid::text                        AS studyid,
                            ex.siteid::text                         AS siteid,
                            ex.usubjid::text                        AS usubjid,
                            ex.visit::text                          AS visit,
                            ex.exstdtc::TIMESTAMP without TIME zone AS lbdtc,
                            NULL::INTEGER                           AS lbdy,
                            concat(ex.exseq,09)::int                           AS lbseq,
                            'EXPOSURE'::text                        AS lbtestcd,
                            'EXPOSURE'::text                        AS lbtest,
                            'EXPOSURE'::text                        AS lbcat,
                            to_json((row(ex.extrt), row('Name of Actual Treatment')))::text AS lbscat,
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
                    FROM    ex
                
                    UNION ALL
                    --EG mapping
                    SELECT  eg.studyid::text                      AS studyid,
                            eg.siteid::text                       AS siteid,
                            eg.usubjid::text                      AS usubjid,
                            eg.visit::text                        AS visit,
                            eg.egdtc::TIMESTAMP without TIME zone AS lbdtc,
                            NULL::INTEGER                         AS lbdy,
                            eg.egseq::int                         AS lbseq,
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
                    FROM    eg
                
                    UNION ALL
                    --PE mapping
                    SELECT  pe.studyid::text                      AS studyid,
                            pe.siteid::text                       AS siteid,
                            pe.usubjid::text                      AS usubjid,
                            pe.visit::text                        AS visit,
                            pe.pedtc::TIMESTAMP without TIME zone AS lbdtc,
                            NULL::INTEGER                         AS lbdy,
                            pe.peseq::int                         AS lbseq,
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
                            ''::text                              AS lbstresu,
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
                            peloc::text                           AS lbloc,
                            NULL::text                            AS lbpos,
                            NULL::text                            AS lbstint,
                            NULL::NUMERIC                         AS lbuloq,
                            NULL::text                            AS lbclsig
                    FROM    pe 
            ) lb   
            left join   ds_en ds 
            on          lb.studyid = ds.studyid and lb.siteid = ds.siteid   and lb.usubjid = ds.usubjid
			left join ctable_listing.ctcae_listing c
on lb.studyid = c.studyid
and lb.usubjid = c.usubjid
and lb.lbtest = c.lbtest
and lb.lbcat = c.lbcat
and lb.visit = c.visit
and lb.lbdtc = c.lbdtc
and lb.lbstresn = c.lbstresn
            WHERE       lb.lbdtc IS NOT NULL
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
left join normlab nl on nl.studyid=ex.studyid and nl.siteid = ex.siteid and nl.usubjid=ex.usubjid and lb.lbseq = nl.lbseq
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



    

