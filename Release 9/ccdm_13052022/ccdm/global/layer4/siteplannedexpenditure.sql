/*
CCDM SitePlannedExpenditure mapping
Notes: Standard mapping to CCDM SitePlannedExpenditure table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     siteplannedexpenditure_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS exptype,
                        null::text AS expcat,
                        null::text AS expsubcat,
                        null::text AS explabel,
                        null::date AS expdtc,
                        null::int AS expseq,
                        null::numeric AS expamount,
                        null::text AS expunit,
                        null::numeric AS expamountstd,
                        null::text AS expunitstd )

SELECT 
        /*KEY (spe.studyid || '~' || spe.siteid)::text AS comprehendid, KEY*/
        spe.studyid::text AS studyid,
        spe.siteid::text AS siteid,
        spe.exptype::text AS exptype,
        spe.expcat::text AS expcat,
        spe.expsubcat::text AS expsubcat,
        spe.explabel::text AS explabel,
        spe.expdtc::date AS expdtc,
        spe.expseq::int AS expseq,
        spe.expamount::numeric AS expamount,
        spe.expunit::text AS expunit,
        spe.expamountstd::numeric AS expamountstd,
        spe.expunitstd::text AS expunitstd
        /*KEY , (spe.studyid || '~' || spe.siteid || '~' || spe.expSeq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteplannedexpenditure_data spe
JOIN included_sites si ON (spe.studyid = si.studyid AND spe.siteid = si.siteid)
WHERE 1=2; 
