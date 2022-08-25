
/*
      Study: CQS all
      Table: CM
*/

--DROP TABLE IF EXISTS "medical_review"."cqs_all_cm";

--CREATE TABLE "medical_review"."cqs_all_cm" AS

SELECT
        comprehendid,
        studyid	,
        siteid	,
        usubjid	,
        cmseq	,
        cmtrt	,
        cmmodify,
        cmdecod	,
        cmcat	,
        cmscat	,
        cmindc	,
        cmdose	,
        cmdosu	,
        cmdosfrm,
        cmdosfrq,
        cmdostot,
        cmroute	,
        cmstdtc	,
        cmendtc	,
        cmsttm	,
        cmentm	,
        objectuniquekey,	
        sitecountrycode	,
        sitename	,
        sitecountry,
        cmongo	,
        cmspid	,
        cmstdtc_iso,
        cmendtc_iso	,
        studyname,
        cmdostxt,
        siteregion
FROM "cqs"."cm";


--ALTER SCHEMA "medical_review" OWNER TO "taiho-stage-app";

--ALTER TABLE "medical_review"."cqs_all_cm" OWNER TO "taiho-stage-app-clinical-master-write";
