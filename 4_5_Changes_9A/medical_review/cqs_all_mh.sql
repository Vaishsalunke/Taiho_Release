/*
      Study: CQS all
      Table: MH
*/

--DROP TABLE IF EXISTS "medical_review"."cqs_all_mh";

--CREATE TABLE "medical_review"."cqs_all_mh" AS


SELECT
        comprehendid,
        studyid,
        siteid	,
        usubjid	,
        mhseq	,
        mhterm	,
        mhdecod	,
        mhcat	,
        mhscat	,
        mhbodsys,
        mhstdtc	,
        mhsttm,
        mhendtc	,
        mhendtm	,
        objectuniquekey,		
        mhstdtc_iso	,
        mhendtc_iso	,
        mhspid
FROM "cqs"."mh";

--ALTER SCHEMA "medical_review" OWNER TO "taiho-stage-app";

--ALTER TABLE "medical_review"."cqs_all_mh" OWNER TO "taiho-stage-app-clinical-master-write";
