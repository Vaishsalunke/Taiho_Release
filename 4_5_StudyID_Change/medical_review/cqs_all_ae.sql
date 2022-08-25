--DROP TABLE IF EXISTS "medical_review"."cqs_all_ae";

--CREATE TABLE "medical_review"."cqs_all_ae" AS
 select
	comprehendid,
	studyid,
	siteid,
	usubjid,
	aeterm,
	aeverbatim,
	aebodsys,
	aestdtc,
	aeendtc,
	aesev,
	aeser,
	aerelnst,
	aeseq,
	aesttm,
	aeentm,
	aellt,
	aelltcd,
	aeptcd,
	aehlt,
	aehltcd,
	aehlgt,
	aehlgtcd,
	aebdsycd,
	aesoc,
	aesoccd,
	aeacn,
	aeout,
	aetoxgr,
	aerptdt,
	preferredterm,
	aesi,
	objectuniquekey,
	aestdtc_iso,
	aeendtc_iso,
	sitecountrycode,
	aetox,
	sitename,
	sitecountry,
	aespid,
	aeongo,
	aecomm,
	studyname,
	siteregion
from
	"cqs"."ae";
--ALTER SCHEMA "medical_review" OWNER TO "taiho-stage-app";
--ALTER TABLE "medical_review"."cqs_all_ae" OWNER TO "taiho-stage-app-clinical-master-write";

