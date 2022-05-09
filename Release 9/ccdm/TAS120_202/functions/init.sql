CREATE SCHEMA IF NOT EXISTS cqs_tas120_202_new; 

CREATE TABLE IF NOT EXISTS cqs_tas120_202_new.comprehendeventlog  (
  jobnum            int4 NOT NULL,
  jobseq            int4 NOT NULL,
  comprehendid      text NULL,
  studyid           text NULL,
  siteid            text NULL,
  croid             text NULL,
  modulecategory    text NOT NULL,
  modulesubcategory text NULL,
  eventid           text NOT NULL,
  eventdtc          timestamp NOT NULL,
  eventname         text NULL,
  eventdesc         text NULL,
  eventmessage      text NULL
);

CREATE TABLE IF NOT EXISTS cqs_tas120_202_new.rpt_site_cro_scores  (
  comprehendid            text NOT NULL,
  studyid                 text NOT NULL,
  croid                   text NOT NULL,
  siteid                  text NOT NULL,
  kpiid                   text NOT NULL,
  kpicategory             text NOT NULL,
  numerator               numeric NULL,
  denominator             numeric NULL,
  multiplier              numeric NULL,
  kpiscore                numeric NULL,
  kpicalculationdate      date NOT NULL,
  currentflag             bool NOT NULL,
  comprehend_update_time  timestamp NULL
);

CREATE TABLE IF NOT EXISTS cqs_tas120_202_new.rpt_study_cro_scores  (
  comprehendid            text NOT NULL,
  studyid                 text NOT NULL,
  croid                   text NOT NULL,
  kpiid                   text NOT NULL,
  kpicategory             text NOT NULL,
  numerator               numeric NULL,
  denominator             numeric NULL,
  multiplier              numeric NULL,
  kpiscore                numeric NULL,
  kpicalculationdate      date NOT NULL,
  currentflag             bool NOT NULL,
  comprehend_update_time  timestamp NULL
);

