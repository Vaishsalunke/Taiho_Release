/*
CCDM comprehend custom metrics
Notes: Standard mapping to CCDM custom metrics table
*/
WITH
included_studies AS (
  SELECT DISTINCT studyid FROM study
),
custommetric AS (
  SELECT
    NULL::TEXT AS metricid,
    NULL::TEXT AS studyid,
    NULL::TEXT AS siteid,
    NULL::NUMERIC AS numerator,
    NULL::NUMERIC AS denominator,
    NULL::BOOLEAN AS invalidvalue
)
SELECT 
  cm.metricid::TEXT AS metricid,
  cm.studyid::TEXT AS studyid,
  cm.siteid::TEXT AS siteid,
  cm.numerator::NUMERIC AS numerator,
  cm.denominator::NUMERIC AS denominator,
  cm.invalidvalue::BOOLEAN AS invalidvalue
  /*KEY , NOW()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM custommetric cm
JOIN included_studies istu ON (istu.studyid = cm.studyid);
