/*
CCDM Subject second pass script

Notes: 
    - Update status to latest disposition record for subject. Set to 'Unknown'
        if null in order to satisfy not null constraint
    - Update exit date to earliest exiting disposition date for subject
    - Remove subject records without a matching site

Revision History: 17-May-2016 ACK Initial Version
*/

UPDATE subject s 
SET status = ds.dsterm 
FROM (SELECT comprehendid, dsterm
        FROM (SELECT ds.comprehendid, ds.dsterm, RANK() OVER (PARTITION BY ds.comprehendid ORDER BY c.ui_sequence DESC, ds.dsstdtc DESC) AS dsrank
                FROM ds
                JOIN app.ds_config c ON (ds.dsterm = c.customer_event)
                WHERE c.enabled IS TRUE) a
        WHERE dsrank = 1) ds
WHERE s.comprehendid = ds.comprehendid;

UPDATE subject
SET status = 'Unknown'
WHERE status IS NULL;

UPDATE subject SET exitdate = NULL;

UPDATE subject s 
SET exitdate = ds.dsstdtc 
FROM (SELECT ds.comprehendid, min(ds.dsstdtc) dsstdtc
        FROM ds
        JOIN app.ds_config c ON (ds.dsterm = c.customer_event)
        WHERE c.enabled IS TRUE
        AND c.event_state_id IN ('WITHDRAWN', 'COMPLETED')
        GROUP BY ds.comprehendid) ds
WHERE s.comprehendid = ds.comprehendid;
