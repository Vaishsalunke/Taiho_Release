

/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     eg_data AS (
  SELECT   eg.studyid, 
                 eg.siteid AS siteid,
          eg.usubjid  AS usubjid,
                   eg.egseq,
                  upper(eg.egtestcd) as egtestcd, 
                  eg.egtest, 
                  eg.egcat, 
                  eg.egscat, 
                  eg.egpos, 
                  eg.egorres, 
                  eg.egorresu, 
                  eg.egstresn, 
                  eg.egstresu, 
                  eg.egstat, 
                  eg.egloc, 
                  eg.egblfl, 
                  eg.visit, 
                  eg.egdtc, 
                  eg.egtm 
         FROM     (
          SELECT     "project"::text    AS studyid, 
           "SiteNumber"::text AS siteid, 
           "Subject"::text    AS usubjid, 
           --Row_number() OVER (partition BY "project", "SiteNumber", "Subject" ORDER BY "ECGDAT")::int AS egseq, 
		   egseq::int AS egseq,
           'ECG'::text     AS egtestcd, 
           'ECG'::text       AS egtest, 
           'ECG'::text        AS egcat, 
           egscat::text         AS egscat, 
           null::text      AS egpos, 
           egorres::text         AS egorres,  
           egorresu::text         AS egorresu, 
           egstresn::numeric     AS egstresn, 
           egstresu::text     AS egstresu, 
           null::text     AS egstat, 
           NULL::text         AS egloc, 
           NULL::text         AS egblfl, 
           "FolderName"::text AS visit, 
           "ECGDAT"::timestamp without time zone AS egdtc, 
           null::time without time zone AS egtm   
FROM       "tas120_202"."ECG" eg
CROSS JOIN lateral( VALUES 
       (concat("instanceId",1),'RR Interval',"ECGRR"::text , "ECGRR_Units" , "ECGRR_Units" , "ECGRR"::text ),
       (concat("instanceId",2),'HR',"ECGHR"::text , "ECGHR_Units" , "ECGHR_Units" , "ECGHR"::text ),
	   (concat("instanceId",3),'QT Interval',"ECGQT"::text , "ECGQT_Units" , "ECGQT_Units" , "ECGQT"::text ),
	   (concat("instanceId",4),'QTc Interval',"ECGQTC"::text , "ECGQTC_Units" , "ECGQTC_Units" , "ECGQTC"::text )
       ) AS t 
       (egseq,egscat,egorres, egorresu, egstresu, egstresn ))eg
     )
   

SELECT
        /*KEY (eg.studyid::text || '~' || eg.siteid::text || '~' || eg.usubjid::text) AS comprehendid, KEY*/
        eg.studyid::text AS studyid,
        eg.siteid::text AS siteid,
        eg.usubjid::text AS usubjid,
        eg.egseq::int AS egseq,
        eg.egtestcd::text AS egtestcd,
        eg.egtest::text AS egtest,
        eg.egcat::text AS egcat,
        eg.egscat::text AS egscat,
        eg.egpos::text AS egpos,
        eg.egorres::text AS egorres,
        eg.egorresu::text AS egorresu,
        eg.egstresn::numeric AS egstresn,
        eg.egstresu::text AS egstresu,
        eg.egstat::text AS egstat,
        eg.egloc::text AS egloc,
        eg.egblfl::text AS egblfl,
        eg.visit::text AS visit,
        eg.egdtc::timestamp without time zone AS egdtc,
        eg.egtm::time without time zone AS egtm
        /*KEY , (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM eg_data eg
JOIN included_subjects s ON (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid);      
