/*
CCDM TV mapping
Notes: Standard mapping to CCDM TV table
*/

WITH included_studies AS (
	SELECT studyid FROM study
),

tv_scheduled AS (

	SELECT visit_number     as      visitnum, visit, visitdy , window_before_visit_day as "visitwindowbefore", window_after_visit_day as "visitwindowafter"
            FROM tv_internal.tv_tracker_list where studyid='TAS0612-101'
	
),

tv_data AS (
select replace(studyid,'TAS0612_101','TAS0612-101') as studyid,
visitnum,
visit,
visitdy,
visitwindowafter,
visitwindowbefore
,case when ((lower(visit) like '%day 1' OR lower(visit) like 'day 1 %' 
OR lower(visit) like '% day 1 %' OR lower(visit) like '% day 1<%') or (lower(visit) like '%day 01' OR lower(visit) like 'day 01 %' 
OR lower(visit) like '% day 01 %' OR lower(visit) like '% day 01<%') or (lower(visit) like '%day 1-%') or (lower(visit) like '%day 01-%')) then 'True' else null end as isbaselinevisit
from (
	SELECT
		'TAS0612_101'::text AS studyid,
		visitnum::numeric AS visitnum,
		visit::text AS visit,
		visitdy::int AS visitdy,
		visitwindowbefore::int AS visitwindowbefore,
		visitwindowafter::int AS visitwindowafter
	FROM tv_scheduled tvs

	UNION 

	SELECT
		DISTINCT sv.studyid::text AS studyid,
		99::numeric AS visitnum,
		sv."visit"::text AS visit,
		99999::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM sv 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM tv_scheduled)
	--and studyid = 'TAS0612_101'

	UNION 
	SELECT 
		DISTINCT studyid::text AS studyid,
		'99'::numeric AS visitnum,
		visit::text AS visit,
		'99999'::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM formdata 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM sv ) 
	AND (studyid, visit) NOT IN (select distinct studyid, visit FROM tv_scheduled)
	--and studyid = 'TAS0612_101'
  )a --where (visit <> 'Day 1 of Cycle' and visitnum <> 99)
	
)

SELECT
	/*KEY tv.studyid::text AS comprehendid, KEY*/
	tv.studyid::text AS studyid,
	tv.visitnum::numeric AS visitnum,
	tv.visit::text AS visit,
	tv.visitdy::int AS visitdy,
	tv.visitwindowbefore::int AS visitwindowbefore,
	tv.visitwindowafter::int AS visitwindowafter,
	tv.isbaselinevisit::boolean AS isbaselinevisit,
	'True'::boolean as isvisible
	/*KEY , (tv.studyid || '~' || tv.visit)::text  AS objectuniquekey KEY*/
	/*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM tv_data tv
JOIN included_studies st ON (st.studyid = tv.studyid);




