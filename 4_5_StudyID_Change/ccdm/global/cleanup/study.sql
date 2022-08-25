/*
CCDM Study second pass script

Notes: 
    - Update studystatus to next or last milestone label
    - Update studycompletion to STUDY CLOSED milestone actual date

Revision History: 17-May-2016 ACK Initial Version
*/

WITH next_milestone AS (select * 
                        from (
                            select plan.studyid, coalesce(map.originalterm, plan.milestonelabel) as milestonelabel, plan.milestoneseq,
                            rank() over (partition by plan.studyid order by plan.milestoneseq) as milestonerank
                            from studymilestone plan
                            left join comprehendtermmap map on (plan.studyid = map.studyid and map.tablename = 'studymilestone'
                                                                        and map.columnname = 'milestonelabel' and map.comprehendterm = plan.milestonelabel)
                            where lower(plan.milestonetype) = 'planned'
                            and plan.milestoneseq > (select max(milestoneseq) from studymilestone actual
                                                        where actual.studyid = plan.studyid
                                                        and lower(actual.milestonetype) = 'actual'
                                                        and actual.expecteddate = (select max(comp.expecteddate) from studymilestone comp
                                                                                    where comp.studyid = actual.studyid
                                                                                    and lower(comp.milestonetype) = 'actual'))
                            order by plan.studyid, plan.milestoneseq) a
                        where milestonerank = 1),

    last_milestone AS (select p.studyid, coalesce(map.originalterm, p.milestonelabel) as milestonelabel
                        from studymilestone p
                        left join comprehendtermmap map on (p.studyid = map.studyid and map.tablename = 'studymilestone'
                                                                    and map.columnname = 'milestonelabel' and map.comprehendterm = p.milestonelabel)
                        where lower(p.milestonetype) = 'planned'
                        and p.milestoneseq = (select max(l.milestoneseq) from studymilestone l
                                                where  l.studyid = p.studyid
                                                and lower(l.milestonetype) = 'planned')),

    study_closed_milestone AS (select studyid, min(expecteddate) as expecteddate
                                from studymilestone
                                where milestonetype = 'Actual' and milestonelabel = 'STUDY CLOSED'
                                group by studyid)

UPDATE study
SET 
--studystatus = coalesce(n.milestonelabel, l.milestonelabel),
studycompletiondate = coalesce(sc.expecteddate, s.studycompletiondate)
FROM study s
LEFT JOIN next_milestone n ON(s.studyid = n.studyid)
LEFT JOIN last_milestone l ON(s.studyid = l.studyid)
LEFT JOIN study_closed_milestone sc ON (s.studyid = sc.studyid)
WHERE study.studyid = s.studyid;

update study a 
set studystartdate = least(b.dt,studystartdate)
from (select studyid, min(expecteddate) dt from studymilestone where milestonetype = 'Actual' group by studyid) b
where  a.studyid = b.studyid;

