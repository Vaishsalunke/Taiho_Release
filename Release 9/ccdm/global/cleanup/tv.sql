/*
CCDM Subject second pass script

Notes: 
    -To update the VISITNUM based on the TV table.

*/

update ie set visitnum = tv.visitnum
from tv 
where ie.studyid = tv.studyid and
        ie.visit = tv.visit;

update visitform set visitnum = tv.visitnum
from tv 
where visitform.studyid = tv.studyid and
        visitform.visit = tv.visit;

update dm set visitnum = tv.visitnum
from tv 
where dm.studyid = tv.studyid and
        dm.visit = tv.visit;


update sv set visitnum = tv.visitnum
from tv
where sv.studyid = tv.studyid and
        sv.visit = tv.visit;


