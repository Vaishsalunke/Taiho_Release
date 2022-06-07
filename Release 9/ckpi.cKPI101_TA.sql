/*Mapping script for cKPI101_TA
Table name : "ckpi.cKPI101_TA.sql"
Project name : Taiho*/

CREATE SCHEMA IF NOT EXISTS ckpi;

drop table if exists ckpi."cKPI101_TA_new";

create table ckpi."cKPI101_TA_new" as



with NL_3681 as
  (
    Select 'TAS3681_101_DOSE_EXP' as Study, "SiteNumber", "Subject","NLSITE","InstanceName",
    count(*) as CountOfNL, max("NLIMGDAT") as nldate
    From tas3681_101."NL" nl
    where lower(nl."NLSITE") = 'bone'
    and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_EXP')
    group by 1,2,3,4,5

Union

Select 'TAS3681_101_DOSE_ESC' as Study, "SiteNumber", "Subject","NLSITE","InstanceName",
    count(*) as CountOfNL, max("NLIMGDAT") as nldate
    From tas3681_101."NL" nl
    where lower(nl."NLSITE") = 'bone'
    and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_ESC')
    group by 1,2,3,4,5
    ),  
 
ALLTLB as
(
  Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
min(DateofImage) as DateofImage,
FolderSeq,
max(SumTLMeasure) as SumTLMeasure,
tlbsite,
tlbterm,
tlbdim,
tlbmeth,
"RecordPosition"
From
    (
      ----tas0612_101.TLB--------------
      Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"TLBSUM" :: numeric as SumTLMeasure,
"TLSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
From   tas0612_101."TLB"
WHERE "TLBDIM" IS NOT null --or "TLBDAT" IS NOT NULL
 
Union all
 
        ----tas3681_101.TLB--------------
( Select 'TAS3681_101_DOSE_ESC' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"TLBSUM" :: numeric as SumTLMeasure,
"TLBSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
From   tas3681_101."TLB"
WHERE "TLBDIM" IS NOT null
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_ESC')

Union all

Select 'TAS3681_101_DOSE_EXP' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"TLBSUM" :: numeric as SumTLMeasure,
"TLBSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
From tas3681_101."TLB"
WHERE "TLBDIM" IS NOT null
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_EXP')
)
     
Union all
 
        ----tas120_201.TLB--------------
    Select "project" :: text as Study,
            "SiteNumber" :: text as SiteNumber,
            "Subject" :: text as Subject,
            "InstanceName" :: text as InstanceName,
            "InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
            "TLBDAT" :: timestamp as DateofImage,
            "FolderSeq" :: numeric as FolderSeq,
            "TLBSUM" :: numeric as SumTLMeasure,
            "TLBSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
    From tas120_201."TLB"
WHERE "TLBDIM" IS NOT null --or "TLBDAT" IS NOT NULL
 
    Union all
     
----tas120_202.TLB--------------
    Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"TLBSUM" :: numeric as SumTLMeasure,
"TLBSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
    From tas120_202."TLB"
    WHERE "TLBDIM" IS NOT null --or "TLBDAT" IS NOT NULL

Union all

----tas2940_101.TLB--------------
    Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
min("TLBDAT") :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max("TLBSUM") :: numeric as SumTLMeasure,
"TLBSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
    From tas2940_101."TLB"
    WHERE "TLBDIM" IS NOT null --or "TLBDAT" IS NOT NULL
    group by 1,2,3,4,5,7,9,10,11,12,13
   
   
    Union all
    ----tas120_203.TLB--------------
    Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
min("TLBDAT") :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max("TLBSUM") :: numeric as SumTLMeasure,
"TLBSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
    From tas120_203."TLB"
    WHERE "TLBDIM" IS NOT null --or "TLBDAT" IS NOT NULL
    group by 1,2,3,4,5,7,9,10,11,12,13
   
    Union all
    ----tas120_204.TLB--------------
    Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
min("TLBDAT") :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max("TLBSUM") :: numeric as SumTLMeasure,
"TLBSITE":: text as tlbsite,
"TLBTERM":: text as tlbterm,
"TLBDIM":: numeric as tlbdim,
"TLBMETH":: text as tlbmeth,
"RecordPosition"
    From tas120_204."TLB"
    WHERE "TLBDIM" IS NOT null --or "TLBDAT" IS NOT NULL
    group by 1,2,3,4,5,7,9,10,11,12,13

Union all

Select "project" :: text as Study,
            "SiteNumber" :: text as SiteNumber,
            "Subject" :: text as Subject,
            "InstanceName" :: text as InstanceName,
            "InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
            min("TUSTDT") :: timestamp as DateofImage,
            "FolderSeq" :: numeric as FolderSeq,
            max("TUTLSUM") :: numeric as SumTLMeasure,
            "TLSITE":: text as tlbsite,
            "LESDESC":: text as tlbterm,
"MEASURMT":: numeric as tlbdim,
"TUMETH1":: text as tlbmeth,
"RecordPosition"
From tas117_201."TLRN2"
WHERE "MEASURMT" IS NOT null or "TUSTDT" IS NOT NULL
group by 1,2,3,4,5,7,9,10,11,12,13
    ) a
where (DateofImage is not null or SumTLMeasure is not null )
group by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq, tlbsite, tlbterm, tlbdim, tlbmeth,"RecordPosition"
),

ALLNTLB as (
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
min(DateofImage) as DateofImage,
FolderSeq,
ntlbsite,
ntlbterm,
ntlbmeth,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage) as rnk
From (
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas0612_101."NTLB"
WHERE "NTLBDAT" IS NOT NULL

Union all

Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLBSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas120_201."NTLB"
WHERE "NTLBDAT" IS NOT null

Union all

Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLBSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas120_202."NTLB"
WHERE "NTLBDAT" IS NOT NULL

Union all

(
Select 'TAS3681_101_DOSE_ESC' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLBSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas3681_101."NTLB"
WHERE "NTLBDAT" IS NOT null
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_ESC')

Union all

Select 'TAS3681_101_DOSE_EXP' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLBSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas3681_101."NTLB"
WHERE "NTLBDAT" IS NOT null
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_EXP')

Union all

Select distinct "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLBSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas120_203."NTLB"
WHERE "NTLBDAT" IS NOT NULL

Union all

Select distinct "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLBSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas120_204."NTLB"
WHERE "NTLBDAT" IS NOT NULL

Union all

Select distinct "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLBDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLBSITE":: text as ntlbsite,
"NTLBTERM":: text as ntlbterm,
"NTLBMETH":: text as ntlbmeth
From   tas2940_101."NTLB"
WHERE "NTLBDAT" IS NOT NULL

Union all

Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TUSTDT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"NTLSITE":: text as ntlbsite,
"LESDESC":: text as ntlbterm,
"TUMETH1":: text as ntlbmeth
From   tas117_201."NTLBASE"
WHERE "TUSTDT" IS NOT NULL
)
  )a
  where DateofImage is not null
  group by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, DateofImage, FolderSeq,ntlbsite,ntlbterm, ntlbmeth
),

ALLTL AS (
 Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlsite,
tlterm,
tldim,
tlmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
 From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
"TLTERM":: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition",
min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
From TAS0612_101."TL"
WHERE   "TLDIM" IS NOT null --or "TLDAT" IS NOT NULL
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13,14
) a

  Union all

  Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlsite,
tlterm,
tldim,
tlmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
"TLTERM":: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition"--,
,min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
From tas120_201."TL"
WHERE   "TLDIM" IS NOT null --or "TLDAT" IS NOT NULL
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13, 14
) a

  Union all

  Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlsite,
tlterm,
tldim,
tlmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
  From
   (
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
"TLTERM":: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition",
min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
From tas120_202."TL"
WHERE   "TLDIM" IS NOT null --or "TLDAT" IS NOT NULL
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13,14
) a

  Union ALL

  Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlsite,
tlterm,
tldim,
tlmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
  From
(
Select 'TAS3681_101_DOSE_ESC' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
"TLTERM":: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition",
min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
From TAS3681_101."TL"
WHERE   "TLDIM" IS NOT null --or "TLDAT" IS NOT NULL
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_ESC')
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13,"project","RecordPosition"

Union all

Select 'TAS3681_101_DOSE_EXP' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
"TLTERM":: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition",
min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
From TAS3681_101."TL"
WHERE   "TLDIM" IS NOT null --or "TLDAT" IS NOT NULL
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_EXP')
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13,"project","RecordPosition"
) a

union all

Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlsite,
tlterm,
tldim,
tlmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
  From
   (
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
regexp_replace("TLTERM", E'[\\n\\r]+', ' ' ):: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition",
min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
From tas120_203."TL"
WHERE   "TLDIM" IS NOT null or "TLDAT" IS NOT NULL
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13,14
) a

  Union all

  Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlsite,
tlterm,
tldim,
tlmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
  From
   (
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
"TLTERM":: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition",
min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
   From tas120_204."TL"
WHERE   "TLDIM" IS NOT null or "TLDAT" IS NOT NULL
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13,14
) a

  Union all

  Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlsite,
tlterm,
tldim,
tlmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
  From
   (
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlsite,
"TLTERM":: text as tlterm,
"TLDIM":: numeric as tldim,
"TLMETH":: text as tlmeth,
"PageRepeatNumber",
"RecordPosition",
min("TLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TLSUM") ) AS order_date
From tas2940_101."TL"
WHERE   "TLDIM" IS NOT null or "TLDAT" IS NOT NULL
group by 1, 2, 3, 4, 5, 6, 7,9,10,11,12,13,14
) a
 
  UNION ALL
 
  Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
SumTLMeasure,
tlbsite,
tlbterm,
tlbdim,
tlbmeth,
"RecordPosition",
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
  From(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TUSTDT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
max(nullif("TUTLSUM"::text,'')) :: numeric as SumTLMeasure,
"TLSITE":: text as tlbsite,
"LESDESC":: text as tlbterm,
"MEASURMT":: numeric as tlbdim,
"TUMETH4":: text as tlbmeth,
"PageRepeatNumber",
"RecordPosition",
min("TUSTDT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName", "InstanceRepeatNumber", "FolderSeq", "PageRepeatNumber", max("TUTLSUM") ) AS order_date
From tas117_201."TLPB"
WHERE "MEASURMT" IS NOT null or "TUSTDT" IS NOT NULL
group by 1,2,3,4,5,6,7,9,10,11,12,"PageRepeatNumber","RecordPosition"
)a
),

ALLNTL AS (
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From TAS0612_101."NTL"
WHERE "NTLDAT" IS NOT NULL and concat("project","Subject") NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)
group by 1, 2, 3, 4, 5,6,7,8
) a
   
Union all
   
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq ,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From TAS120_201."NTL"
WHERE "NTLDAT" IS NOT NULL and concat("project","Subject") NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)
group by 1, 2, 3, 4, 5,6, 7, 8
) a
   
Union all
   
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq ,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
  From TAS120_202."NTL"
WHERE "NTLDAT" IS NOT NULL  and concat("project","Subject") NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)
group by 1, 2, 3, 4, 5, 6, 7, 8
) a
   
Union all
   
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From  
(
Select 'TAS3681_101_DOSE_ESC' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From TAS3681_101."NTL"
WHERE "NTLDAT" IS NOT null
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_ESC')
group by 1, 2, 3, 4, 5, 6, 7, 8,"project"

Union all

Select 'TAS3681_101_DOSE_EXP' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From TAS3681_101."NTL"
WHERE "NTLDAT" IS NOT null
and "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_EXP')
group by 1, 2, 3, 4, 5, 6, 7, 8,"project"
) a
where  concat(Study,Subject) NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)

union all

Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq ,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From TAS120_203."NTL"
WHERE "NTLDAT" IS NOT NULL  and concat("project","Subject") NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)
group by 1, 2, 3, 4, 5, 6, 7, 8
) a

Union all
   
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq ,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From TAS120_204."NTL"
WHERE "NTLDAT" IS NOT NULL  and concat("project","Subject") NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)
group by 1, 2, 3, 4, 5, 6, 7, 8
) a

Union all
   
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"NTLDAT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq ,
"PageRepeatNumber"
,min("NTLDAT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From TAS2940_101."NTL"
WHERE "NTLDAT" IS NOT NULL  and concat("project","Subject") NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)
group by 1, 2, 3, 4, 5, 6, 7, 8
) a

Union all
   
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
FolderSeq,
DENSE_RANK() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by order_date ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"TUSTDT" :: timestamp as DateofImage,
"FolderSeq" :: numeric as FolderSeq,
"PageRepeatNumber"
,min("TUSTDT") OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName",
"InstanceRepeatNumber", "FolderSeq","PageRepeatNumber")::text AS order_date
From   tas117_201."NTLPB"
WHERE "TUSTDT" IS NOT null and concat("project","Subject") NOT IN (Select DISTINCT concat(Study,Subject) From ALLTL)
group by 1,2,3,4,5,6,7,8
) a
          ),


ALLOR AS (
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES" :: text as ORTLRES,
"ORNTLRES" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From   TAS0612_101."OR"
) a
 
Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES" :: text as ORTLRES,
"ORNTLRES" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From   TAS120_201."OR"
) a
 
Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES" :: text as ORTLRES,
"ORNTLRES" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From   TAS120_202."OR1"
) a
 
Union ALL
   
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select 'TAS3681_101_DOSE_ESC' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES" :: text as ORTLRES,
"ORNTLRES" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From TAS3681_101."OR"
where   "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_ESC')

Union all

Select 'TAS3681_101_DOSE_EXP' :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES" :: text as ORTLRES,
"ORNTLRES" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From TAS3681_101."OR"
where   "Subject" in (Select usubjid From cqs.subject where studyid='TAS3681_101_DOSE_EXP')
) a

union all

Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES_STD" :: text as ORTLRES,
"ORNTLRES_STD" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES_STD" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From   TAS120_203."OR"
) a

Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES_STD" :: text as ORTLRES,
"ORNTLRES_STD" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES_STD" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From   TAS120_204."OR"
) a

Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES_STD" :: text as ORTLRES,
"ORNTLRES_STD" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES_STD" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From   TAS117_201."OR"
) a

Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage as "Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
FolderSeq,
rank() over (partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq order by DateofImage ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORDAT" :: timestamp as DateofImage,
"ORTLRES_STD" :: text as ORTLRES,
"ORNTLRES_STD" :: text as ORNTLRES,
"ORNLYN" :: text as ORNLYN,
"ORRES_STD" :: text as ORRES,
"FolderSeq" :: numeric as FolderSeq
From   TAS2940_101."OR"
) a

),

ALLBOR AS (
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
bestoverallresponse,
rank() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber order by bestoverallresponse ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"BORTLRES" :: text as bestoverallresponse
From TAS0612_101."BOR"
) a
 
Union all

Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
bestoverallresponse,
rank() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber order by bestoverallresponse ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"BORTLRES" :: text as bestoverallresponse
From TAS120_201."BOR"
) a
 
           Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
bestoverallresponse,
rank() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber order by bestoverallresponse ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"BORTLRES" :: text as bestoverallresponse
From TAS120_202."BOR"
) a

Union all

Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
bestoverallresponse,
rank() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber order by bestoverallresponse ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORRES_STD" :: text as bestoverallresponse
From TAS120_203."OR"
) a

Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
bestoverallresponse,
rank() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber order by bestoverallresponse ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORRES_STD" :: text as bestoverallresponse
--"BORTLRES":: text as bestoverallresponse
From TAS120_204."OR"

) a

 
Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
bestoverallresponse,
rank() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber order by bestoverallresponse ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORRES_STD" :: text as bestoverallresponse
From TAS117_201."OR"
) a

 
Union all
 
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
bestoverallresponse,
rank() over ( partition by Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber order by bestoverallresponse ) as rnk
From
(
Select "project" :: text as Study,
"SiteNumber" :: text as SiteNumber,
"Subject" :: text as Subject,
"InstanceName" :: text as InstanceName,
"InstanceRepeatNumber" :: numeric as InstanceRepeatNumber,
"ORRES_STD" :: text as bestoverallresponse
From TAS2940_101."OR"
) a
)

,ALLTL_NTL_OR_BOR as(
Select coalesce(TL.Study,NTL.Study,R.Study) as Study,
coalesce(TL.SiteNumber,NTL.SiteNumber,R.SiteNumber) as SiteNumber,
coalesce(TL.Subject,NTL.Subject,R.Subject)as Subject,
coalesce(TL.InstanceName,NTL.InstanceName,R.InstanceName) as InstanceName ,
coalesce(TL.InstanceRepeatNumber,NTL.InstanceRepeatNumber,R.InstanceRepeatNumber)as InstanceRepeatNumber,
COALESCE (TL.Dateofimage, NTL.Dateofimage) AS Dateofimage,
R."Date of Tumor Assessment",
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
"RecordPosition",
case when bestoverallresponse is null then best.minbr else best1.minbor end as bestoverallresponse,
coalesce(TL.FolderSeq,NTL.FolderSeq,R.FolderSeq) as FolderSeq,
SumTLMeasure,
TL.tlsite,
TL.tlterm,
TL.tldim,
TL.tlmeth
From ALLOR AS R
full outer JOIN ALLTL AS TL
on     (TL.Study = R.Study AND
TL.SiteNumber = R.SiteNumber AND
TL.Subject = R.Subject AND
TL.InstanceName = R.InstanceName AND
TL.InstanceRepeatNumber = R.InstanceRepeatNumber and
TL.rnk = R.rnk)
full outer JOIN ALLNTL AS NTL
on     (NTL.Study = R.Study AND
NTL.SiteNumber = R.SiteNumber AND
NTL.Subject = R.Subject AND
NTL.InstanceName = R.InstanceName AND
NTL.InstanceRepeatNumber = R.InstanceRepeatNumber and
NTL.rnk = R.rnk
and concat(NTL.Study,NTL.Subject) not in (select concat(TL.Study,TL.Subject) from ALLTL TL)
)
Left JOIN ALLBOR BR
on (R.Study = BR.Study AND
R.SiteNumber = BR.SiteNumber AND
R.Subject = BR.Subject AND
R.InstanceName = BR.InstanceName AND
R.InstanceRepeatNumber = BR.InstanceRepeatNumber
--and TL.rank = BR.rank
)
Left JOIN (
Select min(bestor) over ( partition by a.Study, a.SiteNumber, a.Subject order by bestor ) :: text as minbr,
a.Study,
a.SiteNumber,
a.Subject,
a.InstanceName,
a.InstanceRepeatNumber,
a.rnk
From
(
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
rnk,
CASE WHEN ORRES = 'CR' THEN 1
WHEN ORRES = 'PR' THEN 2
WHEN ORRES = 'SD' THEN 3
WHEN ORRES = 'PD' THEN 4
ELSE 5
End as bestor
From ALLOR
) a
) best
on ( R.Study = best.Study AND
 R.SiteNumber = best.SiteNumber AND
 R.Subject = best.Subject AND
 R.InstanceName = best.InstanceName and
 R.rnk = best.rnk
)

Left JOIN (
Select min(bestbor) over ( partition by a.Study, a.SiteNumber, a.Subject order by bestbor ) :: text as minbor,
a.Study,
a.SiteNumber,
a.Subject,
a.InstanceName,
a.InstanceRepeatNumber,
a.rnk
From
(
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
rnk,
CASE WHEN bestoverallresponse = 'CR' THEN 1
WHEN bestoverallresponse = 'PR' THEN 2
WHEN bestoverallresponse = 'SD' THEN 3
WHEN bestoverallresponse = 'PD' THEN 4
ELSE 5
End as bestbor
From ALLBOR
) a
) best1
on ( R.Study = best1.Study AND
 R.SiteNumber = best1.SiteNumber AND
 R.Subject = best1.Subject AND
 R.InstanceName = best1.InstanceName
 --and R.rnk = best1.rnk
)


ORDER BY TL.Subject, R.FolderSeq, TL.Dateofimage
),
 
sv_tv as (
Select s.studyid,
s.usubjid,
s.svstdtc as C1D1,
s.visit
From cqs.sv s
left join cqs.tv t
on ( s.studyid = t.studyid and s.visit = t.visit)
where t.visitdy = 0
),

ALLDATA as (
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
"Date of Tumor Assessment",
FolderSeq,
SumTLMeasure,
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
"RecordPosition",
bestoverallresponse,
"Site Of Lesion",
"Lesion Description",
"Measurement of Target Lesion",
"Imaging Method",
DENSE_RANK () over( partition by Study, SiteNumber, Subject order BY order_date, "Date of Tumor Assessment", FolderSeq )-1 :: int as Order_of_Scan
From
(
Select Study,
SiteNumber,
Subject,
InstanceName,
InstanceRepeatNumber,
DateofImage,
"Date of Tumor Assessment",
FolderSeq,
SumTLMeasure,
ORTLRES,
ORNTLRES,
ORNLYN,
ORRES,
"RecordPosition",
bestoverallresponse,
tlsite as "Site Of Lesion",
tlterm as "Lesion Description",
tldim as "Measurement of Target Lesion",
tlmeth as "Imaging Method",
min(DateofImage) OVER (PARTITION BY Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq ) AS order_date
From ALLTL_NTL_OR_BOR
     
Union all
     
Select Study
,SiteNumber
,Subject
,InstanceName
,InstanceRepeatNumber
,DateofImage
,null--:: as"Date of Tumor Assessment"
,FolderSeq
,SumTLMeasure
,null --::"ORTLRES"
,null --::"ORNTLRES"
,null --::"ORNLYN"
,null --::"ORRES"
,"RecordPosition"
,null --::"bestoverallresponse"
,tlbsite as "Site Of Lesion",
tlbterm as "Lesion Description",
tlbdim as "Measurement of Target Lesion",
tlbmeth as "Imaging Method",
min(DateofImage) OVER (PARTITION BY Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq ) AS order_date
From ALLTLB

Union ALL

Select Study
,SiteNumber
,Subject
,InstanceName
,InstanceRepeatNumber
,DateofImage
,null--:: as"Date of Tumor Assessment"
,FolderSeq
,NULL --::SumTLMeasure
,null --::"ORTLRES"
,null --::"ORNTLRES"
,null --::"ORNLYN"
,null --::"ORRES"
,null --::"RecordPosition"
,null --::"bestoverallresponse"
,NULL --::ntlbsite as "Site Of Lesion"
,NULL --::ntlbterm as "Lesion Description"
,NULL --tlbdim as "Measurement of Target Lesion",
,NULL --::ntlbmeth as "Imaging Method",
,min(DateofImage) OVER (PARTITION BY Study, SiteNumber, Subject, InstanceName, InstanceRepeatNumber, FolderSeq ) AS order_date
From ALLNTLB
WHERE concat(Study,Subject) NOT IN (Select DISTINCT concat(Study,Subject) From ALLTLB)

) b
WHERE "Date of Tumor Assessment" IS NOT null or DateofImage is not null
)

Select Distinct t.Study,
case
when (t.Study like'%TAS3681_101%' or t.Study = 'TAS120_201' or  t.Study = 'TAS120_202')
then t.SiteNumber
else t.study||'_'||split_part(t.SiteNumber,'_',2)
end as SiteNumber,
t.Subject,
t.InstanceName,
t.InstanceRepeatNumber,
t.DateofImage as "Date of Image",
t."Date of Tumor Assessment",
t.FolderSeq,
t.SumTLMeasure,
t.ORTLRES,
t.ORNTLRES,
t.ORNLYN,
t.ORRES,
t."Imaging Method",
CASE
WHEN t.bestoverallresponse = '1' THEN 'CR'
WHEN t.bestoverallresponse = '2' THEN 'PR'
WHEN t.bestoverallresponse = '3' THEN 'SD'
WHEN t.bestoverallresponse = '4' THEN 'PD'
WHEN t.bestoverallresponse = '5' THEN 'NE'
ELSE t.bestoverallresponse
END AS bestoverallresponse,
dm.arm :: text as Cohort,
st.C1D1,
t.Order_of_Scan,
date_part('DAY', t.DateofImage - st.C1D1)+1 as DaysFromC1D1, -- change as per UAT Log 16
ROUND(  CASE
WHEN Order_of_Scan <> 0 and (ORTLRES <>'NE' or t.SumTLMeasure <> 0)
THEN ( (t.SumTLMeasure / c.SumTLMeasure)-1) * 100
ELSE 0
END,2
) as ChangeFromBaseline,

ROUND( CASE
WHEN ( Select min(B.SumTLMeasure)
From ALLDATA B
WHERE t.Study = B.Study AND
t.SiteNumber = B.SiteNumber AND
t.Subject = B.Subject AND
t.Order_of_Scan > B.Order_of_Scan
) > 1
AND ( Select min(B.SumTLMeasure)
From ALLDATA B
WHERE t.Study = B.Study AND
t.SiteNumber = B.SiteNumber AND
t.Subject = B.Subject AND
t.Order_of_Scan > B.Order_of_Scan
) < t.SumTLMeasure
--and (ORTLRES <>'NE' or t.SumTLMeasure <> 0)
THEN (
(t.SumTLMeasure /( Select min(B.SumTLMeasure)
From ALLDATA B
WHERE t.Study = B.Study AND
t.SiteNumber = B.SiteNumber AND
t.Subject = B.Subject AND
t.Order_of_Scan > B.Order_of_Scan
)
)-1
)* 100 END, 2
) AS "Increase",
"Site Of Lesion",
"Lesion Description",
"Measurement of Target Lesion"
,nl.CountOfNL
,nl.nldate
,s.Status
,case when lower(status) in ('enrolled','completed') then 'Ongoing'
 when lower(status) in ('early eot','withdrawn') then 'Discontinued'
end "Subject Enrollment Status"

From ALLDATA t
left join ALLTLB c
on (t.Study = c.Study AND
t.SiteNumber = c.SiteNumber AND
t.Subject = c.Subject and
lower( t."Site Of Lesion")=lower(c.tlbsite) and
--lower(t."Lesion Description")=lower(c.tlbterm)
(case when lower(t."Lesion Description")!=lower(c.tlbterm) then t."RecordPosition"=c."RecordPosition" else lower(t."Lesion Description")=lower(c.tlbterm)end)
)
left join sv_tv st
on   ( t.Study = st.studyid AND t.Subject = st.usubjid )
left join cqs.dm
on (t.Study = dm.Studyid AND
(case
when (t.Study like'%TAS3681_101%' or t.Study = 'TAS120_201' or  t.Study = 'TAS120_202')
then t.SiteNumber
else t.study||'_'||split_part(t.SiteNumber,'_',2)
end) = dm.Siteid AND
t.Subject = dm.usubjid
)
left join NL_3681 nl
on          (t.Study = nl.Study and
             t.SiteNumber = nl."SiteNumber" and
             t.Subject = nl."Subject" and
             --lower(t."Site Of Lesion") = lower(nl."NLSITE") and
             t.InstanceName = nl."InstanceName" and
             t.DateofImage = nl.nldate and
             lower(t.ORNLYN) = 'yes'
)
left join cqs.subject s
on (
t.Study = s.studyid and
(case
when (t.Study like'%TAS3681_101%' or t.Study = 'TAS120_201' or  t.Study = 'TAS120_202')
then t.SiteNumber
else t.study||'_'||split_part(t.SiteNumber,'_',2)
end) = s.siteid and
t.Subject = s.usubjid
)
;






drop table if exists ckpi."cKPI101_TA_orig";

alter table if exists ckpi."cKPI101_TA" rename to "cKPI101_TA_orig";

alter table if exists ckpi."cKPI101_TA_new" rename to "cKPI101_TA";

--ALTER TABLE ckpi."cKPI101_TA" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE ckpi."cKPI101_TA_orig" OWNER TO "taiho-dev-app-clinical-master-write";