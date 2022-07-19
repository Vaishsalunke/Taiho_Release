
select
	"Study",
	"SiteNumber",
	"Subject",
	"Study_Phase",
	"Timepoint",
	"Timepoint_STD",
	"Visit",
	"eCRF_PageName",
	"Dose_DtTm",
	"Lower Range Date",
	"Upper Range Date",
	"Sample_Collection_DtTm",
	"Range",
	"Outside_Range",
	case
		when "Outside_Range" = 'Yes' then
		case
			when "Lower Range Date" > "Sample_Collection_DtTm" then "Lower Range Date" - "Sample_Collection_DtTm"
			when "Upper Range Date" < "Sample_Collection_DtTm" then "Sample_Collection_DtTm" - "Upper Range Date"
		end
	end as "Number of Minutes Outside the Window",
	"MaxUpdated" as "Date Last Updated"
from
	(
	select
		distinct a."Study",
		a."SiteNumber",
		a."Subject",
		"Study_Phase",
		a."Timepoint",
		a."Timepoint_STD",
		a."Visit",
		a."eCRF_PageName",
		a."Dose_DtTm",
		case
			when a."Timepoint_STD" = '0' then (a."Dose_DtTm"-(interval '30 minutes'))
			when a."Timepoint_STD" = '0.5' then (a."Dose_DtTm"-(interval '10 minutes'))
			when a."Timepoint_STD" = '1' then (a."Dose_DtTm"-(interval '10 minutes'))
			when a."Timepoint_STD" = '2'
			and "Study_Phase" = 'Dose Escalation' then (a."Dose_DtTm"-(interval '15 minutes'))
			when a."Timepoint_STD" = '2'
			and "Study_Phase" = 'Dose Expansion' then (a."Dose_DtTm"-(interval '60 minutes'))
			when a."Timepoint_STD" = '3' then (a."Dose_DtTm"-(interval '20 minutes'))
			when a."Timepoint_STD" = '4' then (a."Dose_DtTm"-(interval '25 minutes'))
			when a."Timepoint_STD" = '6'
			and "Study_Phase" = 'Dose Escalation' then (a."Dose_DtTm"-(interval '35 minutes'))
			when a."Timepoint_STD" = '6'
			and "Study_Phase" = 'Dose Expansion' then (a."Dose_DtTm"-(interval '120 minutes'))
			when a."Timepoint_STD" = '8' then (a."Dose_DtTm"-(interval '45 minutes'))
			when a."Timepoint_STD" = '24' then (a."Dose_DtTm"-(interval '120 minutes'))
		end as "Lower Range Date",
		case
			when a."Timepoint_STD" = '0' then (a."Dose_DtTm")
			when a."Timepoint_STD" = '0.5' then (a."Dose_DtTm" +(interval '10 minutes'))
			when a."Timepoint_STD" = '1' then (a."Dose_DtTm" +(interval '10 minutes'))
			when a."Timepoint_STD" = '2'
			and "Study_Phase" = 'Dose Escalation' then (a."Dose_DtTm" +(interval '15 minutes'))
			when a."Timepoint_STD" = '2'
			and "Study_Phase" = 'Dose Expansion' then (a."Dose_DtTm" +(interval '60 minutes'))
			when a."Timepoint_STD" = '3' then (a."Dose_DtTm" +(interval '20 minutes'))
			when a."Timepoint_STD" = '4' then (a."Dose_DtTm" +(interval '25 minutes'))
			when a."Timepoint_STD" = '6'
			and "Study_Phase" = 'Dose Escalation' then (a."Dose_DtTm" +(interval '35 minutes'))
			when a."Timepoint_STD" = '6'
			and "Study_Phase" = 'Dose Expansion' then (a."Dose_DtTm" +(interval '120 minutes'))
			when a."Timepoint_STD" = '8' then (a."Dose_DtTm" +(interval '45 minutes'))
			when a."Timepoint_STD" = '24' then (a."Dose_DtTm" +(interval '120 minutes'))
		end as "Upper Range Date",
		a."Sample_Collection_DtTm",
		a."Range",
		case
			when a."Timepoint_STD" = '0'
			and (("Sample_Collection_DtTm" +(interval '30 minutes')) < "Dose_DtTm")
			or ("Sample_Collection_DtTm" >= "Dose_DtTm") then 'Yes'
			when a."Timepoint_STD" = '0.5'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '20 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '40 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '1'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '50 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '70 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '2'
			and "Study_Phase" = 'Dose Escalation'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '105 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '135 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '2'
			and "Study_Phase" = 'Dose Expansion'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '60 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '180 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '3'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '160 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '200 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '4'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '215 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '265 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '6'
			and "Study_Phase" = 'Dose Escalation'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '325 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '395 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '6'
			and "Study_Phase" = 'Dose Expansion'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '240 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '480 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '8'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '435 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '525 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '24'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '1320 minutes')))
			or ("Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '1560 minutes'))) then 'Yes'
			else 'No'
		end as "Outside_Range",
		"MaxUpdated"
	from
		(
		select
			distinct 'TAS0612_101' as "Study",
			'TAS0612_101_' || split_part(a."SiteNumber", '_', 2) as "SiteNumber",
			a."Subject",
			"PKBTMPT" as "Timepoint",
			case
				when "PKBTMPT_STD" = 'Predose' then '0'
				when "PKBTMPT_STD" = '30 Min' then '0.5'
				when "PKBTMPT_STD" = '1 Hour' then '1'
				when "PKBTMPT_STD" = '2 Hours' then '2'
				when "PKBTMPT_STD" = '3 Hours' then '3'
				when "PKBTMPT_STD" = '4 Hours' then '4'
				when "PKBTMPT_STD" = '6 Hours' then '6'
				when "PKBTMPT_STD" = '8 Hours' then '8'
				when "PKBTMPT_STD" = '24 Hours' then '24'
			end as "Timepoint_STD",
			a."InstanceName" as "Visit",
			a."DataPageName" as "eCRF_PageName",
			(("PKDOSDAT"::date)+("PKDOSTM"::time)) as "Dose_DtTm",
			(("PKBSCDAT"::date)+("PKBTM"::time)) as "Sample_Collection_DtTm",
			"TAPHASE" as "Study_Phase",
			case
				when "PKBTMPT_STD" = 'Predose' then '-30 min'
				when "PKBTMPT_STD" = '30 Min' then '+/-10 min'
				when "PKBTMPT_STD" = '1 Hour' then '+/-10 min'
				when "PKBTMPT_STD" = '2 Hours'
				and "TAPHASE" = 'Dose Escalation' then '+/-15 min'
				when "PKBTMPT_STD" = '2 Hours'
				and "TAPHASE" = 'Dose Expansion' then '+/-1 hr'
				when "PKBTMPT_STD" = '3 Hours' then '+/-20 min'
				when "PKBTMPT_STD" = '4 Hours' then '+/-25 min'
				when "PKBTMPT_STD" = '6 Hours'
				and "TAPHASE" = 'Dose Escalation' then '+/-35 min'
				when "PKBTMPT_STD" = '6 Hours'
				and "TAPHASE" = 'Dose Expansion' then '+/-2 hr'
				when "PKBTMPT_STD" = '8 Hours' then '+/-45 min'
				when "PKBTMPT_STD" = '24 Hours' then '+/-2 hr'
			end as "Range",
			a."MaxUpdated"
		from
			tas0612_101."PKBLD" a
		left join tas0612_101."TREAT" t on
			'TAS0612_101' = t.project
			and split_part(a."SiteNumber", '_', 2)= split_part(t."SiteNumber", '_', 2)
			and a."Subject" = t."Subject")a
union
	select
		distinct a."Study",
		a."SiteNumber",
		a."Subject",
		"Study_Phase",
		a."Timepoint",
		a."Timepoint_STD",
		a."Visit",
		a."eCRF_PageName",
		a."Dose_DtTm",
		case
			when a."Timepoint_STD" = '0' then (a."Dose_DtTm"-(interval '30 minutes'))
			when a."Timepoint_STD" = '0.5' then (a."Dose_DtTm"-(interval '10 minutes'))
			when a."Timepoint_STD" = '1' then (a."Dose_DtTm"-(interval '10 minutes'))
			when a."Timepoint_STD" = '2' then (a."Dose_DtTm"-(interval '15 minutes'))
			when a."Timepoint_STD" = '3' then (a."Dose_DtTm"-(interval '20 minutes'))
			when a."Timepoint_STD" = '4' then (a."Dose_DtTm"-(interval '25 minutes'))
			when a."Timepoint_STD" = '6' then (a."Dose_DtTm"-(interval '35 minutes'))
			when a."Timepoint_STD" = '8' then (a."Dose_DtTm"-(interval '45 minutes'))
			when a."Timepoint_STD" = '24' then (a."Dose_DtTm"-(interval '120 minutes'))
		end as "Lower Range Date",
		case
			when a."Timepoint_STD" = '0' then (a."Dose_DtTm")
			when a."Timepoint_STD" = '0.5' then (a."Dose_DtTm" +(interval '10 minutes'))
			when a."Timepoint_STD" = '1' then (a."Dose_DtTm" +(interval '10 minutes'))
			when a."Timepoint_STD" = '2' then (a."Dose_DtTm" +(interval '15 minutes'))
			when a."Timepoint_STD" = '3' then (a."Dose_DtTm" +(interval '20 minutes'))
			when a."Timepoint_STD" = '4' then (a."Dose_DtTm" +(interval '25 minutes'))
			when a."Timepoint_STD" = '6' then (a."Dose_DtTm" +(interval '35 minutes'))
			when a."Timepoint_STD" = '8' then (a."Dose_DtTm" +(interval '45 minutes'))
			when a."Timepoint_STD" = '24' then (a."Dose_DtTm" +(interval '120 minutes'))
		end as "Upper Range Date",
		a."Sample_Collection_DtTm",
		a."Range",
		case
			when a."Timepoint_STD" = '0'
			and (("Sample_Collection_DtTm" +(interval '30 minutes')) < "Dose_DtTm"
			or "Sample_Collection_DtTm" >= "Dose_DtTm") then 'Yes'
			when a."Timepoint_STD" = '0.5'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '20 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '40 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '1'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '50 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '70 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '2'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '105 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '135 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '3'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '160 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '200 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '4'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '215 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '265 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '6'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '325 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '395 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '8'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '435 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '525 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '24'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '1320 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '1560 minutes'))) then 'Yes'
			else 'No'
		end as "Outside_Range",
		"MaxUpdated"
	from
		(
		select
			distinct a."Study",
			a."SiteNumber",
			a."Subject",
			"TAPHASE" as "Study_Phase",
			a."Timepoint",
			a."Timepoint_STD",
			a."Visit",
			a."eCRF_PageName",
			case
				when a."Timepoint_STD" = '24' then b."Dose_Date"
				else "Dose_DtTm"
			end "Dose_DtTm",
			a."Sample_Collection_DtTm" ,
			a."Range",
			a."MaxUpdated"
		from
			(
			select
				'TAS0612_101' as "Study",
				'TAS0612_101_' || split_part("SiteNumber", '_', 2) as "SiteNumber",
				"Subject",
				"ECGTMPT" as "Timepoint",
				case
					when "ECGTMPT_STD" = 'Predose' then '0'
					when "ECGTMPT_STD" = '30 Min' then '0.5'
					when "ECGTMPT_STD" = '1 Hour' then '1'
					when "ECGTMPT_STD" = '2 Hours' then '2'
					when "ECGTMPT_STD" = '3 Hours' then '3'
					when "ECGTMPT_STD" = '4 Hours' then '4'
					when "ECGTMPT_STD" = '6 Hours' then '6'
					when "ECGTMPT_STD" = '8 Hours' then '8'
					when "ECGTMPT_STD" = '24 Hours' then '24'
				end as "Timepoint_STD",
				"InstanceName" as "Visit",
				"DataPageName" as "eCRF_PageName",
				(("ECGDAT"::date) + ("ECGDOSTM"::time)) as "Dose_DtTm",
				(("ECGDAT"::date) + ("ECGTM"::time)) as "Sample_Collection_DtTm",
				case
					when "ECGTMPT_STD" = 'Predose' then '-30 min'
					when "ECGTMPT_STD" = '30 Min' then '+/-10 min'
					when "ECGTMPT_STD" = '1 Hour' then '+/-10 min'
					when "ECGTMPT_STD" = '2 Hours' then '+/-15 min'
					when "ECGTMPT_STD" = '3 Hours' then '+/-20 min'
					when "ECGTMPT_STD" = '4 Hours' then '+/-25 min'
					when "ECGTMPT_STD" = '6 Hours' then '+/-35 min'
					when "ECGTMPT_STD" = '8 Hours' then '+/-45 min'
					when "ECGTMPT_STD" = '24 Hours' then '+/-2 hr'
				end as "Range",
				"MaxUpdated"
			from
				tas0612_101."ECG")a
		left join (
			select
				'TAS0612_101' as "Study",
				'TAS0612_101_' || split_part("SiteNumber", '_', 2) as "SiteNumber",
				"Subject",
				"ECGTMPT" as "Timepoint",
				"ECGTMPT_STD" as "Timepoint_STD",
				"InstanceName" as "Visit",
				(("ECGDAT"::date) + ("ECGDOSTM"::time)) as "Dose_Date"
			from
				tas0612_101."ECG"
			where
				"ECGTMPT_STD" = 'Predose') b on
			a."Study" = b."Study"
			and a."SiteNumber" = b."SiteNumber"
			and a."Subject" = b."Subject"
			and a."Visit" = b."Visit"
		left join tas0612_101."TREAT" t on
			'TAS0612_101' = t.project
			and split_part(a."SiteNumber", '_', 3)= split_part(t."SiteNumber", '_', 2)
			and a."Subject" = t."Subject" )a
union
	select
		distinct a."Study",
		a."SiteNumber",
		a."Subject",
		"Study_Phase",
		a."Timepoint",
		a."Timepoint_STD",
		a."Visit",
		a."eCRF_PageName",
		a."Dose_DtTm",
		case
			when a."Timepoint_STD" = '0' then (a."Dose_DtTm"-(interval '30 minutes'))
			when a."Timepoint_STD" = '0.5' then (a."Dose_DtTm"-(interval '10 minutes'))
			when a."Timepoint_STD" = '1' then (a."Dose_DtTm"-(interval '10 minutes'))
			when a."Timepoint_STD" = '2' then (a."Dose_DtTm"-(interval '15 minutes'))
			when a."Timepoint_STD" = '3' then (a."Dose_DtTm"-(interval '20 minutes'))
			when a."Timepoint_STD" = '4' then (a."Dose_DtTm"-(interval '25 minutes'))
			when a."Timepoint_STD" = '6' then (a."Dose_DtTm"-(interval '35 minutes'))
			when a."Timepoint_STD" = '8' then (a."Dose_DtTm"-(interval '45 minutes'))
			when a."Timepoint_STD" = '24' then (a."Dose_DtTm"-(interval '120 minutes'))
		end as "Lower Range Date",
		case
			when a."Timepoint_STD" = '0' then (a."Dose_DtTm")
			when a."Timepoint_STD" = '0.5' then (a."Dose_DtTm" +(interval '10 minutes'))
			when a."Timepoint_STD" = '1' then (a."Dose_DtTm" +(interval '10 minutes'))
			when a."Timepoint_STD" = '2' then (a."Dose_DtTm" +(interval '15 minutes'))
			when a."Timepoint_STD" = '3' then (a."Dose_DtTm" +(interval '20 minutes'))
			when a."Timepoint_STD" = '4' then (a."Dose_DtTm" +(interval '25 minutes'))
			when a."Timepoint_STD" = '6' then (a."Dose_DtTm" +(interval '35 minutes'))
			when a."Timepoint_STD" = '8' then (a."Dose_DtTm" +(interval '45 minutes'))
			when a."Timepoint_STD" = '24' then (a."Dose_DtTm" +(interval '120 minutes'))
		end as "Upper Range Date",
		a."Sample_Collection_DtTm",
		a."Range",
		case
			when a."Timepoint_STD" = '0'
			and (("Sample_Collection_DtTm" +(interval '30 minutes')) < "Dose_DtTm"
			or "Sample_Collection_DtTm" >= "Dose_DtTm") then 'Yes'
			when a."Timepoint_STD" = '0.5'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '20 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '40 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '1'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '50 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '70 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '2'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '105 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '135 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '3'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '160 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '200 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '4'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '215 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '265 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '6'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '325 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '395 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '8'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '435 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '525 minutes'))) then 'Yes'
			when a."Timepoint_STD" = '24'
			and ("Sample_Collection_DtTm" < ("Dose_DtTm" +(interval '1320 minutes'))
			or "Sample_Collection_DtTm" > ("Dose_DtTm" +(interval '1560 minutes'))) then 'Yes'
			else 'No'
		end as "Outside_Range",
		"MaxUpdated"
	from
		(
		select
			distinct a."Study",
			a."SiteNumber",
			a."Subject",
			"TAPHASE" as "Study_Phase",
			a."Timepoint",
			a."Timepoint_STD",
			a."Visit",
			a."eCRF_PageName",
			case
				when a."Timepoint_STD" = '24' then b."Dose_Date"
				else "Dose_DtTm"
			end "Dose_DtTm",
			a."Sample_Collection_DtTm",
			a."Range",
			a."MaxUpdated"
		from
			(
			select
				'TAS0612_101' as "Study",
				'TAS0612_101_' || split_part("SiteNumber", '_', 2) as "SiteNumber",
				"Subject",
				"PDBTMPT" as "Timepoint",
				case
					when "PDBTMPT_STD" = 'Predose' then '0'
					when "PDBTMPT_STD" = '30 Min' then '0.5'
					when "PDBTMPT_STD" = '1 Hour' then '1'
					when "PDBTMPT_STD" = '2 Hours' then '2'
					when "PDBTMPT_STD" = '3 Hours' then '3'
					when "PDBTMPT_STD" = '4 Hours' then '4'
					when "PDBTMPT_STD" = '6 Hours' then '6'
					when "PDBTMPT_STD" = '8 Hours' then '8'
					when "PDBTMPT_STD" = '24 Hours' then '24'
				end as "Timepoint_STD",
				"InstanceName" as "Visit",
				"DataPageName" as "eCRF_PageName",
				(("PDBSCDAT"::date) + ("PDBDSAD"::time)) as "Dose_DtTm",
				(("PDBSCDAT"::date) + ("PDBTM"::time)) as "Sample_Collection_DtTm",
				case
					when "PDBTMPT_STD" = 'Predose' then '-30 min'
					when "PDBTMPT_STD" = '30 Min' then '+/-10 min'
					when "PDBTMPT_STD" = '1 Hour' then '+/-10 min'
					when "PDBTMPT_STD" = '2 Hours' then '+/-15 min'
					when "PDBTMPT_STD" = '3 Hours' then '+/-20 min'
					when "PDBTMPT_STD" = '4 Hours' then '+/-25 min'
					when "PDBTMPT_STD" = '6 Hours' then '+/-35 min'
					when "PDBTMPT_STD" = '8 Hours' then '+/-45 min'
					when "PDBTMPT_STD" = '24 Hours' then '+/-2 hr'
				end as "Range",
				"MaxUpdated"
			from
				tas0612_101."PDBLD" )a
		left join (
			select
				'TAS0612_101' as "Study",
				'TAS0612_101_' || split_part("SiteNumber", '_', 2) as "SiteNumber",
				"Subject",
				"PDBTMPT" as "Timepoint",
				"PDBTMPT_STD" as "Timepoint_STD",
				"InstanceName" as "Visit",
				(("PDBSCDAT"::date) + ("PDBDSAD"::time)) as "Dose_Date"
			from
				tas0612_101."PDBLD"
			where
				"PDBTMPT_STD" = 'Predose') b on
			a."Study" = b."Study"
			and a."SiteNumber" = b."SiteNumber"
			and a."Subject" = b."Subject"
			and a."Visit" = b."Visit"
		left join tas0612_101."TREAT" t on
			'TAS0612_101' = t.project
			and split_part(a."SiteNumber", '_', 3)= split_part(t."SiteNumber", '_', 2)
			and a."Subject" = t."Subject" )a )b
			