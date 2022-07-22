-- Swimmer plot

DROP TABLE ckpi.ckpi08_swim_plot
;
CREATE TABLE ckpi.ckpi08_swim_plot (
    id serial PRIMARY KEY,
	study VARCHAR(250) NOT NULL,
	sitenumber VARCHAR(250) NOT NULL,
	subject VARCHAR(250) NOT NULL,
	cohort text NULL,
	cancer_diag text NULL,
	wks_frm_c1d1 SMALLINT NULL,
	wks_frm_c1d1_surv_con SMALLINT NULL,
	cum_wks_frm_c1d1_surv_con SMALLINT NULL,
	wks_frm_c1d1_surv_uncon SMALLINT NULL,
	cum_wks_frm_c1d1_surv_uncon SMALLINT NULL,
	eot_reason text NULL,
	eot_reason_updtd text NULL,
	eot_frm_c1d1_wks int4 NULL,
	pr_wks_from_c1d1 text NULL,
	cr_wks_from_c1d1 text NULL,
	death_frm_c1d1_wks int4 NULL,
	last_date_of_surv timestamp NULL,
	date_of_death timestamp NULL
);

INSERT INTO ckpi.ckpi08_swim_plot (
with swmr1 as (
select
	study
	, sitenumber
	, subject
	, cohort
	, cancer_diag
	, eot_reason
	, weeks_from_c1d1
    , ((date_of_or::date - c1d1_date_of_visit::date) + 1) / 7 as pr_weeks_from_c1d1
	, case
		when
			eot_reason in ('Radiological Progression', 'Clinical Disease Progression', 'Physician decision including need for other anticancer therapy not specified in the protocol or surgery or radiotherapy to the only sites of disease being followed in the study', 'Adverse Event / SAE')
		then ((date_of_trt_disc_met::date - c1d1_date_of_visit::date) + 1) / 7
		else null end
	as eot_frm_c1d1_wks  
	, case when date_of_death is not null then ((date_of_death::date - c1d1_date_of_visit::date) + 1) / 7 else null end as death_frm_c1d1_wks
	, weeks_from_c1d1_surv_con
	, weeks_from_c1d1_surv_uncon
	, last_date_of_surv	
	, date_of_death
	, overall_respnse
from
	ckpi.ckpi07_swimp 
)
select
	nextval('ckpi.ckpi08_swim_plot_id_seq'::regclass)
	, study
	, sitenumber
	, subject
	, cohort
	, cancer_diag
	, weeks_from_c1d1 as wks_frm_c1d1 
	, weeks_from_c1d1_surv_con as wks_frm_c1d1_surv_con
	, weeks_from_c1d1 + COALESCE(weeks_from_c1d1_surv_con, 0) as cum_wks_frm_c1d1_surv_con 
	, weeks_from_c1d1_surv_uncon as wks_frm_c1d1_surv_uncon
	, weeks_from_c1d1 + COALESCE(weeks_from_c1d1_surv_con, 0) + COALESCE(weeks_from_c1d1_surv_uncon, 0) as cum_wks_frm_c1d1_surv_uncon
	, eot_reason
	, case when eot_reason not in ('Ongoing', 'Radiological Progression', 'Clinical Disease Progression', 'Death') then 'Discontinuation (Other)' else eot_reason end as eot_reason_updtd
	, eot_frm_c1d1_wks
	, case when overall_respnse = 'PR' then string_agg(pr_weeks_from_c1d1::text,',') else null end as pr_wks_from_c1d1
	, case when overall_respnse = 'CR' then string_agg(pr_weeks_from_c1d1::text,',') else null end as cr_wks_from_c1d1
	, death_frm_c1d1_wks
	, last_date_of_surv
	, date_of_death
from swmr1
group by study
, sitenumber, subject, cohort, cancer_diag
, eot_reason
,case when eot_reason not in ('Ongoing', 'Radiological Progression', 'Clinical Disease Progression', 'Death') then 'Discontinuation (Other)' else eot_reason end
,weeks_from_c1d1, eot_frm_c1d1_wks, death_frm_c1d1_wks, weeks_from_c1d1_surv_con, weeks_from_c1d1_surv_uncon, last_date_of_surv, date_of_death, overall_respnse
ORDER BY study, sitenumber, subject
)
;