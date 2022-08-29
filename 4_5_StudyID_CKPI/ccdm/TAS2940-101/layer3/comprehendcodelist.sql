/*
CCDM comprehendcodelist mapping
Notes: The Code/value pairs here can be used to configure the client  
Avaiilable Configuration Options
Code                Value                       Description
SUBJECT_DAY_START   DSTERM value                e.g. consented/enrolled Specifies which disposition event to use for the subject start date
VISIT_COMPLIANCE    "hide first"/"show first"   e.g. "hide first"/"show first" Specifies if first visist has to be taken in to consideration for subject days calculation
INCLUDED_TABLES     USDM object                 e.g. comma seperated USDM objects present in this field determines if that object is expected to have data or not
*/


WITH included_studies AS (SELECT studyid FROM study),
included_tables AS (select array[
                                        'study',
                                        'studycro',
                                        'site',
										'subject',
                                        'comprehendcodelist',
                                        'comprehendtermmap',
                                        'comprehendeventlog',
                                        'ae',
                                        'ds',
                                        'fielddef',
                                        'formdef',
                                        'query',
                                        'siteissue',
                                        'sitemilestone',
                                        'sitemonitoringvisit',
                                        'sitemonitoringvisitschedule',
                                        'siteresource',
                                        'studymilestone',
                                        'studyplannedenrollment',
										'cm',
										'mh',
										'vs',
										'ie',
										'dm',
										'fielddata',
										'formdata',
										'eg',
										'pe',
                                        'tv',
										'ex',
										'sv',
                                        'rpt_ae_rate_by_subject_days',
                                        'rpt_ae_rel_week',
                                        'rpt_ae_study_baseline',
                                        'rpt_ds',
                                        'rpt_enrollment_analytics',
                                        'rpt_latest_ds',
                                        'rpt_monitoring_visit_record',
										'rpt_ae_rate_by_subject_days_per_month',
                                        'rpt_open_query_age',
                                        'rpt_pd_rate_by_subject_days',
										'rpt_portfolio_summary_analytics',
                                        'rpt_pivotal_study_analytics',
                                        'rpt_portfolio_oversight',
                                        'rpt_query_rate_by_subject_days',
                                        'rpt_resource_analytics',
                                        'rpt_site_performance',
                                        'rpt_study_risk_analytics',
                                        'rpt_subject_days',
                                        'rpt_subject_information',
                                        'rpttab_query',
                                        'rpt_site_days',
                                        'rpt_pd_rate_by_subject_days_per_month',
										'rpt_oversight_metrics',
                                        'rpt_study_cro_scores',
                                        'rpt_site_cro_scores',
										'rpt_subject_disposition',
                                        'dimdate',
                                        'dimdisposition',
                                        'dimsite',
                                        'dimstudy',
                                        'dimstudymilestone',
                                        'dimsubject',
                                        'dimsitedates',
                                        'factadverseevents',
                                        'factportfoliodaily',
										'rpt_data_entry',
										'rpt_site_metrics',
										'rpt_missing_data',
										'rpt_country_metrics',
										'rpttab_fielddata',
										'rpt_subject_visit_schedule_last_visit',
										'rpt_view_subj_count_by_last_visit',
										'factsiteperformance',
                                        'kpisummary'
                                    ] tab),

        code_value_pairs AS (
                SELECT  'default'::text AS codekey,
                        'SUBJECT_DAY_START'::text AS codename,
                        'enrolled'::text AS codevalue
                UNION ALL
                SELECT  'default'::text AS codekey,
                        'VISIT_COMPLIANCE'::text AS codename,
                        'show first'::text AS codevalue
                UNION ALL
                SELECT
                        'default'::text AS codekey,
                        'INCLUDED_TABLES'::text AS codename,
                        string_agg(tabstr, ',')::text AS codevalue
                FROM (SELECT unnest(tab) tabstr from included_tables) t
		UNION ALL
		SELECT  
			'default'::text AS codekey,
			'INCLUDED_TABLES_SOFT_CHECK'::text AS codename,
			'true'::text AS codevalue)

SELECT s.studyid::text AS codekey,
       cvp.codename::text AS codename,
       cvp.codevalue::text AS codevalue
FROM
code_value_pairs cvp CROSS JOIN included_studies s;

