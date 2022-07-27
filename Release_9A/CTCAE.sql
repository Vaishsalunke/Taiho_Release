/*Mapping script for Cancer Diagnosis - Listing
Table name : "ctable_listing"."ctcae_listing"
Project name : Taiho*/
create schema if not exists ctable_listing;

drop table if exists ctable_listing.ctcae_listing;

create table ctable_listing.ctcae_listing as select * from
			( with ae as (
			select
				distinct ae.studyid,
				ae.usubjid,
				ae.aeterm,
				ae.aeverbatim,
				ae.preferredterm,
				aetx.faorres aesev,
				aetx.fastdtc aestdtc,
				aetx.faendtc aeendtc
			from
				cqs.ae
			left join cqs.ae_sev_toxgr aetx on
				ae.studyid = aetx.studyid
				and aetx.usubjid = ae.usubjid
				and ae.aeterm = aetx.aeterm
				and ae.aestdtc = aetx.fastdtc )
			select
				distinct a.studyid,
				a.usubjid,
				a.lbcat,
				a.lbtest,
				a.lbtestcd,
				a.visit,
				a.lbdtc,
				a.lbstresn,
				a.lbstresu,
				a.lbstnrhi,
				a.lbstnrlo,
				a.lbtox,
				a.result,
				a.bl_lbstresn_1 as bl_lbstresn,
				ae.aeterm,
				trim(ae.aeverbatim) as aeverbatim,
				ae.preferredterm,
				ae.aesev,
				ae.aestdtc,
				ae.aeendtc,
				row_number() over (partition by a.studyid, a.usubjid,a.lbtest,a.visit,a.lbdtc ,a.result,a.lbstresn order by
				ae.aeterm ) as rnk
			from
				( with min_baseline as (
				select
					studyid,
					usubjid,
					lbtestcd,
					lbblfl,
					lbdtc as min_lbdtc,
					lbseq
				from
					cqs.rpt_lab_information lb
				where
					lbblfl = 'Yes' ) 
					
				,bl_val as (
				select
					k.*,
					Case when lb.lbstnrhi is not null and lb.lbstnrlo is not NULL
					then
						case
							when (k.bl_lbstresn > lb.lbstnrhi or k.bl_lbstresn < lb.lbstnrlo) 
								then 'abnormal'
							when k.bl_lbstresn is null then null
						else 'normal'
						end
		end as "result"
				from
					(
					select
						lb.studyid,
						lb.usubjid,
						lb.lbtestcd,
						min_lbdtc,
						lb.lbstresn as bl_lbstresn,
						min_baseline.lbseq
					from
						min_baseline
					left join cqs.rpt_lab_information lb on
						lb.studyid = min_baseline.studyid
						and lb.usubjid = min_baseline.usubjid
						and lb.lbtestcd = min_baseline.lbtestcd
						and lb.lbdtc = min_baseline.min_lbdtc 
						and lb.lbseq = min_baseline.lbseq
						)k
				left join cqs.rpt_lab_information lb on
					lb.studyid = k.studyid
					and lb.usubjid = k.usubjid
					and lb.lbtestcd = k.lbtestcd
					and lb.lbdtc = k.min_lbdtc
					and lb.lbstresn = k.bl_lbstresn
					and lb.lbseq = k.lbseq
										) 
										
			,result_abromal_normal as (
				select
					lb.*,
					a.result
					,a.bl_lbstresn as bl_lbstresn_1

					from cqs.rpt_lab_information lb
				left join (
						select distinct studyid,
						usubjid,
						lbtestcd,
						result
						,bl_lbstresn
					from
						bl_val
					--where result is not null
						) a on
					lb.studyid = a.studyid
					and lb.usubjid = a.usubjid
					and lb.lbtestcd = a.lbtestcd ) 
				,baseline as (
				select
					distinct lb.*
					--,bl_val.bl_lbstresn
				from
					result_abromal_normal lb
				left join bl_val on
					lb.studyid = bl_val.studyid
					and lb.usubjid = bl_val.usubjid
					and lb.lbtestcd = bl_val.lbtestcd
					--and lb.lbstresn = bl_val.bl_lbstresn
					),
					
				aeterm as ( (
				select
					distinct studyid,
					lbtestcd test,
					case
						when lbtestcd = 'APTT' then 'Activated partial thromboplastin time prolonged'
						when lbtestcd = 'FIBRINO' then 'Fibrinogen decreased'
						when lbtestcd = 'INR' then 'INR increased'
						when lbtestcd = 'HGB' then 'Hemoglobin increased'
						--when lbtestcd = 'HGB' then 'Anemia'
						when lbtestcd = 'EOS' then 'Eosinophilia'
						when lbtestcd = 'NEUT' then 'Neutrophil count decreased'
						when lbtestcd = 'LYM' then 'Lymphocyte count increased'
						--when lbtestcd = 'LYM' then 'Lymphocyte count decreased'
						when lbtestcd = 'PLAT' then 'Platelet count decreased'
						when lbtestcd = 'WBC' then 'Leukocytosis'
						--when lbtestcd = 'WBC' then 'White blood cell decreased'
						when lbtestcd = 'ALB' then 'Hypoalbuminemia'
						when lbtestcd = 'ALP' then 'Alkaline phosphatase increased'
						when lbtestcd = 'ALT' then 'Alanine aminotransferase increased'
						when lbtestcd = 'AST' then 'Aspartate aminotransferase increased'
						when lbtestcd = 'BICARB' then 'Blood bicarbonate decreased'
						when lbtestcd = 'BILI' then 'Blood bilirubin increased'
						when lbtestcd = 'LDH' then 'Blood lactate dehydrogenase increased'
						when lbtestcd = 'LIPASET' then 'Lipase increased'
						--when lbtestcd = 'LIPASE' then 'Lipase increased'
						when lbtestcd = 'CK' then 'CPK increased'
						when lbtestcd = 'CREAT' then 'Creatinine increased'
						when lbtestcd = 'CHOL' then 'Cholesterol high'
						when lbtestcd = 'GGT' then 'GGT increased'
						when lbtestcd = 'AMYLASE' then 'serum amylase increased'
						when lbtestcd = 'CACR' then 'Hypercalcemia'
						--when lbtestcd = 'CACR' then 'Hypocalcemia'
						--when lbtestcd = 'GLUC' then 'Hyperglycemia' --Asked to remove
						when lbtestcd = 'GLUC' then 'Hypoglycemia'
						when lbtestcd = 'K' then 'Hyperkalemia'
						--when lbtestcd = 'K' then 'Hypokalemia'
						when lbtestcd = 'MG' then 'Hypermagnesemia'
						--when lbtestcd = 'MG' then 'Hypomagnesemia'
						when lbtestcd = 'SODIUM' then 'Hypernatremia'
						--when lbtestcd = 'SODIUM' then 'Hyponatremia'
						when lbtestcd = 'TRIG' then 'Hypertriglyceridemia'
						when lbtestcd = 'PHOS' then 'Hyperphosphatemia'
						--when lbtestcd = 'PHOS' then 'Hypophosphatemia' --Asked to remove
						when lbtestcd = 'URATE' then 'Hyperuricemia'
						else null
					end as "aeterm"
				from
					cqs.rpt_lab_information lb)
		union all (
			select
				distinct studyid,
				lbtestcd test,
				case
					when lbtestcd = 'APTT' then 'Activated partial thromboplastin time prolonged'
					when lbtestcd = 'FIBRINO' then 'Fibrinogen decreased'
					when lbtestcd = 'INR' then 'INR increased'
					when lbtestcd = 'HGB' then 'Anemia'
					--when lbtestcd = 'HGB' then 'Hemoglobin increased'
					when lbtestcd = 'EOS' then 'Eosinophilia'
					when lbtestcd = 'NEUT' then 'Neutrophil count decreased'
					when lbtestcd = 'LYM' then 'Lymphocyte count decreased'
					--when lbtestcd = 'LYM' then 'Lymphocyte count increased'
					when lbtestcd = 'PLAT' then 'Platelet count decreased'
					when lbtestcd = 'WBC' then 'White blood cell decreased'
					--when lbtestcd = 'WBC' then 'Leukocytosis'
					when lbtestcd = 'ALB' then 'Hypoalbuminemia'
					when lbtestcd = 'ALP' then 'Alkaline phosphatase increased'
					when lbtestcd = 'ALT' then 'Alanine aminotransferase increased'
					when lbtestcd = 'AST' then 'Aspartate aminotransferase increased'
					when lbtestcd = 'BICARB' then 'Blood bicarbonate decreased'
					when lbtestcd = 'BILI' then 'Blood bilirubin increased'
					when lbtestcd = 'LDH' then 'Blood lactate dehydrogenase increased'
					when lbtestcd = 'LIPASET' then 'Lipase increased'
					--when lbtestcd = 'LIPASE' then 'Lipase increased'
					when lbtestcd = 'CK' then 'CPK increased'
					when lbtestcd = 'CREAT' then 'Creatinine increased'
					when lbtestcd = 'CHOL' then 'Cholesterol high'
					when lbtestcd = 'GGT' then 'GGT increased'
					when lbtestcd = 'AMYLASE' then 'serum amylase increased'
					when lbtestcd = 'CACR' then 'Hypocalcemia'
					--when lbtestcd = 'CACR' then 'Hypercalcemia'
					when lbtestcd = 'GLUC' then 'Hypoglycemia'
					--when lbtestcd = 'GLUC' then 'Hyperglycemia' --Asked to remove
					when lbtestcd = 'K' then 'Hypokalemia'
					--when lbtestcd = 'K' then 'Hyperkalemia'
					when lbtestcd = 'MG' then 'Hypomagnesemia'
					--when lbtestcd = 'MG' then 'Hypermagnesemia'
					when lbtestcd = 'SODIUM' then 'Hyponatremia'
					--when lbtestcd = 'SODIUM' then 'Hypernatremia'
					when lbtestcd = 'TRIG' then 'Hypertriglyceridemia'
					--when lbtestcd = 'PHOS' then 'Hypophosphatemia' --Asked to remove
					when lbtestcd = 'PHOS' then 'Hyperphosphatemia'
					when lbtestcd = 'URATE' then 'Hyperuricemia'
					else null
				end as "aeterm"
			from
				cqs.rpt_lab_information lb) )
				select
					distinct baseline.studyid,
					usubjid,
					lbcat,
					lbtest,
					lbtestcd,
					lbdtc,
					lbstresn,
					lbstresu,
					lbstnrhi,
					lbstnrlo,
					case
						when lbtestcd = 'APTT'
						and (lbstresn > lbstnrhi
						and lbstresn <= 1.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'APTT'
						and (lbstresn > (1.5 * lbstnrhi)
						and lbstresn <= 2.5 * lbstnrhi) then 'G2'
						when lbtestcd = 'APTT'
						and lbstresn > 2.5 * lbstnrhi then 'G3'
						when lbtestcd = 'ALB'
						and lbstresu = 'g/dL'
						and (lbstresn < lbstnrlo
						and lbstresn >= 3) then 'G1'
						when lbtestcd = 'ALB'
						and lbstresu = 'g/dL'
						and (lbstresn < 3
						and lbstresn >= 2) then 'G2'
						when lbtestcd = 'ALB'
						and lbstresu = 'g/dL'
						and lbstresn < 2 then 'G3'
						when lbtestcd = 'ALB'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn < lbstnrlo
						and lbstresn >= 30) then 'G1'
						when lbtestcd = 'ALB'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn < 30
						and lbstresn >= 20) then 'G2'
						when lbtestcd = 'ALB'
						and lbstresu in ('g/L',
						'g/l')
						and lbstresn < 20 then 'G3'
						when lbtestcd = 'K'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < lbstnrlo
						and lbstresn >= 3.0) then 'G1'
						when lbtestcd = 'K'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 3.0
						and lbstresn >= 2.5) then 'G3'
						when lbtestcd = 'K'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and lbstresn < 2.5 then 'G4'
						when lbtestcd = 'K'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > lbstnrhi
						and lbstresn <= 5.5) then 'G1' /*when lbtestcd = 'K'
			and lbstresu in ('mmol/L', 'mmol/l')
			and (lbstresn > 5.5
				and lbstresn <= 6.0)
			then 'G2'*/
						when lbtestcd = 'K'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 6.0
						and lbstresn <= 7.0) then 'G3'
						when lbtestcd = 'K'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and lbstresn > 7.0 then 'G4'
						when lbtestcd = 'ALP'
						and baseline.result = 'normal'
						and (lbstresn > lbstnrhi
						and lbstresn <= 2.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'ALP'
						and baseline.result = 'normal'
						and (lbstresn > 2.5 * lbstnrhi
						and lbstresn <= 5 * lbstnrhi) then 'G2'
						when lbtestcd = 'ALP'
						and baseline.result = 'normal'
						and (lbstresn > 5 * lbstnrhi
						and lbstresn <= 20 * lbstnrhi) then 'G3'
						when lbtestcd = 'ALP'
						and baseline.result = 'normal'
						and (lbstresn > 20 * lbstnrhi) then 'G4'
						when lbtestcd = 'ALP'
						and baseline.result = 'abnormal'
						and (lbstresn >= 2 * baseline.bl_lbstresn_1
						and lbstresn <= 2.5 * baseline.bl_lbstresn_1) then 'G1'
						when lbtestcd = 'ALP'
						and baseline.result = 'abnormal'
						and (lbstresn >= 2.5 * baseline.bl_lbstresn_1
						and lbstresn <= 5 * baseline.bl_lbstresn_1) then 'G2'
						when lbtestcd = 'ALP'
						and baseline.result = 'abnormal'
						and (lbstresn >= 5 * baseline.bl_lbstresn_1
						and lbstresn <= 20 * baseline.bl_lbstresn_1) then 'G3'
						when lbtestcd = 'ALP'
						and baseline.result = 'abnormal'
						and (lbstresn > 20 * baseline.bl_lbstresn_1) then 'G4'
						when lbtestcd = 'ALT'
						and baseline.result = 'normal'
						and (lbstresn > lbstnrhi
						and lbstresn <= 3 * lbstnrhi) then 'G1'
						when lbtestcd = 'ALT'
						and baseline.result = 'normal'
						and (lbstresn > 3 * lbstnrhi
						and lbstresn <= 5 * lbstnrhi) then 'G2'
						when lbtestcd = 'ALT'
						and baseline.result = 'normal'
						and (lbstresn > 5 * lbstnrhi
						and lbstresn <= 20 * lbstnrhi) then 'G3'
						when lbtestcd = 'ALT'
						and baseline.result = 'normal'
						and (lbstresn > 20 * lbstnrhi) then 'G4'
						when lbtestcd = 'ALT'
						and baseline.result = 'abnormal'
						and (lbstresn >= 1.5 * baseline.bl_lbstresn_1
						and lbstresn <= 3 * baseline.bl_lbstresn_1) then 'G1'
						when lbtestcd = 'ALT'
						and baseline.result = 'abnormal'
						and (lbstresn >= 3 * baseline.bl_lbstresn_1
						and lbstresn <= 5 * baseline.bl_lbstresn_1) then 'G2'
						when lbtestcd = 'ALT'
						and baseline.result = 'abnormal'
						and (lbstresn >= 5 * baseline.bl_lbstresn_1
						and lbstresn <= 20 * baseline.bl_lbstresn_1) then 'G3'
						when lbtestcd = 'ALT'
						and baseline.result = 'abnormal'
						and (lbstresn > 20 * baseline.bl_lbstresn_1) then 'G4'
						when lbtestcd = 'BILI'
						and baseline.result = 'normal'
						and (lbstresn > lbstnrhi
						and lbstresn <= 1.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'BILI'
						and baseline.result = 'normal'
						and (lbstresn > 1.5 * lbstnrhi
						and lbstresn <= 3 * lbstnrhi) then 'G2'
						when lbtestcd = 'BILI'
						and baseline.result = 'normal'
						and (lbstresn > 3 * lbstnrhi
						and lbstresn <= 10 * lbstnrhi) then 'G3'
						when lbtestcd = 'BILI'
						and baseline.result = 'normal'
						and (lbstresn > 10 * lbstnrhi) then 'G4'
						when lbtestcd = 'BILI'
						and baseline.result = 'abnormal'
						and (lbstresn >= 1 * baseline.bl_lbstresn_1
						and lbstresn <= 1.5 * baseline.bl_lbstresn_1) then 'G1'
						when lbtestcd = 'BILI'
						and baseline.result = 'abnormal'
						and (lbstresn >= 1.5 * baseline.bl_lbstresn_1
						and lbstresn <= 3 * baseline.bl_lbstresn_1) then 'G2'
						when lbtestcd = 'BILI'
						and baseline.result = 'abnormal'
						and (lbstresn >= 3 * baseline.bl_lbstresn_1
						and lbstresn <= 10 * baseline.bl_lbstresn_1) then 'G3'
						when lbtestcd = 'BILI'
						and baseline.result = 'abnormal'
						and (lbstresn > 10 * baseline.bl_lbstresn_1) then 'G4'
						when lbtestcd = 'AST'
						and baseline.result = 'normal'
						and (lbstresn > lbstnrhi
						and lbstresn <= 3 * lbstnrhi) then 'G1'
						when lbtestcd = 'AST'
						and baseline.result = 'normal'
						and (lbstresn > 3 * lbstnrhi
						and lbstresn <= 5 * lbstnrhi) then 'G2'
						when lbtestcd = 'AST'
						and baseline.result = 'normal'
						and (lbstresn > 5 * lbstnrhi
						and lbstresn <= 20 * lbstnrhi) then 'G3'
						when lbtestcd = 'AST'
						and baseline.result = 'normal'
						and (lbstresn > 20 * lbstnrhi) then 'G4'
						when lbtestcd = 'AST'
						and baseline.result = 'abnormal'
						and (lbstresn >= 1.5 * baseline.bl_lbstresn_1
						and lbstresn <= 3 * baseline.bl_lbstresn_1) then 'G1'
						when lbtestcd = 'AST'
						and baseline.result = 'abnormal'
						and (lbstresn >= 3 * baseline.bl_lbstresn_1
						and lbstresn <= 5 * baseline.bl_lbstresn_1) then 'G2'
						when lbtestcd = 'AST'
						and baseline.result = 'abnormal'
						and (lbstresn >= 5 * baseline.bl_lbstresn_1
						and lbstresn <= 20 * baseline.bl_lbstresn_1) then 'G3'
						when lbtestcd = 'AST'
						and baseline.result = 'abnormal'
						and (lbstresn > 20 * baseline.bl_lbstresn_1) then 'G4'
						when lbtestcd = 'AMYLASE'
						and (lbstresn > lbstnrhi
						and lbstresn <= 1.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'AMYLASE'
						and (lbstresn > 1.5 * lbstnrhi
						and lbstresn <= 2 * lbstnrhi) then 'G2'
						when lbtestcd = 'BICARB'
						and (lbstresn < lbstnrlo) then 'G1'
						when lbtestcd = 'CK'
						and (lbstresn > lbstnrhi
						and lbstresn <= 2.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'CK'
						and (lbstresn > 2.5 * lbstnrhi
						and lbstresn <= 5 * lbstnrhi) then 'G2'
						when lbtestcd = 'CK'
						and (lbstresn >= 5 * lbstnrhi
						and lbstresn <= 10 * lbstnrhi) then 'G3'
						when lbtestcd = 'CK'
						and (lbstresn > 10 * lbstnrhi) then 'G4'
						when lbtestcd = 'EOS'
						and baseline.result = 'normal'
						and (lbstresn > lbstnrhi) then 'G1'
						when lbtestcd = 'EOS'
						and baseline.result = 'abnormal'
						and (lbstresn > baseline.bl_lbstresn_1) then 'G1'
						when lbtestcd = 'INR'
						and (lbstresn >= 1.2
						and lbstresn <= 1.5) then 'G1'
						when lbtestcd = 'INR'
						and (lbstresn >= 1.5
						and lbstresn <= 2.5) then 'G2'
						when lbtestcd = 'INR'
						and (lbstresn >= 2.5) then 'G3'
						when lbtestcd = 'LDH'
						and (lbstresn > lbstnrhi) then 'G1'
						when lbtestcd = 'LIPASET'
						and (lbstresn > lbstnrhi
						and lbstresn <= 1.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'LIPASET'
						and (lbstresn > 1.5 * lbstnrhi
						and lbstresn <= 2 * lbstnrhi) then 'G2'
						when lbtestcd = 'SODIUM'
						and (lbstresn > lbstnrhi
						and lbstresn <= 150) then 'G1' /*when lbtestcd = 'SODIUM'
			and (lbstresn > 150
				and lbstresn <= 155)
			then 'G2'
			when lbtestcd = 'SODIUM'
			and (lbstresn > 155
				and lbstresn <= 160)
			then 'G3'*/
						when lbtestcd = 'SODIUM'
						and (lbstresn > 160) then 'G4'
						when lbtestcd = 'SODIUM'
						and (lbstresn < lbstnrlo
						and lbstresn >= 130) then 'G1'
						when lbtestcd = 'SODIUM'
						and (lbstresn < 120) then 'G4'
						when lbtestcd = 'WBC'
						and (lbstresn > 100000) then 'G3'
						when lbtestcd = 'WBC'
						and lbstresu = 'x10^9/L'
						and (lbstresn < lbstnrlo
						and lbstresn >= 3) then 'G1'
						when lbtestcd = 'WBC'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 3
						and lbstresn >= 2) then 'G2'
						when lbtestcd = 'WBC'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 2
						and lbstresn >= 1) then 'G3'
						when lbtestcd = 'WBC'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 1) then 'G4'
						when lbtestcd = 'WBC'
						and lbstresu = 'mm3'
						and (lbstresn < lbstnrlo
						and lbstresn >= 3000) then 'G1'
						when lbtestcd = 'WBC'
						and lbstresu = 'mm3'
						and (lbstresn < 3000
						and lbstresn >= 2000) then 'G2'
						when lbtestcd = 'WBC'
						and lbstresu = 'mm3'
						and (lbstresn < 2000
						and lbstresn >= 1000) then 'G3'
						when lbtestcd = 'WBC'
						and lbstresu = 'mm3'
						and (lbstresn < 1000) then 'G4'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn > lbstnrhi
						and lbstresn <= 11.5) then 'G1'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn > 11.5
						and lbstresn <= 12.5) then 'G2'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn > 12.5
						and lbstresn <= 13.5) then 'G3'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn > 13.5) then 'G4'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > lbstnrhi
						and lbstresn <= 2.9) then 'G1'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 2.9
						and lbstresn <= 3.1) then 'G2'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 3.1
						and lbstresn <= 3.4) then 'G3'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 3.4) then 'G4'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < lbstnrlo
						and lbstresn >= 2) then 'G1'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 2
						and lbstresn >= 1.75) then 'G2'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 1.75
						and lbstresn >= 1.5) then 'G3'
						when lbtestcd = 'CACR'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 1.5) then 'G4'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn < lbstnrlo
						and lbstresn >= 8) then 'G1'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn < 8
						and lbstresn >= 7) then 'G2'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn < 7
						and lbstresn >= 6) then 'G3'
						when lbtestcd = 'CACR'
						and lbstresu = 'mg/dL'
						and (lbstresn < 6) then 'G4'
						when lbtestcd = 'CHOL'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > lbstnrhi
						and lbstresn <= 7.75) then 'G1'
						when lbtestcd = 'CHOL'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 7.75
						and lbstresn <= 10.34) then 'G2'
						when lbtestcd = 'CHOL'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 10.34
						and lbstresn <= 12.92) then 'G3'
						when lbtestcd = 'CHOL'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 12.92) then 'G4'
						when lbtestcd = 'CHOL'
						and lbstresu = 'mg/dL'
						and (lbstresn > lbstnrhi
						and lbstresn <= 300) then 'G1'
						when lbtestcd = 'CHOL'
						and lbstresu = 'mg/dL'
						and (lbstresn > 300
						and lbstresn <= 400) then 'G2'
						when lbtestcd = 'CHOL'
						and lbstresu = 'mg/dL'
						and (lbstresn > 400
						and lbstresn <= 500) then 'G3'
						when lbtestcd = 'CHOL'
						and lbstresu = 'mg/dL'
						and (lbstresn > 500) then 'G4'
						when lbtestcd = 'CREAT'
						and (lbstresn > lbstnrhi
						and lbstresn <= 1.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'CREAT'
						and baseline.result = 'normal'
						and (lbstresn > 1.5 * lbstnrhi
						and lbstresn <= 3 * lbstnrhi) then 'G2'
						when lbtestcd = 'CREAT'
						and baseline.result = 'normal'
						and (lbstresn > 3 * lbstnrhi
						and lbstresn <= 6 * lbstnrhi) then 'G3'
						when lbtestcd = 'CREAT'
						and (lbstresn > 6 * lbstnrhi) then 'G4'
						when lbtestcd = 'CREAT'
						and baseline.result = 'abnormal'
						and (lbstresn > 1.5 * baseline.bl_lbstresn_1
						and lbstresn <= 3 * baseline.bl_lbstresn_1) then 'G2'
						when lbtestcd = 'CREAT'
						and baseline.result = 'abnormal'
						and (lbstresn >= 3 * baseline.bl_lbstresn_1) then 'G3'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'normal'
						and (lbstresn < 1 * lbstnrlo
						and lbstresn >= 0.75 * lbstnrlo) then 'G1'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'normal'
						and (lbstresn < 0.75 * lbstnrlo
						and lbstresn >= 0.5 * lbstnrlo) then 'G2'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'normal'
						and (lbstresn < 0.5 * lbstnrlo
						and lbstresn >= 0.25 * lbstnrlo) then 'G3'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'normal'
						and (lbstresn < 0.25 * lbstnrlo) then 'G4'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'abnormal'
						and (lbstresn < 1 * baseline.bl_lbstresn_1
						and lbstresn >= 0.75 * baseline.bl_lbstresn_1) then 'G1'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'abnormal'
						and (lbstresn < 0.75 * baseline.bl_lbstresn_1
						and lbstresn >= 0.5 * baseline.bl_lbstresn_1) then 'G2'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'abnormal'
						and (lbstresn < 0.5 * baseline.bl_lbstresn_1
						and lbstresn >= 0.25 * baseline.bl_lbstresn_1) then 'G3'
						when lbtestcd = 'FIBRINO'
						and baseline.result = 'abnormal'
						and (lbstresn < 0.25 * baseline.bl_lbstresn_1) then 'G4'
						when lbtestcd = 'GGT'
						and baseline.result = 'normal'
						and (lbstresn > lbstnrhi
						and lbstresn <= 2.5 * lbstnrhi) then 'G1'
						when lbtestcd = 'GGT'
						and baseline.result = 'normal'
						and (lbstresn > 2.5 * lbstnrhi
						and lbstresn <= 5 * lbstnrhi) then 'G2'
						when lbtestcd = 'GGT'
						and baseline.result = 'normal'
						and (lbstresn > 5 * lbstnrhi
						and lbstresn <= 20 * lbstnrhi) then 'G3'
						when lbtestcd = 'GGT'
						and baseline.result = 'normal'
						and (lbstresn > 20 * lbstnrhi) then 'G4'
						when lbtestcd = 'GGT'
						and baseline.result = 'abnormal'
						and (lbstresn > 2 * baseline.bl_lbstresn_1
						and lbstresn <= 2.5 * baseline.bl_lbstresn_1) then 'G1'
						when lbtestcd = 'GGT'
						and baseline.result = 'abnormal'
						and (lbstresn > 2.5 * baseline.bl_lbstresn_1
						and lbstresn <= 5 * baseline.bl_lbstresn_1) then 'G2'
						when lbtestcd = 'GGT'
						and baseline.result = 'abnormal'
						and (lbstresn > 5 * baseline.bl_lbstresn_1
						and lbstresn <= 20 * baseline.bl_lbstresn_1) then 'G3'
						when lbtestcd = 'GGT'
						and baseline.result = 'abnormal'
						and (lbstresn > 20 * baseline.bl_lbstresn_1) then 'G4'
						when lbtestcd = 'GLUC'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < lbstnrlo
						and lbstresn >= 3) then 'G1'
						when lbtestcd = 'GLUC'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 3
						and lbstresn >= 2.2) then 'G2'
						when lbtestcd = 'GLUC'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 2.2
						and lbstresn >= 1.7) then 'G3'
						when lbtestcd = 'GLUC'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 1.7) then 'G4'
						when lbtestcd = 'GLUC'
						and lbstresu = 'mg/dL'
						and (lbstresn < lbstnrlo
						and lbstresn >= 55) then 'G1'
						when lbtestcd = 'GLUC'
						and lbstresu = 'mg/dL'
						and (lbstresn < 55
						and lbstresn >= 40) then 'G2'
						when lbtestcd = 'GLUC'
						and lbstresu = 'mg/dL'
						and (lbstresn < 40
						and lbstresn >= 30) then 'G3'
						when lbtestcd = 'GLUC'
						and lbstresu = 'mg/dL'
						and (lbstresn < 30) then 'G4'
						when lbtestcd = 'HGB'
						and lbblfl = 'No'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn > (lbstresn + 1)
						and lbstresn <= (lbstresn + 20)) then 'G1'
						when lbtestcd = 'HGB'
						and lbblfl = 'No'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn > (lbstresn + 20)
						and lbstresn <= (lbstresn + 40)) then 'G2'
						when lbtestcd = 'HGB'
						and lbblfl = 'No'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn > (lbstresn + 40)) then 'G3'
						when lbtestcd = 'HGB'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn < lbstnrlo
						and lbstresn >= 100) then 'G1'
						when lbtestcd = 'HGB'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn < 100
						and lbstresn >= 80) then 'G2'
						when lbtestcd = 'HGB'
						and lbstresu in ('g/L',
						'g/l')
						and (lbstresn < 80) then 'G3'
						when lbtestcd = 'HGB'
						and lbstresu in ('mmol/l',
						'mmol/L')
						and (lbstresn < lbstnrlo
						and lbstresn >= 6.2) then 'G1'
						when lbtestcd = 'HGB'
						and lbstresu in ('mmol/l',
						'mmol/L')
						and (lbstresn < 6.2
						and lbstresn >= 4.9) then 'G2'
						when lbtestcd = 'HGB'
						and lbstresu in ('mmol/l',
						'mmol/L')
						and (lbstresn < 4.9) then 'G3'
						when lbtestcd = 'LYM'
						and lbstresu = 'mm3'
						and (lbstresn > 4000
						and lbstresn <= 20000) then 'G2'
						when lbtestcd = 'LYM'
						and lbstresu = 'mm3'
						and (lbstresn > 20000) then 'G3'
						when lbtestcd = 'LYM'
						and lbstresu = 'mm3'
						and (lbstresn < lbstnrlo
						and lbstresn >= 800) then 'G1'
						when lbtestcd = 'LYM'
						and lbstresu = 'mm3'
						and (lbstresn < 800
						and lbstresn >= 500) then 'G2'
						when lbtestcd = 'LYM'
						and lbstresu = 'mm3'
						and (lbstresn < 500
						and lbstresn >= 200) then 'G3'
						when lbtestcd = 'LYM'
						and lbstresu = 'mm3'
						and (lbstresn < 200) then 'G4'
						when lbtestcd = 'LYM'
						and lbstresu = 'x10^9/L'
						and (lbstresn < lbstnrlo
						and lbstresn >= 0.8) then 'G1'
						when lbtestcd = 'LYM'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 0.8
						and lbstresn >= 0.5) then 'G2'
						when lbtestcd = 'LYM'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 0.5
						and lbstresn >= 0.2) then 'G3'
						when lbtestcd = 'LYM'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 0.2) then 'G4'
						when lbtestcd = 'MG'
						and lbstresu = 'mg/dL'
						and (lbstresn > lbstnrhi
						and lbstresn <= 3) then 'G1'
						when lbtestcd = 'MG'
						and lbstresu = 'mg/dL'
						and (lbstresn > 3
						and lbstresn <= 8) then 'G3'
						when lbtestcd = 'MG'
						and lbstresu = 'mg/dL'
						and (lbstresn > 8) then 'G4'
						when lbtestcd = 'MG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > lbstnrhi
						and lbstresn <= 1.23) then 'G1'
						when lbtestcd = 'MG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 1.23
						and lbstresn <= 3.30) then 'G3'
						when lbtestcd = 'MG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 3.30) then 'G4'
						when lbtestcd = 'MG'
						and lbstresu = 'mg/dL'
						and (lbstresn < lbstnrlo
						and lbstresn >= 1.2) then 'G1'
						when lbtestcd = 'MG'
						and lbstresu = 'mg/dL'
						and (lbstresn < 1.2
						and lbstresn >= 0.9) then 'G2'
						when lbtestcd = 'MG'
						and lbstresu = 'mg/dL'
						and (lbstresn < 0.9
						and lbstresn >= 0.7) then 'G3'
						when lbtestcd = 'MG'
						and lbstresu = 'mg/dL'
						and (lbstresn < 0.7) then 'G4'
						when lbtestcd = 'MG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < lbstnrlo
						and lbstresn >= 0.5) then 'G1'
						when lbtestcd = 'MG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 0.5
						and lbstresn >= 0.4) then 'G2'
						when lbtestcd = 'MG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 0.4
						and lbstresn >= 0.3) then 'G3'
						when lbtestcd = 'MG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn < 0.3) then 'G4'
						when lbtestcd = 'NEUT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < lbstnrlo
						and lbstresn >= 1.5) then 'G1'
						when lbtestcd = 'NEUT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 1.5
						and lbstresn >= 1) then 'G2'
						when lbtestcd = 'NEUT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 1
						and lbstresn >= 0.5) then 'G3'
						when lbtestcd = 'NEUT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 0.5) then 'G4'
						when lbtestcd = 'NEUT'
						and lbstresu = 'mm3'
						and (lbstresn < lbstnrlo
						and lbstresn >= 1500) then 'G1'
						when lbtestcd = 'NEUT'
						and lbstresu = 'mm3'
						and (lbstresn < 1500
						and lbstresn >= 1000) then 'G2'
						when lbtestcd = 'NEUT'
						and lbstresu = 'mm3'
						and (lbstresn < 1000
						and lbstresn >= 500) then 'G3'
						when lbtestcd = 'NEUT'
						and lbstresu = 'mm3'
						and (lbstresn < 500) then 'G4'
						when lbtestcd = 'PHOS'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > lbstnrhi
						and lbstresn < 1.78) then 'G1'
						when lbtestcd = 'PHOS'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn >= 1.78
						and lbstresn <= 2.26) then 'G2'
						when lbtestcd = 'PHOS'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn >= 2.26
						and lbstresn <= 3.23) then 'G3'
						when lbtestcd = 'PHOS'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 3.23) then 'G4'
						when lbtestcd = 'PHOS'
						and lbstresu = 'mg/dL'
						and (lbstresn > lbstnrhi
						and lbstresn < 5.5) then 'G1'
						when lbtestcd = 'PHOS'
						and lbstresu = 'mg/dL'
						and (lbstresn >= 5.5
						and lbstresn <= 7.0) then 'G2'
						when lbtestcd = 'PHOS'
						and lbstresu = 'mg/dL'
						and (lbstresn >= 7.0
						and lbstresn <= 10.0) then 'G3'
						when lbtestcd = 'PHOS'
						and lbstresu = 'mg/dL'
						and (lbstresn > 10.0) then 'G4'
						when lbtestcd = 'PLAT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < lbstnrlo
						and lbstresn >= 75.0) then 'G1'
						when lbtestcd = 'PLAT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 75.0
						and lbstresn >= 50.0) then 'G2'
						when lbtestcd = 'PLAT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 50.0
						and lbstresn >= 25.0) then 'G3'
						when lbtestcd = 'PLAT'
						and lbstresu = 'x10^9/L'
						and (lbstresn < 25.0) then 'G4'
						when lbtestcd = 'PLAT'
						and lbstresu = 'mm3'
						and (lbstresn < lbstnrlo
						and lbstresn >= 75000) then 'G1'
						when lbtestcd = 'PLAT'
						and lbstresu = 'mm3'
						and (lbstresn < 75000
						and lbstresn >= 50000) then 'G2'
						when lbtestcd = 'PLAT'
						and lbstresu = 'mm3'
						and (lbstresn < 50000
						and lbstresn >= 25000) then 'G3'
						when lbtestcd = 'PLAT'
						and lbstresu = 'mm3'
						and (lbstresn < 25000) then 'G4'
						when lbtestcd = 'TRIG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn >= 1.71
						and lbstresn <= 3.42) then 'G1'
						when lbtestcd = 'TRIG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 3.42
						and lbstresn <= 5.7) then 'G2'
						when lbtestcd = 'TRIG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 5.7
						and lbstresn <= 11.4) then 'G3'
						when lbtestcd = 'TRIG'
						and lbstresu in ('mmol/L',
						'mmol/l')
						and (lbstresn > 11.4) then 'G4'
						when lbtestcd = 'TRIG'
						and lbstresu = 'mg/dL'
						and (lbstresn >= 150
						and lbstresn <= 300) then 'G1'
						when lbtestcd = 'TRIG'
						and lbstresu = 'mg/dL'
						and (lbstresn > 300
						and lbstresn <= 500) then 'G2'
						when lbtestcd = 'TRIG'
						and lbstresu = 'mg/dL'
						and (lbstresn > 500
						and lbstresn <= 1000) then 'G3'
						when lbtestcd = 'TRIG'
						and lbstresu = 'mg/dL'
						and (lbstresn > 1000) then 'G4'
						when lbstresn is null
						or lbstnrhi is null
						or lbstnrlo is null then null
						else null
					end as lbtox,
					/*case
			when lbstresn is null then null
			else baseline.result
		end as "result",*/
					baseline.result as "result",
					baseline.bl_lbstresn_1,
					baseline.visit,
					aeterm.aeterm
				from
					baseline 
				left join aeterm on
					baseline.studyid = aeterm.studyid
					and aeterm.test = baseline.lbtestcd) a
			left join ae on
				ae.studyid = a.studyid
				and a.usubjid = ae.usubjid
				--and trim(lower(ae.aeterm)) = trim(lower(a.aeterm))
				and trim(lower(ae.aeverbatim)) = trim(lower(a.aeterm))
				and a.lbdtc = ae.aestdtc
			where
				lbcat not in ('Thyroid Function Test',
				'Physical Examination',
				'Hemoglobin A1C',
				'ECG',
				'PSA Blood Sampling',
				'Urinalysis',
				'Vital Signs',
				'ECG - Triplicate Assessments',
				'EXPOSURE')
				and lbtestcd not in ('BICARB',
				'CL',
				'URATE',
				'UREA',
				'GFRE',
				'NEUTLE',
				'CA',
				'CAPHOSPD',
				'BILDIR',
				'BASOLE',
				'TROPONT',
				'RETIRBC',
				'TESTOS',
				'MONO',
				'UACID',
				'BUN',
				'NEUTB',
				'BASO',
				'CREATCLR',
				'EOSLE',
				'ANC',
				'LYMLE',
				'MONOLE',
				'HCT',
				'PT',
				'BILUNCON',
				'TROPONI',
				'BILCON',
				'BILIND',
				'RBC') )a
where rnk =1 
			--ALTER TABLE ctable_listing.ctcae_listing OWNER TO "taiho-dev-app-clinical-master-write";
			--ALTER TABLE ctable_listing.ctcae_listing OWNER TO "taiho-stage-app-clinical-master-write";
			--ALTER TABLE ctable_listing.ctcae_listing OWNER TO "taiho-app-clinical-master-write";
