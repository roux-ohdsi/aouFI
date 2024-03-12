select r.person_id, count(distinct r.drug_concept_id) cnt
FROM @cdm_schema.drug_era r
join @my_schema.frailty_cohort f
on r.person_id = f.person_id
where ((r.drug_era_start_DATE >  DATEADD(DAY, -@lookback_days, f.index_date) AND r.drug_era_start_DATE <= f.index_date)
	OR (r.drug_era_start_DATE <= DATEADD(DAY, -@lookback_days, f.index_date) AND r.drug_era_end_DATE   >  DATEADD(DAY, -@lookback_days, f.index_date)))
and DATEDIFF(DAY, r.drug_era_start_DATE, r.drug_era_end_DATE) >= 90
and drug_concept_id not in (@excluded_ingrd)
GROUP BY r.person_id

