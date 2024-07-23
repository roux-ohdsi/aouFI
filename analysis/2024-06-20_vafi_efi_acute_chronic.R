
library(tidyverse)
library(ohdsilab)
library(DBI)
library(DatabaseConnector)
library(CDMConnector)
library(glue)

source(here::here("analysis", "connection_setup.R"))

# holdig code books here...
# they are saved in the github and on the pharmetrics schema
tbl(con, inDatabaseSchema(my_schema, "vafi_rev")) |> collect() -> vafi_rev
tbl(con, inDatabaseSchema(my_schema, "efi_rev")) |> collect() -> efi_rev

# need to add lookback information
# its now in the package
vafi_lb = tbl(con, inDatabaseSchema(my_schema, "vafi_rev")) %>% collect() %>%
    left_join(aouFI::lb %>% filter(fi == "vafi") %>% select(-fi), by = "category")
# add to db
#insertTable_chunk(vafi_lb, "vafi_rev2")
#rm(vafi_lb)

# test it
# tbl(con, inDatabaseSchema(my_schema, "vafi_rev2"))

efi_lb = tbl(con, inDatabaseSchema(my_schema, "efi_rev")) %>% collect() %>%
    left_join(aouFI::lb %>% filter(fi == "efi") %>% select(-fi), by = "category")

#efi_lb
#insertTable_chunk(efi_lb, "efi_rev2")
# rm(efi_lb)

# test it
# tbl(con, inDatabaseSchema(my_schema, "efi_rev2"))
# replace for saving files
data_source = "pharmetrics"

#write_csv(vafi_rev, here("KI", "2024-03-25_vafid.csv"))
#write_csv(efi_rev, here("KI", "2024-03-25_efi_clegg_snomed.csv"))

# vafi_rev = vafi_rev %>% mutate(concept_id = as.integer(concept_id))
# usethis::use_data(vafi_rev, overwrite = TRUE)

# Summary Functions. now in separate script
source(here::here("analysis", "summary_functions.R"))

# cdm_schema is the omop db
# my_schema is the user write schema


# ============================================================================
# ################################ COHORT DEFINITION #########################
# ============================================================================

# Cohort Generation

# Note - this only needs to be run once, if the intermediate steps are saved to
# persistent tables in the user schema

# Join person table to visit occurrence table
# Pick a random visit. I think this SqlRender::translate() should make this a bit
# more flexible to the different dbms...
index_date_query <- tbl(con, inDatabaseSchema(cdm_schema, "person")) |>
    select(person_id, year_of_birth, gender_concept_id) |>
    omop_join("visit_occurrence", type = "inner", by = "person_id") |>
    select(person_id, index_date = visit_start_date, year_of_birth, gender_concept_id) |>
    mutate(rand_index = sql(SqlRender::translate("RAND()", con@dbms)[[1]])) |>
    slice_min(n = 1, by = person_id, order_by = rand_index) |>
    select(-rand_index)

# From that query, make sure there are 365 days preceeding to the observation_period
# start date. Filter for age at index date is >= 40
#!!!!!!!!!!! Changed to: >= 365 below !!!!!!!!!!!!#
cohort <- index_date_query |>
    omop_join("observation_period", type = "inner", by = "person_id") |>
    filter(dateDiff("day", observation_period_start_date, index_date) >= 365) |>
    select(person_id, year_of_birth, gender_concept_id, index_date, observation_period_start_date, observation_period_end_date) |>
    mutate(age = year(index_date) - year_of_birth,
           yob_imputed = ifelse(year_of_birth < 1938, 1, 0)) |>
    filter(age >= 40)

# test = cohort |> dbi_collect()

# saving as a persistent table in my schema as a midpoint/ intermediate table. This could be a
# temporary table if needed.
ohdsilab::set_seed(0.5)
#CDMConnector::computeQuery(cohort, "frailty_cohort", temporary = temporary_intermediate_steps, schema = my_schema, overwrite = TRUE)



# ============================================================================
# ################################ COHORT #######################################
# ============================================================================
# Do it with everyone just omop VAFI
cohort_ids <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) |>
    distinct(person_id)

cohort_all <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) %>%
    mutate(age = ifelse(yob_imputed == 1, 84, age)) %>%
    mutate(
        is_female = ifelse(gender_concept_id == 8507, 0, 1),
        age_group = cut(age,
                        breaks = c(40,  45,  50,  55, 60,  65,  70,  75,  80,  100),
                        right = FALSE,
                        include.lowest = TRUE),
        visit_lookback_date = !!CDMConnector::dateadd("index_date", -1, interval = "year")
    ) |>
    select(person_id, is_female, age_group, visit_lookback_date, index_date, yob_imputed)|>
    inner_join(cohort_ids, by = "person_id") |>
    group_by(person_id) |>
    filter(index_date == min(index_date)) |>
    ungroup()

CDMConnector::computeQuery(cohort_all, "frailty_cohort_clean",
                           temporary = FALSE,
                           schema = my_schema, overwrite = TRUE)



# ============================================================================
# ################################ PolyPharmacy #######################################
# ============================================================================

# I had to do this separately - for some reason adding this to the query in redshift
# Is overloading the db and it either won't collect and times out or just crashes.
dbms = con@dbms

pp_lookback <- switch (dbms,
                       "redshift" = glue::glue("DATEADD(YEAR, -1, person_end_date)"),
                       "bigquery" = glue::glue("DATE_ADD(person_end_date, INTERVAL -1 YEAR)"),
                       rlang::abort(glue::glue("Connection type {paste(class(dot$src$con), collapse = ', ')} is not supported!"))
)

pp_datediff <- switch (dbms,
                       "redshift" = glue::glue("DATEDIFF(DAY,  drug_era_start_date, drug_era_end_date)"),
                       "bigquery" = glue::glue("DATE_DIFF(drug_era_end_date, person_start_date, DAY)"),
                       rlang::abort(glue::glue("Connection type {paste(class(dot$src$con), collapse = ', ')} is not supported!"))
)

# at least 90 days of drug era
pp = tbl(con, inDatabaseSchema(cdm_schema, "drug_era")) |>
    inner_join(cohort_all, by = "person_id", x_as = "pp1", y_as = "pp2") |>
    select(person_start_date = visit_lookback_date, person_end_date = index_date,
           person_id, drug_concept_id, drug_era_start_date, drug_era_end_date) |>
    mutate(drug_search_start_date = dplyr::sql(!!pp_lookback),
           era_date_diff = dplyr::sql(!!pp_datediff)) |>
    filter(
        (drug_era_start_date >= drug_search_start_date & drug_era_start_date <= person_end_date) | (drug_era_end_date >= drug_search_start_date & drug_era_end_date <= person_end_date),
        era_date_diff > 1
    ) |>
    distinct(person_id, drug_concept_id) |>
    count(person_id) |>
    filter(n >= 10) |>
    mutate(score = 1,
           category = "Polypharmacy",
           #start_date = NA,
           #chronic_category = NA
    ) |>
    inner_join(cohort_all, by = "person_id", x_as = "pp3", y_as = "pp4") |>
    select(
        person_id,
        is_female,
        age_group,
        score,
        category
    )

# CDMConnector::computeQuery(pp, "frailty_cohort_polypharmacy", temporary = FALSE, schema = my_schema, overwrite = TRUE)

# ============================================================================
# ################################ VAFI #######################################
# ============================================================================

# vafi_all <- omop2fi(con = con,
#                        schema = cdm_schema,
#                        index = "efi",
#                        .data_search = cohort_all %>% head(10),
#                        search_person_id = "person_id",
#                        search_start_date = "visit_lookback_date",
#                        search_end_date = "index_date",
#                        keep_columns = c("age_group", "is_female"),
#                        collect = FALSE,
#                        unique_categories = TRUE,
#                        concept_location = tbl(con, inDatabaseSchema(my_schema, "vafi_rev2")) |> rename(chronic_category = lookback)
# ) |>
#     distinct(person_id, age_group, is_female, score, category)
#

# ============================================================================
# ################################ VAFI VARIABLE LOOKBACK ####################
# ============================================================================

cohort_all = tbl(con, inDatabaseSchema(my_schema, "frailty_cohort_clean"))
# test_cohort = cohort_all %>% head(100)
# #pid = collect(test_cohort %>% select(person_id))
# test_pp = tbl(con, inDatabaseSchema(my_schema, "frailty_cohort_polypharmacy")) %>% inner_join(test_cohort %>% select(person_id), by = "person_id")
vafi_all <- omop2fi_lb(con = con,
                    schema = cdm_schema,
                    index = "vafi",
                    .data_search = cohort_all,
                    search_person_id = "person_id",
                    search_start_date = "visit_lookback_date",
                    search_end_date = "index_date",
                    keep_columns = c("age_group", "is_female"),
                    collect = FALSE,
                    unique_categories = TRUE,
                    dbms = "redshift",
                    concept_location = tbl(con, inDatabaseSchema(my_schema, "vafi_rev2")) |> rename(chronic_category = lookback),
                    acute_lookback = 1,
                    chronic_lookback = 3
) |>
    distinct(person_id, age_group, is_female, score, category)


dplyr::compute(vafi_all, inDatabaseSchema(my_schema, "vafi_all_ac"),
               temporary = FALSE,
               overwrite = TRUE)
vafi_all = tbl(con, inDatabaseSchema(my_schema, "vafi_all"))


# add robust individuals back
vafi_all_summary <- fi_with_robust(
                           fi_query = vafi_all,
                           cohort = cohort_all,
                           denominator = 31, lb = 0.11, ub = 0.21)

# summarize
t = summarize_fi(vafi_all_summary) %>% collect()
write.csv(t, glue("KI/{Sys.Date()}_vafi_acute1-chronic3_{data_source}.csv"), row.names = FALSE)

vafi_cats = aouFI::vafi_rev %>% distinct(category) %>% pull(category)
vafi_c = vafi_all %>% select(person_id, category) %>% collect() %>% mutate(score = 1)
cohort_c = cohort_all |> select(person_id, age_group, is_female) %>% collect()

vafi_cat_summary = summarize_cats(
    vafi_c,
    cohort = cohort_c,
    cats = vafi_cats) %>% arrange(category, age_group, is_female) %>%
    drop_na() %>%
    mutate(count = ifelse(count < 20, 0, count),
           percent = ifelse(count < 20, 0, percent))
write.csv(vafi_cat_summary, glue("KI/{Sys.Date()}_vafi_categories_acute1-chronic1_{data_source}.csv"), row.names = FALSE)


rm(t)
rm(vafi_cat_summary)
rm(vafi_c)
gc()
# may want to clear memory at this point...

# ============================================================================
# ################################ EFI #######################################
# ============================================================================

efi_all <- aouFI::omop2fi_lb(con = con,
                           schema = cdm_schema,
                           index = "efi",
                           .data_search = cohort_all,
                           search_person_id = "person_id",
                           search_start_date = "visit_lookback_date",
                           search_end_date = "index_date",
                           keep_columns = c("age_group", "is_female"),
                           collect = FALSE,
                           unique_categories = TRUE,
                          concept_location = tbl(con, inDatabaseSchema(my_schema, "efi_rev2")) |> rename(chronic_category = lookback)
) |>
    distinct(person_id, age_group, is_female, score, category)

union_all(
    efi_all,
    tbl(con, inDatabaseSchema(my_schema, "frailty_cohort_polypharmacy"))
) %>% distinct() -> efi_all

# save result of query as intermediate step #2

dplyr::compute(efi_all, inDatabaseSchema(my_schema, "efi_all"),
                           temporary = FALSE,
                           overwrite = TRUE)



efi_all_pp = tbl(con, inDatabaseSchema(my_schema, "efi_all"))

# add robust individuals back
efi_all_summary <- fi_with_robust(
    fi_query = efi_all_pp,
    cohort = cohort_all,
    denominator = 35, lb = 0.12, ub = 0.24)

# summarize
t = summarize_fi(efi_all_summary) %>% collect()
write.csv(t, glue("KI/{Sys.Date()}_efi_acute1-chronic1_{data_source}.csv"), row.names = FALSE)

# t %>% select(1, 2, 7, 8, 9) %>% pivot_longer(3:5) %>%
#     ggplot(aes(x = age_group, y = value, color = name, shape = factor(is_female))) +
#     geom_point()


efi_cats = aouFI::fi_indices %>% filter(fi == "efi_sno") %>% distinct(category) %>% pull(category)
efi_c = efi_all_pp %>% select(person_id, category, score) %>% collect()
# cohort_c from above with vafi

efi_cat_summary = summarize_cats(
    efi_c,
    cohort = cohort_c,
    cats = efi_cats) %>% arrange(category, age_group, is_female) %>%
    drop_na() %>%
    mutate(count = ifelse(count < 20, 0, count),
           percent = ifelse(count < 20, 0, percent))
write.csv(efi_cat_summary, glue("KI/{Sys.Date()}_efi_categories_acute1-chronic1_{data_source}.csv"), row.names = FALSE)


rm(t)
rm(efi_cat_summary)
rm(efi_c)
rm(cohort_c)
gc()



t %>% select(1, 2, 7:9) %>% pivot_longer(cols = 3:5) %>%
    ggplot(aes(x = age_group, y = value, color = name, shape = factor(is_female))) +
    geom_point()



