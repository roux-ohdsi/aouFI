# Script for calculating FI by age/gender in different OMOP Databases
# Uses the aouFI package and ohdsilab package from roux-ohdsi


# Packages
library(keyring)
library(DatabaseConnector)
library(CDMConnector)
library(tidyverse)
library(ohdsilab)
library(aouFI)
library(here)
# ============================================================================

# Connection details script
# results in a connection object called "con"
# also saves object names for a cdm_schema and a write_schema
# also sets the default options for cdm_schema, write_schema, and connection
source(here::here("analysis", "connection_setup.R"))

# ============================================================================
# Frailty Index lookup tables

# Make sure that the aouFI::fi_indices lookup tables are saved in the instance.
# insertTable_chunk() is just a wrapper on insertTable() from DatabaseConnector.
# We're having problems writing local tables of > 1000 rows to the db at a time,
# so this functino breaks up the data into pieces before writing.
# Only run once

# ohdsilab::insertTable_chunk(aouFI::fi_indices, table_name = "fi_indices", overwrite = TRUE)

# ============================================================================

# set this to toggle the intermediate steps as persistent or temporary to the user schema
temporary_intermediate_steps = FALSE

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

test = cohort |> dbi_collect()

# saving as a persistent table in my schema as a midpoint/ intermediate table. This could be a
# temporary table if needed.
ohdsilab::set_seed(0.5)
CDMConnector::computeQuery(cohort, "frailty_cohort", temporary = temporary_intermediate_steps, schema = my_schema, overwrite = TRUE)

# ============================================================================
# Pull and summarize the cohort from the new table

# note magrittr pipe for dateadd!!
# age changed to 84 if it was imputed due to age - this will fall in the
# 80+ bracket which we can later mark as 80+
cohort <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) %>%
    mutate(age = ifelse(yob_imputed == 1, 84, age)) %>%
    mutate(
        is_female = ifelse(gender_concept_id == 8507, 0, 1),
        age_group = cut(age,
                        breaks = c(40,  45,  50,  55, 60,  65,  70,  75,  80,  100),
                        right = FALSE,
                        include.lowest = TRUE),
        visit_lookback_date = !!CDMConnector::dateadd("index_date", -1, interval = "year")
    ) |>
    select(person_id, is_female, age_group, visit_lookback_date, index_date, yob_imputed)

# get total sample size and female sample size
# calculate percent female
sample_size = as.numeric(tally(cohort) |> collect())
sample_size_female = as.numeric(tally(cohort %>% filter(is_female == 1)) |> collect())
percent_female = sample_size_female / sample_size

# 2023-06-14 numbers
sample_size # 5292854
sample_size_female # 2838483
percent_female # 0.5362859

# Cohort by sex and age group
cohort_summary <- cohort |>
    count(age_group, is_female) |>
    group_by(age_group, is_female) |>
    mutate(percent_group = n/!!sample_size) |>
    ohdsilab::dbi_collect()

write.csv(cohort_summary, here::here("analysis", paste(Sys.Date(), "pharmetrics_cohort_summary.csv", sep = "-")), row.names = FALSE)

# ============================================================================

# Summary Functions. now in separate script
source(here::here("analysis", "summary_functions.R"))

# ============================================================================
# VAFI

vafi <- aouFI::omop2fi(con = con,
                       schema = cdm_schema,
                       index = "vafi",
                       .data_search = cohort,
                       search_person_id = "person_id",
                       search_start_date = "visit_lookback_date",
                       search_end_date = "index_date",
                       keep_columns = c("age_group", "is_female"),
                       collect = FALSE,
                       unique_categories = TRUE,
                       concept_location = tbl(con, inDatabaseSchema(my_schema, "fi_indices")) |> filter(fi == "vafi")
) |>
    select(person_id, age_group, is_female, score, category)

# save result of query as intermediate step #2
CDMConnector::computeQuery(vafi, "vafi_fi",
                           temporary = temporary_intermediate_steps,
                           schema = my_schema, overwrite = TRUE)

# add robust individuals back
vafi_all <- fi_with_robust(fi_query = tbl(con, inDatabaseSchema(my_schema, "vafi_fi")),
                           cohort = cohort,
                           denominator = 30, lb = 0.11, ub = 0.21)

# get stratified summaries, collected and saved
vafi_summary <- summarize_fi(vafi_all) |> dbi_collect() |> mutate(fi = "vafi")

# category specifics
vafi_cat1 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "vafi_fi")),
                              cohort = cohort,
                              fi_category = "Osteo",
                              index = "vafi") |> dbi_collect()

vafi_cat2 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "vafi_fi")),
                              cohort = cohort,
                              fi_category = "Dementia",
                              index = "vafi") |> dbi_collect()

vafi_cat3 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "vafi_fi")),
                              cohort = cohort,
                              fi_category = "CAD",
                              index = "vafi") |> dbi_collect()


vafi_cats = bind_rows(vafi_cat1,vafi_cat2, vafi_cat3) |>
    mutate(fi = "vafi") |>
    arrange(category, age_group)


# ============================================================================
# EFRAGICAP
efragicap <- aouFI::omop2fi(con = con,
                            schema = cdm_schema,
                            index = "efragicap",
                            .data_search = cohort,
                            search_person_id = "person_id",
                            search_start_date = "visit_lookback_date",
                            search_end_date = "index_date",
                            keep_columns = c("age_group", "is_female"),
                            collect = FALSE,
                            unique_categories = TRUE,
                            concept_location = tbl(con, inDatabaseSchema(my_schema, "fi_indices")) |> filter(fi == "efragicap")
)

# save result of query as intermediate step #2
CDMConnector::computeQuery(efragicap, "egragicap_fi",
                           temporary = temporary_intermediate_steps,
                           schema = my_schema, overwrite = TRUE)

# add robust individuals back
efragicap_all <- fi_with_robust(fi_query = tbl(con, inDatabaseSchema(my_schema, "egragicap_fi")),
                                cohort = cohort,
                                denominator = 35, lb = 0.12, ub = 0.24)
# get stratified summaries, collected and saved
efragicap_summary <- summarize_fi(efragicap_all) |> dbi_collect() |> mutate(fi = "efragicap")

# category specifics
efragicap_cat1 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "egragicap_fi")),
                                   cohort = cohort,
                                   fi_category = "Osteoporosis",
                                   index = "efragicap") |> dbi_collect()

efragicap_cat2 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "egragicap_fi")),
                                   cohort = cohort,
                                   fi_category = "Memory & cognitive problems",
                                   index = "efragicap") |> dbi_collect()

efragicap_cat3 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "egragicap_fi")),
                                   cohort = cohort,
                                   fi_category = "Ischaemic heart disease",
                                   index = "efragicap") |> dbi_collect()

efragicap_cats = bind_rows(efragicap_cat1,efragicap_cat2, efragicap_cat3) |>
    mutate(fi = "efragicap") |>
    arrange(category, age_group)

# ============================================================================
# EFI - not run for now.
# efi <- aouFI::omop2fi(con = con,
#                       schema = cdm_schema,
#                       index = "efi_sno_expanded",
#                       .data_search = cohort,
#                       search_person_id = "person_id",
#                       search_start_date = "visit_lookback_date",
#                       search_end_date = "index_date",
#                       keep_columns = c("age_group", "is_female"),
#                       collect = FALSE,
#                       unique_categories = TRUE,
#                       concept_location = tbl(con, inDatabaseSchema(my_schema, "fi_indices")) |> filter(fi == "efi_sno_expanded")
# )
#
# # save result of query as intermediate step #2
# CDMConnector::computeQuery(efi, "efi_sno_expanded",
#                            temporary = temporary_intermediate_steps,
#                            schema = my_schema, overwrite = TRUE)
#
# # add robust individuals back
# efi_all <- fi_with_robust(fi_query = tbl(con, inDatabaseSchema(my_schema, "efi_sno_expanded")),
#                           cohort = cohort,
#                           denominator = 35, lb = 0.12, ub = 0.24)
#
# # get stratified summaries, collected and saved
# efi_summary <- summarize_fi(efi_all) |> dbi_collect() |> mutate(fi = "efi")
#
# # category specifics
# efi_cat1 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "vafi_fi")),
#                              cohort = cohort,
#                              fi_category = "CVA",
#                              index = "efi") |> dbi_collect()
#
# efi_cat2 <- percent_category(fi_query = tbl(con, inDatabaseSchema(my_schema, "vafi_fi")),
#                              cohort = cohort,
#                              fi_category = "Dementia",
#                              index = "efi") |> dbi_collect()
#
# efi_cats <- bind_rows(efi_cat1,efi_cat2) |> mutate(fi = "efi")


# ============================================================================
# Saving

summaries = bind_rows(vafi_summary, efragicap_summary) # , efi_summary
cats = bind_rows(vafi_cats, efragicap_cats) # , efi_cats


write.csv(summaries, here("analysis", paste(Sys.Date(), "fi_summaries.csv", sep = "-")))
write.csv(cats, here("analysis", paste(Sys.Date(), "fi_cats.csv", sep = "-")))











