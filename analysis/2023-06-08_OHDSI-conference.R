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
# Credentials

# Either set here, or just save as strings as preferred. This is for the db connection
# usr = keyring::key_set("lab_user")
# pw  = keyring::key_set("lab_password")

usr = keyring::key_get("lab_user")
pw  = keyring::key_get("lab_password")

# DB Connections
base_url = "https://atlas.roux-ohdsi-prod.aws.northeastern.edu/WebAPI"
cdm_schema = "omop_cdm_53_pmtx_202203"
my_schema = paste0("work_", keyring::key_get("lab_user"))

# Create the connection
con =  DatabaseConnector::connect(dbms = "redshift",
                                  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
                                  port = 5439,
                                  user = ,
                                  password = )

con <-  DBI::dbConnect(RPostgres::Redshift(),
                              dbname   = "ohdsi_lab",
                              host     = 'ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com',
                              port     = 5439,
                              user     = keyring::key_get("lab_user"),
                              password = keyring::key_get("lab_password"))

date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")), name = "tmpdate", overwrite = TRUE, temporary = TRUE)

df <- date_tbl %>%
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day")) %>%
    # dbplyr::sql_render()
    dplyr::collect()

tbl(con, "tmpdate") |>
    dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
    dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
    dplyr::mutate(dif_days = !!datediff("date1", "date2", interval = "day"))
# check connection
class(con)

# defaults to help with querying
options(con.default.value = con)
options(schema.default.value = cdm_schema)
options(write_schema.default.value = my_schema)

cdm <- cdm_from_con(con, cdm_schema = cdm_schema)

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
    select(person_id, year_of_birth, gender_source_value) |>
    omop_join("visit_occurrence", type = "inner", by = "person_id") |>
    select(person_id, index_date = visit_start_date, year_of_birth, gender_source_value) |>
    mutate(rand_index = sql(SqlRender::translate("RAND()", con@dbms)[[1]])) |>
    slice_min(n = 1, by = person_id, order_by = rand_index) |>
    select(-rand_index)

# From that query, make sure there are 365 days preceeding to the observation_period
# start date. Filter for age at index date is >= 40
cohort <- index_date_query |>
    omop_join("observation_period", type = "inner", by = "person_id") |>
    filter(dateDiff("day", observation_period_start_date, index_date) > 365) |>
    select(person_id, year_of_birth, gender_source_value, index_date, observation_period_start_date, observation_period_end_date) |>
    mutate(age = year(index_date) - year_of_birth) |>
    filter(age >= 40)

# saving as a persistent table in my schema as a midpoint/ intermediate table. This could be a
# temporary table if needed.
ohdsilab::set_seed(0.5)
CDMConnector::computeQuery(cohort, "frailty_cohort", temporary = temporary_intermediate_steps, schema = my_schema, overwrite = TRUE)

# ============================================================================
# Pull and summarize the cohort from the new table

# note magrittr pipe for dateadd!!
cohort <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) %>%
    mutate(
        is_female = ifelse(gender_source_value == "M", 0, 1),
        age_group = cut(age,
                        breaks = c(40,  45,  50,  55, 60,  65,  70,  75,  80,  85,  90,  95, 100, 105, 110, 115),
                        right = FALSE),
        visit_lookback_date = !!CDMConnector::dateadd("index_date", -1, interval = "year")
    ) |>
    select(person_id, is_female, age_group, visit_lookback_date, index_date)

# get total sample size and female sample size
# calculate percent female
sample_size = as.numeric(tally(cohort) |> collect())
sample_size_female = as.numeric(tally(cohort %>% filter(is_female == 1)) |> collect())
percent_female = sample_size_female / sample_size

# Cohort by sex and age group
cohort_summary <- cohort |>
    count(age_group, is_female) |>
    group_by(age_group, is_female) |>
    mutate(percent_group = n/!!sample_size) |>
    ohdsilab::dbi_collect()

# save for abstract
# write.csv(cohort_summary, "cohort_revised.csv")

# ============================================================================
# Summary Functions

# The omop2fi() function only finds people who have FI scores > 0.
# Se we need to find also all the people with FI scores == 0 and add
# them back to the dataset.
# fi_query is the result of omop2fi() and
# lb is the cutoff between robust and prefrail and
# ub is the cutoff between prefrail and frail
# denominator is how many FI categories there are for the index
fi_with_robust <- function(fi_query, cohort, denominator, lb, ub){

    # find all the people in the cohort query that are not in the fi_query
    tmp = cohort |>
        anti_join(fi_query |> select(person_id), by = "person_id") |>
        select(person_id, age_group, is_female) |>
        mutate(fi = 0, frail = 0, prefrail = 0)

    # add them back to the FI query while calculating the person-level FI
    fi_query |>
        select(person_id, is_female, age_group, score) |>
        summarize(fi = sum(score)/denominator, .by = c(person_id, age_group, is_female)) |>
        mutate(prefrail = ifelse(fi>= lb & fi < ub, 1, 0),
               frail = ifelse(fi>= ub, 1, 0)) |> ungroup() |>
        union_all(tmp)

}

# simple function to summarize the FI scores
summarize_fi <- function(fi_query){

    fi_query |>
        summarize(N = n(),
                  prefrail = sum(prefrail)/n(),
                  frail = sum(frail)/n(), .by = c(age_group, is_female))

}


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
vafi_summary <- summarize_fi(vafi_all) |> dbi_collect()
write.csv(vafi_summary, here("analysis", "vafi_summary.csv"))

# ============================================================================
# EFI
efi <- aouFI::omop2fi(con = con,
                      schema = cdm_schema,
                      index = "efi",
                      .data_search = cohort,
                      search_person_id = "person_id",
                      search_start_date = "visit_lookback_date",
                      search_end_date = "index_date",
                      keep_columns = c("age_group", "is_female"),
                      collect = FALSE,
                      unique_categories = TRUE,
                      concept_location = tbl(con, inDatabaseSchema(my_schema, "fi_indices")) |> filter(fi == "efi")
)

# save result of query as intermediate step #2
CDMConnector::computeQuery(efi, "efi_fi",
                           temporary = temporary_intermediate_steps,
                           schema = my_schema, overwrite = TRUE)

# add robust individuals back
efi_all <- fi_with_robust(fi_query = tbl(con, inDatabaseSchema(my_schema, "efi_fi")),
                          cohort = cohort,
                          denominator = 35, lb = 0.12, ub = 0.24)
# get stratified summaries, collected and saved
efi_summary <- summarize_fi(efi_all) |> dbi_collect()
write.csv(efi_summary, here("analysis", "efi_summary.csv"))

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
efragicap_summary <- summarize_fi(efragicap_all) |> dbi_collect()

# Getting this warning...need to track down source:
    # Warning message:
    #     Missing values are always removed in SQL aggregation functions.
    # Use `na.rm = TRUE` to silence this warning
    # This warning is displayed once every 8 hours.

write.csv(efragicap_summary, here("analysis", "efragicap_summary.csv"))

# ============================================================================
# HFRS

hfrs <- aouFI::omop2fi(con = con,
                       schema = cdm_schema,
                       index = "hfrs",
                       .data_search = cohort,
                       search_person_id = "person_id",
                       search_start_date = "visit_lookback_date",
                       search_end_date = "index_date",
                       keep_columns = c("age_group", "is_female"),
                       collect = FALSE,
                       unique_categories = TRUE,
                       concept_location = tbl(con, inDatabaseSchema(my_schema, "fi_indices")) |> filter(fi == "hfrs")
)

# save result of query as intermediate step #2
CDMConnector::computeQuery(hfrs, "hfrs_fi",
                           temporary = temporary_intermediate_steps,
                           schema = my_schema, overwrite = TRUE)

# add robust individuals back
hfrs_all <- fi_with_robust(fi_query = tbl(con, inDatabaseSchema(my_schema, "hfrs_fi")),
                           cohort = cohort,
                           denominator = 1, lb = 5, ub = 15)
# get stratified summaries, collected and saved
hfrs_summary <- summarize_fi(hfrs_all) |> dbi_collect()
write.csv(hfrs_summary, here("analysis", "hfrs_summary.csv"))
