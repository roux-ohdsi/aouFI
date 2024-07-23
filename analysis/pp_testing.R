
library(tidyverse)
library(ohdsilab)
library(DBI)
library(DatabaseConnector)
library(CDMConnector)
library(glue)

# connection details from the package
source(here::here("analysis", "connection_setup.R"))

# redshift of bigquery
dbms = "redshift"
pp_lookback <- switch (dbms,
                       "redshift" = glue::glue("DATEADD(YEAR, -1, person_end_date)"),
                       "bigquery" = glue::glue("DATE_ADD(person_end_date, INTERVAL -1 YEAR)"),
                       rlang::abort(glue::glue("Connection type {paste(class(dot$src$con), collapse = ', ')} is not supported!"))
)

pp_datediff <- switch (dbms,
                       "redshift" = glue::glue("DATEDIFF(DAY,  drug_era_start_date, drug_era_end_date)"),
                       "bigquery" = glue::glue("DATE_DIFF(drug_era_start_date, person_end_date, DAY)"),
                       rlang::abort(glue::glue("Connection type {paste(class(dot$src$con), collapse = ', ')} is not supported!"))
)

# cohort defintion from fi script
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
    filter(index_date == min(index_date))



# at least 90 days of drug era
test = tbl(con, inDatabaseSchema(cdm_schema, "drug_era")) |>
    inner_join(cohort_all, by = "person_id", x_as = "x9", y_as = "y9") |>
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
    mutate(pp = ifelse(n > 5, 1, 0))

cohort_all %>%
    select(person_id) %>%
    left_join(test, by = "person_id") %>%
    mutate(n = ifelse(is.na(n), 0, n)) -> test2

test_c = collect(test2)

n = nrow(test_c)

quantile(test_c$n, probs = seq(0, 1, 0.1))
summary(test_c$n)
sum(test_c$n>=10)/n
sum(test_c$n>=15)/n
ggplot(test_c, aes(x = n)) +geom_histogram(fill = "white",color = "black", bins = 50) + theme_minimal()


# at least 30 days
test_30 = tbl(con, inDatabaseSchema(cdm_schema, "drug_era")) |>
    inner_join(cohort_all, by = "person_id", x_as = "x9", y_as = "y9") |>
    select(person_start_date = visit_lookback_date, person_end_date = index_date,
           person_id, drug_concept_id, drug_era_start_date, drug_era_end_date) |>
    mutate(drug_search_start_date = dplyr::sql(!!pp_lookback),
           era_date_diff = dplyr::sql(!!pp_datediff)) |>
    filter(
        (drug_era_start_date >= drug_search_start_date & drug_era_start_date <= person_end_date) | (drug_era_end_date >= drug_search_start_date & drug_era_end_date <= person_end_date),
        era_date_diff >= 30
    ) |>
    distinct(person_id, drug_concept_id) |>
    count(person_id)

cohort_all %>%
    select(person_id) %>%
    left_join(test_30, by = "person_id") %>%
    mutate(n = ifelse(is.na(n), 0, n)) -> test_30_all

test_c_30 = collect(test_30_all)

quantile(test_c_30$n, probs = seq(0, 1, 0.1))
summary(test_c_30$n)
sum(test_c_30$n>=10)/n
sum(test_c_30$n>=15)/n
ggplot(test_c_30, aes(x = n)) +geom_histogram(fill = "white",color = "black", bins = 50) + theme_minimal()


# no minimum days of drug era
test_1 = tbl(con, inDatabaseSchema(cdm_schema, "drug_era")) |>
    inner_join(cohort_all, by = "person_id", x_as = "x9", y_as = "y9") |>
    select(person_start_date = visit_lookback_date, person_end_date = index_date,
           person_id, drug_concept_id, drug_era_start_date, drug_era_end_date) |>
    mutate(drug_search_start_date = dplyr::sql(!!pp_lookback),
           era_date_diff = dplyr::sql(!!pp_datediff)) |>
    filter(
        (drug_era_start_date >= drug_search_start_date & drug_era_start_date <= person_end_date) | (drug_era_end_date >= drug_search_start_date & drug_era_end_date <= person_end_date),
        era_date_diff >= 1
    ) |>
    distinct(person_id, drug_concept_id) |>
    count(person_id)

cohort_all %>%
    select(person_id) %>%
    left_join(test_1, by = "person_id") %>%
    mutate(n = ifelse(is.na(n), 0, n)) -> test_1_all

test_c_1 = collect(test_1_all)

quantile(test_c_1$n, probs = seq(0, 1, 0.1))
summary(test_c_1$n)
sum(test_c_1$n>=10)/n
sum(test_c_1$n>=15)/n
ggplot(test_c_1, aes(x = n)) +geom_histogram(fill = "white",color = "black", bins = 50) + theme_minimal()


# number of drug/ingredientsin 1 year prior to index: no requirements on duration of drug
quantile(test_c_1$n, probs = seq(0, 1, 0.1))

# number of drug/ingredientsin 1 year prior to index: 30-day requirements on duration of drug
quantile(test_c_30$n, probs = seq(0, 1, 0.1))

# number of drug/ingredientsin 1 year prior to index: 90-day requirements on duration of drug
quantile(test_c$n, probs = seq(0, 1, 0.1))


