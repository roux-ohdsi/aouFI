rm(list = ls())

# run connection_setup.R to define:
# con -- DB connection
# res_dir -- where results are saved
# cdm_schema, fi_schema, my_schema -- data, fi, and cohort schemas
source(here::here("analysis", "connection_setup.R"))

lookback_yrs = 1

genData <- TRUE
if (genData) {

    frailty_cohort <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) %>%
        mutate(age = ifelse(yob_imputed == 1, 84, age)) %>%
        mutate(
            is_female = ifelse(gender_concept_id == 8507, 0, 1),
            age_group = cut(age,
                            breaks = c(40, 45, 50, 55, 60, 65, 70, 75, 80, 120),
                            # breaks = c(40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 120),
                            right = FALSE,
                            include.lowest = TRUE),
            visit_lookback_date = !!CDMConnector::dateadd("index_date", -lookback_yrs, interval = "year")
        ) |>
        select(person_id, is_female, age_group, visit_lookback_date, index_date, yob_imputed) %>%
        mutate(id = RAND()) %>%
        slice_min(prop = .05, order_by = id)


    sample_size = as.numeric(tally(frailty_cohort) |> collect())


    search_person_id = "person_id"
    search_start_date = "visit_lookback_date"
    search_end_date = "index_date"
    keep_cols = c("age_group", "is_female")

    pid = frailty_cohort |>
        dplyr::select(person_id = !!search_person_id,
                      person_start_date = !!search_start_date,
                      person_end_date = !!search_end_date,
                      !!!keep_cols) |>
        mutate(person_start_date = as.Date(person_start_date),
               person_end_date = as.Date(person_end_date))

    concept_table = tbl(con, inDatabaseSchema(my_schema, "fi_indices")) |> filter(fi == "vafi")

    condition_concept_ids <- tbl(con, inDatabaseSchema(cdm_schema, "concept")) |>
        filter(standard_concept == "S") |>
        distinct(concept_id, name = concept_name) |>
        inner_join(concept_table |> distinct(concept_id),
                   by = c("concept_id"), x_as = "c1", y_as = "c2" ) #|> # vocabulary_id

    # condition occurrence
    cond_occurrences <- tbl(con, inDatabaseSchema(cdm_schema, "condition_occurrence")) |>
        inner_join(pid, by = "person_id", x_as = "x1", y_as = "y1") |>
        inner_join(condition_concept_ids, by = c("condition_concept_id" = "concept_id"), x_as = "x2", y_as = "y2") |>
        mutate(source = "condition") |>
        select(person_id, !!!keep_cols,
               concept_id = condition_concept_id,
               concept_name = name,
               start_date = condition_start_date,
               person_start_date,
               person_end_date,
               source
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    # observations
    obs <- tbl(con, inDatabaseSchema(cdm_schema, "observation"))  |>
        inner_join(pid, by = "person_id", x_as = "x3", y_as = "y3") |>
        inner_join(condition_concept_ids, by = c("observation_concept_id" = "concept_id"), x_as = "x4", y_as = "y4") |>
        mutate(source = "observation") |>
        select(person_id, !!!keep_cols,
               concept_id = observation_concept_id,
               concept_name = name,
               start_date = observation_date,
               person_start_date,
               person_end_date,
               source
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    # procedures
    proc <- tbl(con, inDatabaseSchema(cdm_schema, "procedure_occurrence")) |>
        inner_join(pid, by = "person_id", x_as = "x5", y_as = "y5") |>
        inner_join(condition_concept_ids, by = c("procedure_concept_id" = "concept_id"), x_as = "x6", y_as = "y6") |>
        mutate(source = "procedure") |>
        select(person_id, !!!keep_cols,
               concept_id = procedure_concept_id,
               concept_name = name,
               start_date = procedure_date,
               person_start_date,
               person_end_date,
               source
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    # device exposure
    dev <- tbl(con, inDatabaseSchema(cdm_schema, "device_exposure")) |>
        inner_join(pid, by = "person_id", x_as = "x7", y_as = "y7") |>
        inner_join(condition_concept_ids, by = c("device_concept_id" = "concept_id"), x_as = "x8", y_as = "y8") |>
        mutate(source = "device") |>
        select(person_id, !!!keep_cols,
               concept_id = device_concept_id,
               concept_name = name,
               start_date = device_exposure_start_date,
               person_start_date,
               person_end_date,
               source
        ) |>
        filter(start_date >= person_start_date, start_date < person_end_date) |>
        distinct()

    dat <-
        union_all(cond_occurrences, obs, dev, proc)

    dat <- dat %>%
        mutate(person_start_date = as.character(person_start_date),
               person_end_date = as.character(person_end_date),
               start_date = as.character(start_date)) %>%
        select(person_id,
               !!!keep_cols,
               person_start_date,
               person_end_date,
               start_date,
               concept_id,
               source)  |>
        left_join(concept_table, by = c("concept_id")) #|> dbi_collect()

    tally(dat)
    #saveRDS(dat, file.path(res_dir, paste0(cdm_schema, "_fi_indices_10y.RDS")))

    CDMConnector::compute_query(dat, temporary = FALSE, schema = my_schema, name = "vafi_10yr", overwrite = TRUE)
} else {
    dat <- readRDS(file.path(res_dir, paste0(cdm_schema, "_fi_indices_10y.RDS")))
}

fi_table = tbl(con, inDatabaseSchema(my_schema, "vafi_10yr"))

# had to do these separately b/c they won't work in the same summarize
dat_count <- fi_table %>% count(person_id, category)
dat_first <- fi_table %>% group_by(person_id, category) %>% summarize(first_occ = min(as.Date(start_date))) %>% ungroup()
dat_index_date <- fi_table %>% distinct(person_id, category, person_end_date) %>% ungroup()

dat = left_join(dat_count, dat_index_date, by = c("person_id", "category")) %>%
    left_join(dat_first, by = c("person_id", "category"))

dat <- dat |> dbi_collect()

dat.summary <- dat %>%
    rename(first.occ = first_occ) %>%
    group_by(person_id, category) %>%
    # summarise(count = n(),
    #           first.occ = min(as.Date(start_date)),
    #           index_date = min(person_end_date)) %>%
    mutate(index_date = min(person_end_date),
           time.to.index = DatabaseConnector::dateDiff("day", as.Date(first.occ), as.Date(index_date))) %>%
    filter(time.to.index > 365) %>%
    mutate(event.per.year = 365.25 * n/time.to.index)

tmp <- dat.summary %>% ungroup() %>% select(category, event.per.year)

def.summry <- tmp %>%
    group_by(category) %>%
    summarise(avg = mean(event.per.year),
              std = sd(event.per.year),
              median = median(event.per.year),
              p05 = quantile(event.per.year, 0.05),
              p25 = quantile(event.per.year, 0.25),
              p75 = quantile(event.per.year, 0.75),
              p95 = quantile(event.per.year, 0.95),
    )

write.csv(def.summry, here::here("analysis", paste0(cdm_schema, "_deficit_per_year_1y.csv")))
