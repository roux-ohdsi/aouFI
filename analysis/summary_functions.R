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
        distinct(person_id, is_female, age_group, score, category) |>
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


# disorder area
percent_category <- function(fi_query, cohort, fi_category, index){

    # list of possible indices
    indices = c("vafi", "efi", "efragicap", "hfrs")
    if(!(index %in% indices)){stop(paste("index not found. choose from: \n", paste(indices, collapse = ", ")))}
    # check that the desired category is in the FI
    cats = unique(fi_indices |> filter(fi == index) |> pull(category))
    if(!(fi_category %in% cats)){stop(paste("category not found. choose from:", index, "categories: \n", paste(cats, collapse = ", ")))}

    # find all the people in the cohort query that are not in the fi_query
    has_cat = fi_query |> filter(category == fi_category) |> select(person_id)

    tmp = cohort |>
        anti_join(has_cat, by = "person_id") |>
        select(person_id, age_group, is_female) |>
        mutate(score = 0, category = fi_category)

    # add them back to the FI query while calculating the person-level FI
    fi_query |>
        distinct(person_id, is_female, age_group, score, category) |>
        select(person_id, is_female, age_group, score, category) |>
        filter(category == fi_category) |> #print(head()) |>
        union_all(tmp) |>
        summarize(
            n_yes = sum(score, na.rm = TRUE),
            prop_yes = mean(score, na.rm = TRUE),
            .by = c(age_group, is_female, category)
        )
}




icd_fi <- function(){

    pid = cohort |>
        dplyr::select(person_id,
                      person_start_date = visit_lookback_date,
                      person_end_date = index_date,
                      age_group, is_female) |>
        mutate(person_start_date = as.Date(person_start_date),
               person_end_date = as.Date(person_end_date))


    schema = cdm_schema

    concept                 = inDatabaseSchema(schema, "concept")
    condition_occurrence    = inDatabaseSchema(schema, "condition_occurrence")
    observation             = inDatabaseSchema(schema, "observation")
    procedure_occurrence    = inDatabaseSchema(schema, "procedure_occurrence")
    device_exposure         = inDatabaseSchema(schema, "device_exposure")
    person                  = inDatabaseSchema(schema, "person")


    concept_table = tbl(con, inDatabaseSchema(my_schema, "vafi_10")) |> select(icd_code = code, deficit)
    concept_table_proc = tbl(con, inDatabaseSchema(my_schema, "vafi_proc")) |> select(icd_code = code, deficit, codetype)

    # The following four calls go find the presence of the concept IDs in the
    # condition occurrence, procedure, observation, and device tabes, limiting
    # the search to the person_ids and concepts in teh above condition_concept_ids
    # table. They also calculate a start year and month which are important for
    # later analyses that are dependent on when the FI event occurs.

    # go find instances of our concepts in the condition occurrence table
    cond_occurrences <- tbl(con, condition_occurrence) |>
        inner_join(pid, by = "person_id", x_as = "x1", y_as = "y1") |>
        inner_join(concept_table, by = c("condition_source_value" = "icd_code"), x_as = "x2", y_as = "y2") |>
        left_join(tbl(con, concept), by = c("condition_concept_id"="concept_id")) |>
        select(person_id,
               age_group,
               is_female,
               concept_id = condition_concept_id,
               concept_name,
               start_date = condition_start_date,
               deficit,
               source_value = condition_source_value,
               person_start_date,
               person_end_date,
               vocabulary_id
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    # do the same for the observation table
    obs <- tbl(con, observation)  |>
        inner_join(pid, by = "person_id", x_as = "x3", y_as = "y3") |>
        inner_join(concept_table, by = c("observation_source_value" = "icd_code"), x_as = "x4", y_as = "y4") |>
        left_join(tbl(con, concept), by = c("observation_concept_id"="concept_id")) |>
        select(person_id,
               age_group,
               is_female,
               concept_id = observation_concept_id,
               concept_name,
               start_date = observation_date,
               deficit,
               source_value = observation_source_value,
               person_start_date,
               person_end_date,
               vocabulary_id
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    # procedure table
    proc <- tbl(con, procedure_occurrence) |>
        inner_join(pid, by = "person_id", x_as = "x5", y_as = "y5") |>
        inner_join(concept_table_proc, by = c("procedure_source_value" = "icd_code"), x_as = "x4", y_as = "y4") |>
        left_join(tbl(con, concept), by = c("procedure_concept_id"="concept_id")) |>
        select(person_id,
               age_group,
               is_female,
               concept_id = procedure_concept_id,
               concept_name,
               start_date = procedure_date,
               deficit,
               source_value = procedure_source_value,
               person_start_date,
               person_end_date,
               vocabulary_id
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    # device exposure
    dev <- tbl(con, device_exposure) |>
        inner_join(pid, by = "person_id", x_as = "x7", y_as = "y7") |>
        inner_join(concept_table_proc, by = c("device_source_value" = "icd_code"), x_as = "x4", y_as = "y4") |>
        left_join(tbl(con, concept), by = c("device_concept_id"="concept_id")) |>
        select(person_id,
               age_group,
               is_female,
               concept_id = device_concept_id,
               concept_name,
               start_date = device_exposure_start_date,
               deficit,
               source_value = device_source_value,
               person_start_date,
               person_end_date,
               vocabulary_id
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    dat <-
        union_all(cond_occurrences, obs, dev, proc)

    return(dat)
}
