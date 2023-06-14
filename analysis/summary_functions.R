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
        select(person_id, is_female, age_group, score, category) |>
        filter(category == fi_category) |> #print(head()) |>
        union_all(tmp) |>
        summarize(
            n_yes = sum(score, na.rm = TRUE),
            prop_yes = mean(score, na.rm = TRUE),
            .by = c(age_group, is_female, category)
        )
}

