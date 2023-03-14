#' Title
#'
#' @param .data data.frame; FI dataframe of all data (e.g., efi_dat.csv)
#' @param index chr; name of FI index (efi, vafi, hfrs)
#' @param .data_search data.frame; dataframe of participant IDs, start, and end dates to be searched
#' @param person_id chr; column name of person_id within .data_search dataframe
#' @param start_date chr; column name of start date column within .data_search. YYYY-MM-DD
#' @param interval int; number of days to search from start date. defaults to 365
#' @param summary log; whether to return summary data or results broken out by FI concept
#' @param group_var chr; whether to summarize by "person" (default) or "category"
#' @param rejoin log; if TRUE, will join results back to .data_search. defaults to FALSE
#'
#' @return A dataframe of FI indices or raw values
#' @export
getFI = function(
                .data, weighted_fi = NA,
                person_id = "person_id",
                concept_id = "concept_id",
                category = "category",
                concept_start_date = "start_date",

                 .data_search,
                 search_person_id,
                 search_start_date,
                 interval = 365,

                 summary, group_var, rejoin = FALSE){

    if(group_var != "person_id" & group_var != "category"){
        stop("OOPS! please select an allowable grouping variable")
    }

    # index_var = dplyr::case_when(
    #     index == "efi" | index == "efragicap" ~ "efi",
    #     index == "vafi" ~ "vafi",
    #     index == "hfrs" ~ "hfrs",
    #     TRUE ~ NA_character_
    # )

    # if(paste(index_var, "category", sep = "_")  %in% colnames(.data)){
    #     cat("Data and selected index are compatible \n")
    # } else {
    #     stop("OOPS! data and selected index are mismatched")
    # }

    pid = .data_search  |>
        dplyr::select(personId = !!search_person_id,
                      startDate = !!search_start_date) |>
        dplyr::mutate(
            endDate = startDate + !!interval,
            search_interval = lubridate::interval(
                lubridate::ymd(startDate), lubridate::ymd(endDate)
            )
        )



    cat("Searching for matching cases ... \n")

    # create a temporary data frame
    # fix the start and end date columns
    # rename columns to be generic
    # keep only rows within the interval
    tmp = .data |>
        rename(
            personId = !!person_id,
            conceptId = !!concept_id,
            categoryName = !!category,
            date = !!concept_start_date
        ) |>
        dplyr::filter(personId %in% pid$personId) |>
        dplyr::mutate(score = ifelse(is.na(!!weighted_fi), 1, .data[[weighted_fi]])) |>
        dplyr::left_join(pid, by = "personId") |>
        dplyr::mutate(score = ifelse(date %within% search_interval, score, 0)) |>
        dplyr::select(personId,
               conceptId,
               categoryName,
               date,
               score,
               startDate,
               endDate,
               searchInterval)

    # if its a summary dataframe, summarize by person or category
    # HFRS adds up the scores, otherwise we're just counting rows

    cat("Generating distinct occurances for person_id/category combo... \n")
    tmp = tmp |>
        dplyr::distinct(personId, categoryName, score)

    groupVar = ifelse(group_var == "person_id", "personId", "categoryName")


    # if people in the search data were not in the efi data (no EHR obs)
    # then assume FI is zero? This feels risky...
    people_in_efi_dat = unique(.data$person_id)
    people_in_search_dat = unique(pid$personId)
    people_not_in_efi_dat = setdiff(people_in_search_dat, people_in_efi_dat)

    if(isTRUE(summary)){
        cat("Summarizing data ... \n")
        tmp = tmp |>
            dplyr::group_by(.data[[groupVar]]) |>
            dplyr::summarize(FI = sum(score)) |>
            dplyr::arrange(desc(FI)) |>
            rename(person_id = personId) |>
            mutate(found_ehr = 1) |>
            bind_rows(
                tibble(
                    person_id = people_not_in_efi_dat,
                    FI = 0,
                    found_ehr = 0
                )
            )
    } else {
        cat("Note: grouping variable ignored when summary = FALSE \n")
        tmp = tmp |>
            dplyr::select(personId, category, score) |>
            dplyr::arrange(personId) |>
            rename(person_id = personId)
    }

    if(isTRUE(rejoin)){
        cat("Joining results back to original data \n")

    }

    cat("Success!")
    return(tmp)

}



