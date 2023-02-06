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
#' @param ... ; additional grouping variables for summarizing. Column name in .data_search (E.g., calculate by year.)
#'
#' @return A dataframe of FI indices or raw values
#' @export
#'
#' @examples \dontrun{
#' getFI(.data = efi_dat,
#' index = "efi",
#' .data_search = test_dat,
#' person_id = "person_id",
#' start_date = "start_date",
#' end_date = "end_date",
#' group_var = "person_id",
#' summary = TRUE)
#' }
getFI = function(.data, index,
                 .data_search, person_id, start_date,
                 interval = 365,

                 summary, group_var, rejoin = FALSE, ...){

    if(group_var != "person_id" & group_var != "category"){
        stop("OOPS! please select an allowable grouping variable")
    }

    index_var = dplyr::case_when(
        index == "efi" | index == "efragicap" ~ "efi",
        index == "vafi" ~ "vafi",
        index == "hfrs" ~ "hfrs",
        TRUE ~ NA_character_
    )

    if(paste(index_var, "category", sep = "_")  %in% colnames(.data)){
        cat("Data and selected index are compatible \n")
    } else {
        stop("OOPS! data and selected index are mismatched")
    }

    additional_groups = enquos(...)

    pid = .data_search  |>
        dplyr::select(personId = !!person_id,
                      startDate = !!start_date,
                      !!!additional_groups) |>
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
        dplyr::filter(person_id %in% pid$personId) |>
        dplyr::mutate(date = lubridate::ym(paste(start_year, start_month, sep = "-")),
                      obs = 1) |>
        dplyr::select(personId = person_id,
                      category = paste(!!index_var, "category", sep = "_"),
                      date,
                      score = ifelse(!!index_var == "hfrs", paste(!!index_var, "score", sep = "_"), "obs")

        ) |>
        dplyr::left_join(pid, by = "personId") |>
        mutate(score = ifelse(date %within% search_interval, score, 0))

    # if its a summary dataframe, summarize by person or category
    # HFRS adds up the scores, otherwise we're just counting rows

    cat("Generating distinct occurances for person_id/category combo... \n")
    tmp = tmp |>
        dplyr::distinct(personId, category, score, !!!additional_groups, .keep_all = TRUE)

    groupVar = ifelse(group_var == "person_id", "personId", "category")


    # if people in the search data were not in the efi data (no EHR obs)
    # then assume FI is zero? This feels risky...
    people_in_efi_dat = unique(.data$person_id)
    people_in_search_dat = unique(pid$personId)
    people_not_in_efi_dat = setdiff(people_in_search_dat, people_in_efi_dat)

    if(isTRUE(summary)){
        cat("Summarizing data ... \n")
        tmp = tmp |>
            dplyr::group_by(.data[[groupVar]], !!!additional_groups) |>
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
