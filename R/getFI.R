
#' Title
#'
#' @param .data data.frame; FI dataframe of all data (e.g., efi_dat.csv)
#' @param index chr; name of FI index (efi, vafi, hfrs)
#' @param .data_search data.frame; dataframe of participant IDs, start, and end dates to be searched
#' @param person_id chr; column name of person_id within .data_search dataframe
#' @param start_date chr; column name of start date column within .data_search. YYYY-MM-DD
#' @param end_date chr; column name of end date column within .data_search. YYYY-MM-DD
#' @param summary log; whether to return summary data or results broken out by FI concept
#' @param group_var chr; whether to summarize by "person" (default) or "category"
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
                 .data_search, person_id, start_date, end_date,
                 summary, group_var){

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

    pid = .data_search  |>
        dplyr::select(person_id = !!person_id,
               start_date = !!start_date,
               end_date = !!start_date) |>
        dplyr::mutate(search_interval = lubridate::interval(
                        lubridate::ymd(start_date), lubridate::ymd(end_date)
                )
            )



    cat("Searching for matching cases ... \n")

    # create a temporary data frame
    # fix the start and end date columns
    # rename columns to be generic
    # keep only rows within the interval
    tmp_clean = .data |>
        dplyr::filter(person_id %in% pid$person_id) |>
        dplyr::mutate(date = lubridate::ym(paste(start_year, start_month, sep = "-")),
               obs = 1) |>
        dplyr::select(person_id,
               category = paste(!!index_var, "category", sep = "_"),
               date,
               score = ifelse(!!index_var == "hfrs", paste(!!index_var, "score", sep = "_"), "obs")
        )

    cat()

    tmp = pid |> dplyr::left_join(tmp_clean, by = "person_id") |>
        mutate(score = if_else(condition = date %within% search_interval, true = score, false = 0))

    # pid_in <- tmp |>
    #     dplyr::filter(date %within% search_interval)
    #
    # print(head(pid_in))
    #
    # pid_notin <- tmp |>
    #     dplyr::filter(!(date %within% search_interval)) |>
    #     dplyr::mutate(score = 0)
    #
    # print(head(pid_notin))

    # tmp = dplyr::bind_rows(pid_in, pid_notin)


    # if its a summary dataframe, summarize by person or category
    # HFRS adds up the scores, otherwise we're just counting rows

    cat("Generating distinct occurances for person_id/category combo... \n")
    tmp = tmp |>
        dplyr::distinct(person_id, category, .keep_all = TRUE)

    if(isTRUE(summary)){
        cat("Summarizing data ... \n")
        tmp = tmp |>
            dplyr::group_by(.data[[group_var]]) |>
            dplyr::summarize(FI = sum(score)) |>
            dplyr::arrange(desc(FI))
    } else {
        cat("Note: grouping variable ignored when summary = FALSE \n")
        tmp = tmp |>
            dplyr::select(person_id, category, score) |>
            dplyr::arrange(person_id)
    }

    cat("Success!")
    return(tmp)

}
