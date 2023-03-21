
#' @title OMOP to FI
#'
#' @description Generates table of condition occurances from an OMOP database for
#' the EFI, EGRAGICAP, VAFI, and HFRS frailty indices.
#' This requires a database connection using dbConnect() or similar R package
#' The connection should be able to access the following OMOP CDM tables
#'
#' * concept
#' * condition_occurrence
#' * observation
#' * procedure_occurrence
#' * device_exposure
#' @md
#'
#' @param con a database connectino using dbConnect() or similar
#' @param eligible a dataframe or tibble with a single column of unique person_id values. Obtained
#' in the all of us databse using getEligible() which returns a single column dataframe with
#' participatns older than 50 and younger than 120. For other datasources, will need to be custom made.
#' @param index chr vector; a frailty index. One of "efi", "efragicap", "vafi", or "hfrs"
#' @param schema chr vector: a character vector of the schema holding the table. defaults to NULL (no schema)
#' @param collect log; should the query be collected at the end or kept as an SQL query? This must be TRUE for bigquery
#' because bigquery does not permit temporary tables.
#'
#' @return dataframe with EFI occurences that can be summarized into an EFI using aouFI::getFI()
#' @export
#'
#' @examples
#' con <- dbConnect(
#' bigrquery::bigquery(),
#'     billing = Sys.getenv('GOOGLE_PROJECT'),
#'     project = prefix,
#'     dataset = release
#'     )
#' omop2fi(con = con, eligible = aouFI::getEligible(), index = "efi")
#'
omop2fi <- function(con,
                    index,
                    schema = NULL,
                    collect = FALSE,

                    .data_search,
                    search_person_id,
                    search_start_date,
                    search_end_date,
                    keep_columns = NULL
                    ){

    keep_cols <- {{keep_columns}}

    if(!is.null(schema)){

        if(!is.character(schema)){stop("schema must be a character vector")}

        concept                 = paste(schema, "concept", sep = ".")
        condition_occurrence    = paste(schema, "condition_occurrence", sep = ".")
        observation             = paste(schema, "observation", sep = ".")
        procedure_occurrence    = paste(schema, "procedure_occurrence", sep = ".")
        device_exposure         = paste(schema, "device_exposure", sep = ".")
        person                  = paste(schema, "person")

    } else {
        concept                 = "concept"
        condition_occurrence    = "condition_occurrence"
        observation             = "observation"
        procedure_occurrence    = "procedure_occurrence"
        device_exposure         = "device_exposure"
        person                  = "person"
    }

    if(!("person_id" %in% colnames(eligible))){stop("eligible must contain person_id column")}

    index_ = tolower(index)

    if(!(index_ %in% c("efi", "efragicap", "hfrs", "vafi"))){
        stop("oops! frailty index not found; index must be one of: efi, efragicap, hfrs, or vafi.")
    }


     pid = .data_search |>
                dplyr::select(person_id = !!search_person_id,
                              person_start_date = !!search_start_date,
                              person_end_date = !!search_end_date, !!!keep_cols)


    message(glue::glue("retrieving {index} concepts..."))

    # gets a dataframe with columns of category (general description of the FI category)
    # and concept_id which is an OMOP concept ID for the FI category. FI categories can
    # be repeated with different concept IDs, but there should be no concept_id duplicates.
    # If the index is hfrs, there is an additional column called 'hfrs_score' which
    # holds the point value for the hfrs concept.
    # This function is documented in the package and pulls from data sources we generated
    # using the AoU tables. Code for generating these tables can be made available.
    categories_concepts <- getConcepts(index = index_)

    message("joining full concept id list...")

    # get full list of concepts from the concept table
    # originally, this pulled from the All of Us cb_cohort and other
    # tables specific to all of us. Switching to the general concept table
    # made very little, except for hfrs. There were 51 additional concepts in the
    # concept table that were not in the AoU table. We're still not sure what the
    # difference is, but perhaps related to the is_selectable aspect of AoU...
    condition_concept_ids <- tbl(con, concept) |>
        filter(standard_concept == "S") |>
        distinct(concept_id, name = concept_name) |> # vocabulary_id
        filter(concept_id %in% !!unique(categories_concepts$concept_id))

    message("searching for condition occurrences...")

    # The following four calls go find the presence of the concept IDs in the
    # condition occurrence, procedure, observation, and device tabes, limiting
    # the search to the person_ids and concepts in teh above condition_concept_ids
    # table. They also calculate a start year and month which are important for
    # later analyses that are dependent on when the FI event occurs.

    # go find instances of our concepts in the condition occurrence table
    cond_occurrences <- tbl(con, condition_occurrence) |>
        inner_join(pid, by = "person_id") |>
        inner_join(condition_concept_ids, by = c("condition_concept_id" = "concept_id")) |>
        select(person_id,!!!keep_cols,
               concept_id = condition_concept_id,
               concept_name = name,
               start_date = condition_start_date,
               person_start_date,
               person_end_date
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    message("searching for observations...")

    # do the same for the observation table
    obs <- tbl(con, observation)  |>
        inner_join(pid, by = "person_id") |>
        inner_join(condition_concept_ids, by = c("observation_concept_id" = "concept_id")) |>
        select(person_id,!!!keep_cols,
               concept_id = observation_concept_id,
               concept_name = name,
               start_date = observation_date,
               person_start_date,
               person_end_date
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()

    message("searching for procedures...")

    # procedure table
    proc <- tbl(con, procedure_occurrence) |>
        inner_join(pid, by = "person_id") |>
        inner_join(condition_concept_ids, by = c("procedure_concept_id" = "concept_id")) |>
        select(person_id,!!!keep_cols,
               concept_id = procedure_concept_id,
               concept_name = name,
               start_date = procedure_date,
               person_start_date,
               person_end_date
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()


    message("searching for device exposures...")

    # device exposure
    dev <- tbl(con, device_exposure) |>
        inner_join(pid, by = "person_id") |>
        inner_join(condition_concept_ids, by = c("device_concept_id" = "concept_id")) |>
        select(person_id,!!!keep_cols,
               concept_id = device_concept_id,
               concept_name = name,
               start_date = device_exposure_start_date,
               person_start_date,
               person_end_date,
        ) |>
        filter(start_date >= person_start_date, start_date <= person_end_date) |>
        distinct()


    message("putting it all together...")

    # we will need to join the concept IDs and labels back to the events
    # after the four tables above are combined.

    # if the index is hfrs we also need to return the point value "score"
    if(index_ == "hfrs"){
        categories_concepts <- categories_concepts |>
            mutate(concept_id = as.integer(concept_id)) |>
            distinct(concept_id, category, hfrs_score)
    } else { # but not for the other three indices
        categories_concepts <- categories_concepts |>
            mutate(concept_id = as.integer(concept_id)) |>
            distinct(concept_id, category)
    }

    # put them all together, add the fi labels back
    dat <-
        union_all(cond_occurrences, obs, dev, proc)

    # Logic to determine whether a collected df or just a query should be returned.
    # note that copying if false can still take some time...
    if(isTRUE(collect)){
        message("collecting...")
        dat <- dat |>
            collect() |>
            left_join(categories_concepts, by = c("concept_id"))
        message(glue::glue("success! retrieved {nrow(dat)} records."))
    } else {
        message("copying...")
        dat <- dat |>
            left_join(categories_concepts, by = c("concept_id"), copy = TRUE)
        message(glue::glue("success! SQL query from dbplyr returned"))
    }



    return(dat)

}
