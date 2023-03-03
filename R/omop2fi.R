
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
omop2fi <- function(con, eligible, index){

    index_ = tolower(index)

    if(!(index_ %in% c("efi", "efragicap", "hfrs", "vafi"))){
        stop("oops! frailty index not found; index must be one of: efi, efragicap, hfrs, or vafi.")
    }

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
    condition_concept_ids <- tbl(con, "concept") %>%
        filter(standard_concept == "S") %>%
        distinct(concept_id, name = concept_name, vocabulary_id) %>%
        filter(concept_id %in% !!unique(categories_concepts$concept_id)) %>%
        distinct()

    message("searching for condition occurrences...")

    # The following four calls go find the presence of the concept IDs in the
    # condition occurrence, procedure, observation, and device tabes, limiting
    # the search to the person_ids and concepts in teh above condition_concept_ids
    # table. They also calculate a start year and month which are important for
    # later analyses that are dependent on when the FI event occurs.

    # go find instances of our concepts in the condition occurrence table
    cond_occurrences <- tbl(con, "condition_occurrence") %>%
        inner_join(eligible, by = "person_id") %>%
        inner_join(condition_concept_ids, by = c("condition_concept_id" = "concept_id")) %>%
        select(person_id,
               concept_id = condition_concept_id,
               concept_name = name,
               condition_start_datetime,
               condition_end_datetime,
               stop_reason
        ) %>%
        mutate(start_year = lubridate::year(condition_start_datetime),
               start_month = lubridate::month(condition_start_datetime),
               end_year = lubridate::year(condition_end_datetime),
               end_month = lubridate::month(condition_end_datetime)) %>%
        select(-condition_start_datetime, -condition_end_datetime) %>%
        distinct() %>%
        collect()


    message("searching for observations...")

    # do the same for the observation table
    obs <- tbl(con, "observation") %>%
        inner_join(eligible, by = "person_id") %>%
        inner_join(condition_concept_ids, by = c("observation_concept_id" = "concept_id")) %>%
        select(person_id,
               concept_id = observation_concept_id,
               concept_name = name,
               observation_datetime
        ) %>%
        distinct() %>%
        collect() %>%
        # this had to get moved to after the collect because hfrs returns some odd
        # rows from the observation table which bigquery doesn't like...¯\_(ツ)_/¯
        mutate(start_year = as.integer(lubridate::year(observation_datetime)),
                        start_month = as.integer(lubridate::month(observation_datetime))) %>%
        select(-observation_datetime)

    message("searching for procedures...")

    # procedure table
    proc <- tbl(con, "procedure_occurrence") %>%
        inner_join(eligible, by = "person_id") %>%
        inner_join(condition_concept_ids, by = c("procedure_concept_id" = "concept_id")) %>%
        select(person_id,
               concept_id = procedure_concept_id,
               concept_name = name,
               procedure_datetime
        ) %>%
        mutate(start_year = lubridate::year(procedure_datetime),
               start_month = lubridate::month(procedure_datetime)) %>%
        select(-procedure_datetime) %>%
        distinct() %>%
        collect()


    message("searching for device exposures...")

    # device exposure
    dev <- tbl(con, "device_exposure") %>%
        inner_join(eligible, by = "person_id") %>%
        inner_join(condition_concept_ids, by = c("device_concept_id" = "concept_id")) %>%
        select(person_id,
               concept_id = device_concept_id,
               concept_name = name,
               device_exposure_start_datetime
        ) %>%
        mutate(start_year = lubridate::year(device_exposure_start_datetime),
               start_month = lubridate::month(device_exposure_start_datetime)) %>%
        select(-device_exposure_start_datetime) %>%
        distinct() %>%
        collect()


    message("putting it all together...")

    # we will need to join the concept IDs and labels back to the events
    # after the four tables above are combined.

    # if the index is hfrs we also need to return the point value "score"
    if(index_ == "hfrs"){
        categories_concepts <- categories_concepts %>%
            mutate(concept_id = as.integer(concept_id)) %>%
            distinct(concept_id, category, hfrs_score)
    } else { # but not for the other three indices
        categories_concepts <- categories_concepts %>%
            mutate(concept_id = as.integer(concept_id)) %>%
            distinct(concept_id, category)
    }

    # put them all together, add the fi labels back
    dat <-
        bind_rows(cond_occurrences, obs, dev, proc) %>%
        left_join(categories_concepts, by = c("concept_id"))

    message(glue::glue("success! retrieved {nrow(dat)} records."))

    return(dat)

}
