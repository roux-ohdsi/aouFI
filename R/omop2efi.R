




# pull everyone from the cohort builder table that is at least 50 at consent
# this is how we're defining people in all of us > 50
# but other datasets may want to

#' @title Get Eligible FI cohort
#'
#' @description Get single column data frame of eligible IDs named person_id
#'
#' @param con
#'
#' @return dataframe of elgible IDs
#' @export
getEligible <- function(con){
    tbl(con, "cb_search_person") %>%
    filter(age_at_consent >= 50 & age_at_consent <= 120) %>%
    # just hold on to all the unique ids for an innerjoin later
    distinct(person_id)
}

#' @title OMOP to EFI
#'
#' @description Generates table of EFI-related condition occurances from an OMOP database.
#' This requires a database connection using dbConnect() or similar R package
#' The connection should be able to access the following OMOP CDM tables
#'
#' * concept_ancestor
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
#' participatns older than 50 and younger than 120.
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
#' omop2efi(con = con, eligible = aouFI::getEligible())
#'
omop2efi <- function(con, eligible){

    message("retrieving concepts...")

    # basic table of EFI concept numbers and labels
    categories_concepts <- getConcepts(index = "efi")

    # search and add all ancestors
     categories_concepts_and_ancestors <- tbl(con, "concept_ancestor") %>%
        filter(ancestor_concept_id %in% !!categories_concepts$concept_id) %>%
        select(ancestor_concept_id, concept_id = descendant_concept_id) %>%
        collect() %>%
        left_join(categories_concepts, by = c("ancestor_concept_id" = "concept_id")) %>%
        select(efi_category = category, efi_concept_id = concept_id) %>%
        distinct()

     message("finding ancestors...")

    # these are all the ancestors
    ancestors = tbl(con, "concept_ancestor") %>%
        filter(ancestor_concept_id %in% !!categories_concepts$concept_id) %>%
        select(concept_id = descendant_concept_id)


    message("joining full concept id list...")

    # get full list of concepts
    condition_concept_ids <- tbl(con, "concept") %>%
        filter(standard_concept == "S") %>%
        distinct(concept_id, name = concept_name, vocabulary_id) %>%
        inner_join(ancestors, by = "concept_id") %>%
        filter(concept_id %in% !!unique(categories_concepts_and_ancestors$efi_concept_id)) %>%
        distinct()


    message("searching for condition occurrences...")

    # go find instances of our concepts in the condition occurrence table
    cond_occurances <- tbl(con, "condition_occurrence") %>%
        inner_join(eligible, by = "person_id") %>%
        inner_join(condition_concept_ids, by = c("condition_concept_id" = "concept_id")) %>%
        select(person_id,
               concept_id = condition_concept_id,
               concept_name = name,
               condition_start_datetime,
               condition_end_datetime,
               stop_reason
        ) %>%
        mutate(start_year = year(condition_start_datetime),
               start_month = month(condition_start_datetime),
               end_year = year(condition_end_datetime),
               end_month = month(condition_end_datetime)) %>%
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
        )  %>%
        distinct() %>%
        collect() %>%
        mutate(start_year = year(observation_datetime),
               start_month = month(observation_datetime)) %>%
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
        mutate(start_year = year(procedure_datetime),
               start_month = month(procedure_datetime)) %>%
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
        mutate(start_year = year(device_exposure_start_datetime),
               start_month = month(device_exposure_start_datetime)) %>%
        select(-device_exposure_start_datetime) %>%
        distinct() %>%
        collect()


    message("putting it all together...")

    # put them all together, add the efi data back
    dat <-
        bind_rows(cond_occurances, obs, dev, proc) %>%
        left_join(categories_concepts_and_ancestors,
                  by = c("concept_id" = "efi_concept_id"))

    message("Success!!")
    return(dat)

}
