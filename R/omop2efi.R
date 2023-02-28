




# pull everyone from the cohort builder table that is at least 50 at consent
# this is how we're defining people in all of us > 50
# but other datasets may want to
#' Get single column data frame of eligible IDs
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

#' OMOP to EFI
#'
#' @description Generates table of EFI-related condition occurances from an OMOP database.
#'
#' @param con
#'
#' @return dataframe with EFI occurences
#' @export
#'
#' @examples
#' con <- dbConnect(
#' bigrquery::bigquery(),
#'     billing = Sys.getenv('GOOGLE_PROJECT'),
#'     project = prefix,
#'     dataset = release
#'     )
#' omop2efi(con = con, eligible = getEligible())
#'
omop2efi <- function(con, eligible){

    # basic table of EFI concept numbers and labels
    categories_concepts <- getConcepts(index = "efi")

    # search and add all ancestors
     categories_concepts_and_ancestors <- tbl(con, "concept_ancestor") %>%
        filter(ancestor_concept_id %in% !!categories_concepts$concept_id) %>%
        select(ancestor_concept_id, concept_id = descendant_concept_id) %>%
        collect() %>%
        left_join(categories_concepts, by = c("ancestor_concept_id" = "concept_id")) %>%
        select(efi_category, efi_concept_id = concept_id) %>%
        distinct()

    # these are all the ancestors
    ancestors = tbl(con, "concept_ancestor") %>%
        filter(ancestor_concept_id %in% !!categories_concepts$concept_id) %>%
        select(concept_id = descendant_concept_id)

    # dataframe of eligible person_ids
    eligible <- getEligible(con)

    # get full list of concepts
    condition_concept_ids <- tbl(con, "concept") %>%
        filter(standard_concept == "S") %>%
        distinct(concept_id, name = concept_name, vocabulary_id) %>%
        inner_join(ancestors, by = "concept_id") %>%
        filter(concept_id %in% !!unique(categories_codes_and_ancestors$efi_concept_id)) %>%
        distinct()

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

    # do the same for the observation table
    obs <- tbl(con, "observation") %>%
        inner_join(eligible, by = "person_id") %>%
        inner_join(condition_concept_ids, by = c("observation_concept_id" = "concept_id")) %>%
        select(person_id,
               concept_id = observation_concept_id,
               concept_name = name,
               observation_datetime
        ) %>%
        mutate(start_year = year(observation_datetime),
               start_month = month(observation_datetime)) %>%
        select(-observation_datetime) %>%
        distinct() %>%
        collect()

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

    # put them all together, add the efi data back
    dat <-
        bind_rows(obs, cond, dev, proc) %>%
        left_join(categories_codes_and_ancestors,
                  by = c("concept_id" = "efi_concept_id"))

    return(dat)

}
