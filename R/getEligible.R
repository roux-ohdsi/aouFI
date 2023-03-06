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
