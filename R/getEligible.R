# pull everyone from the cohort builder table that is at least 50 at consent
# this is how we're defining people in all of us > 50
# but other datasets may want to
#' Get single column data frame of eligible IDs
#'
#' @param con connection to AoU database
#' @param lower lower bound age limit, inclusive. default 50
#' @param upper upper bound age limit, inclusive. default 120
#'
#' @return sql query for elgible IDs
#' @export
getEligible <- function(con, lower = 50, upper = 120){
    tbl(con, "cb_search_person") %>%
        filter(age_at_consent >= lower & age_at_consent <= upper) %>%
        # just hold on to all the unique ids for an innerjoin later
        distinct(person_id)
}
