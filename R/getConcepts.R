#' Get concepts from different fraily Indices
#'
#' @param index character vector, one of "efi", "vafi", "efragicap", "hfrs"
#'
#' @description Funtion to retrieve table of OMOP concept_ids and their labels
#'
#' @return A tibble with columns labelled "category" and "concept_id"
#' @export
getConcepts <- function(index) {
  if (!(index %in% c("efi", "efragicap", "vafi", "hfrs"))) {
    stop("oops! frailty index not found.")
  }

  if (index == "efi") {

      tmp = get("efi_concepts") |>
                 distinct(
                     category,
                     concept_id)
    return(tmp)

  } else if (index == "efragicap") {

   tmp = get("efragicap_concepts") |>
              distinct(
                  category,
                  concept_id = efi_concept_id)
   return(tmp)

  } else if (index == "vafi") {

   tmp = get("vafi_concepts") |>
              distinct(
                category = vafi_category,
                concept_id = vafi_concept_id)
   return(tmp)

  } else if (index == "hfrs") {

   tmp = get("hfrs_concepts") |>
              distinct(
                  category = hfrs_category,
                  concept_id = hfrs_concept_id,
                  hfrs_score
              )
   return(tmp)

  }
}

