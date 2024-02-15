#' OMOP Concept ID codes for different FI Indices
#'
#' A dataframe of concept ID values for vafi, egragicap, hfrs, and efi. There
#' are three versions of efi. One from an initial mapping attempt (from the
#' allofus project), one is a mapped list of snomed codes, and the third is a
#' mapped list of snomed codes AND all descendents extracted from an OMOP CDM.
#'
#' To get human readable names for each concept_id, join to an OMOP concept table.
#'
#' @format
#' A data frame with 71,917 rows and 4 columns:
#' \describe{
#'   \item{category}{FI category name (e.g., arthritis, visual impairment)}
#'   \item{concept_id}{OMOP concept ID for the fraily concept}
#'   \item{fi}{frailty index. efi (original efi), efi_sno (efi codes from snomed),
#'   efi_sno_expanded (efi codes from snomed with descendants), vafi, hfrs, efragicap.}
#'   \item{score}{1 if vafi, efi, or efragicap; the hfrs score otherwise}
#' }
"fi_indices"
