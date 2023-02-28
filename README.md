
<!-- README.md is generated from README.Rmd. Please edit that file -->

# aouRoux

<!-- badges: start -->
<!-- badges: end -->

R package of helper functions for the Roux OHDSI Frailty project

## Installation

You can install the development version of aouFI like so:

``` r
remotes::install_github("roux-ohdsi/aouFI)
```

## Current list of functions

- `getFI()` returns fralty scores for a several FIs given a dataframe of
  person_id’s, start, and end dates. It requires a specific data input
  and is not ready for sharing yet.
- `getEligible()` gets a dataframe of person_ids from the AoU database
  (when provided with a database connection) that are \> 50 years old
  and \< 120 years old
- `omop2efi()` returns a dataframe of EFI (electronic frailty index)
  occurrences when given a database connection to any (in theory) OMOP
  CDM database and a table of person_id values (e.g., from
  `getEligible()`).

## Current list of useful tables

- `survey_names` List of surveys given in All of US
- `FI_labels` Paper-ready names for AouFI variables with a linking
  column for derived data
- `simulated_ehr` Simulated ehr data to search - similar to what would
  be returned by `omop2efi()`. This will be used for testing and demo
  purposes. It can be used with getFI(). The values in this data are
  entirely fabricated based on code in /data-raw
- `simulated_cohort` Simulated cohort with a starting search date. Can
  be used with `simulated_ehr` to use the `getFI()` function. It
  contains a sample of fake participants in simulated_ehr with randomly
  generated start dates in the observation period for each person_id.
