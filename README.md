
<!-- README.md is generated from README.Rmd. Please edit that file -->

# aouRoux

<!-- badges: start -->
<!-- badges: end -->

R package of helper functions for the Roux OHDSI Frailty project

## Installation

You can install the development version of aouFI like so:

``` r
remotes::install_github("rbcavanaugh/aouFI)
```

## Current list of functions

- `getFI()` returns fralty scores for a several FIs given a dataframe of
  person_id’s, start, and end dates.

## Current list of useful tables

- `survey_names` list of surveys given in All of US
- `FI_labels` Paper-ready names for AouFI variables with a linking
  column for derived data
