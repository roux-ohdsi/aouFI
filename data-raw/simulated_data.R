##### This script generates useful datasets #####
library(dplyr)
library(tidyr)
library(lubridate)
library(tibble)
library(aouFI)

# fake example FI data

person_id = sample(seq(10000, 99999, 1), 1000, replace = FALSE)

obs_start_date = sample(seq(as.Date("2010-01-01"), as.Date("2016-12-31"), by = 1), 1000)
obs_end_date = obs_start_date + round(runif(n = 1000, 365, 365*5), 0)

person_table = bind_cols(
    person_id = person_id,
    obs_start_date = obs_start_date,
    obs_end_date = obs_end_date
)

data(FI_labels, survey_names)

all_ehr = person_table %>%
    tidyr::expand_grid(FI_labels) %>%
    rename(fi_category = FI) %>%
    mutate(num_occurances = sample(c(rep(0, 25), rep(1, 10), rep(2, 4), rep(3, 2), 4), size = 33000, replace = TRUE),
           num_uncount = ifelse(num_occurances == 0, 1, num_occurances)) %>%
    tidyr::uncount(num_uncount) %>%
    rowwise() %>%
    mutate(date = sample(seq.Date(obs_start_date, obs_end_date, by = "day"), 1)) %>%
    ungroup() %>%
    mutate(date = replace(date, which(num_occurances == 0), NA))

prop_sample = 0.3

simulated_cohort = person_table %>%
    slice_sample(prop = prop_sample) %>%
    rowwise() %>%
    mutate(search_date = sample(seq.Date(obs_start_date, obs_end_date, by = "day"), 1)) %>%
    ungroup() %>%
    select(person_id, search_date)


simulated_ehr = tidyr::drop_na(all_ehr, date) |> select(person_id, fi_category, label, date)

# test
fi_efi = getFI(.data = simulated_ehr,
               weighted_fi = NA,
               person_id = "person_id",
               concept_id = "fi_category",
               concept_name = "label",
               concept_start_date = "date",

               .data_search = simulated_cohort,
               search_person_id = "person_id",
               search_start_date = "search_date",
               interval = 365,
               group_var = "person_id",
               summary = TRUE)

# uncomment and run to overwrite and save data
usethis::use_data(
    simulated_ehr,
    simulated_cohort,
    overwrite = TRUE
)

