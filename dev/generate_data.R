##### This script generates useful datasets #####
library(dplyr)
library(tidyr)
library(lubridate)
library(tibble)
library(aouFI)

# uncomment and run to overwrite and save data
usethis::use_data(
    survey_names,
    FI_labels,
    simulated_ehr,
    simulated_cohort,

    overwrite = TRUE
)


# names of the surveys in the AoU database
survey_names = tibble(
    names = c('Family History',
              'Personal Medical History',
              'COVID-19 Vaccine Survey',
              'Overall Health',
              'The Basics',
              'Lifestyle',
              'Healthcare Access & Utilization',
              'COVID-19 Participant Experience (COPE) Survey',
              'Social Determinants of Health'),
    compleation_rate_feb2023 = c(
        0.429,
        0.419,
        0.414,
        0.999,
        0.999,
        0.999,
        0.470,
        0.338,
        0.203),
    covid = c(FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE)
)

# Human readable labels for the FI variables in all_dat.csv
FI_labels =
    tribble(
        ~ "FI",               ~ "label",
        'activities',                "Everyday activities",
        'af',                        "Atrial fibrillation",
        'all.blind',                 "Vision impairment",
        'all.heart',                 "Coronary Artery Disease",
        'anxiety',                   "Anxiety",
        'arthritis',                 "Arthritis",
        'asthma',                    "Asthma",
        'cancer',                    "Cancer",
        'chronic.lung',              "Lung disease",
        'con.heart.fail',            "Heart failure",
        'dementia',                  "Dementia",
        'depression',                "Depression",
        'diabetes',                  "Diabetes",
        'diff.bathing',              "Difficulty bathing",
        'diff.concentration',        "Difficulty Concentrating",
        'diff.errands',              "Difficulty with errands",
        'diff.walk.climb',           "Difficulty walking/stairs",
        'emotional.7',               "Emotional problems",
        'fatigue.7',                 "Fatigue",
        'fractured.bone',            "Fractured bone",
        'gen.health',                "General health",
        'gen.mental.health',         "General mental health",
        'gen.social',                "General social health",
        'health.material.help_b',    "Health literacy",
        'hearing.impairment',        "Hearing impairment",
        'hypertension',              "Hypertension",
        'kidney',                    "Kidney disease",
        'osteoporosis',              "Osteoporosis",
        'pain.7',                    "Pain",
        'per.vas',                   "Peripheral vascular disease",
        'social.satis',              "Social satisfaction",
        'stroke.tia',                "Stroke / TIA",
        'transportation',            "Transportation for care"
    )

# fake example FI data

person_id = sample(seq(10000, 99999, 1), 1000, replace = FALSE)

obs_start_date = sample(seq(as.Date("2010-01-01"), as.Date("2016-12-31"), by = 1), 1000)
obs_end_date = obs_start_date + round(runif(n = 1000, 365, 365*5), 0)

person_table = bind_cols(
    person_id = person_id,
    obs_start_date = obs_start_date,
    obs_end_date = obs_end_date
)

simulated_ehr = person_table %>%
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



fi_dat = tidyr::drop_na(test, date) |> select(person_id, ffi_category, label, date)


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


