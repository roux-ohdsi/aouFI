##### This script generates useful datasets #####
library(tibble)

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

# uncomment and run to overwrite and save data
usethis::use_data(
    survey_names,
    overwrite = TRUE
)
