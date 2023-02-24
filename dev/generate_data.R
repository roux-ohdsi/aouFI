##### This script generates useful datasets #####



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


FI_labels =
        tribble(
            ~ "survey",               ~ "label",
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

# usethis::use_data(
#     survey_names,
#     FI_labels
# )
#
