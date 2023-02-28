library(tibble)

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

# uncomment and run to overwrite and save data
usethis::use_data(
    FI_labels,
    overwrite = TRUE
)
