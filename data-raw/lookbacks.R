

lb = tibble::tribble(
    ~fi,                           ~category, ~lookback,
    "vafi",                               "PVD",         3,
    "vafi",                            "Anemia",         1,
    "vafi",                              "Lung",         3,
    "vafi",                             "Liver",         3,
    "vafi",                         "Arthritis",         3,
    "vafi",                             "Osteo",         3,
    "vafi",                            "Kidney",         1,
    "vafi",                           "DuraMed",         1,
    "vafi",                            "Cancer",         1,
    "vafi",                           "Hearing",         1,
    "vafi",                          "Dementia",         3,
    "vafi",                                "HF",         3,
    "vafi",                               "CVA",         3,
    "vafi",                          "Diabetes",         3,
    "vafi",                            "Vision",         1,
    "vafi",                               "CAD",         3,
    "vafi",                              "AFIB",         1,
    "vafi",                           "Depress",         1,
    "vafi",                             "Falls",         1,
    "vafi",                           "Thyroid",         3,
    "vafi",                           "Anxiety",         1,
    "vafi",                          "PerNeuro",         3,
    "vafi",                                "PD",         3,
    "vafi",                           "Fatigue",         1,
    "vafi",                               "HTN",         3,
    "vafi",                         "Chronpain",         1,
    "vafi",                            "Incont",         1,
    "vafi",                            "GaitAb",         1,
    "vafi",                          "Muscular",         1,
    "vafi",                           "WgtLoss",         1,
    "vafi",                         "FtoThrive",         1,
    "efi",               "Activity limitation",         1,
    "efi", "Anaemia and haematinic deficiency",         1,
    "efi",                         "Arthritis",         3,
    "efi",               "Atrial fibrillation",         1,
    "efi",           "Cerebrovascular disease",         3,
    "efi",            "Chronic kidney disease",         3,
    "efi",                          "Diabetes",         3,
    "efi",                         "Dizziness",         1,
    "efi",                          "Dyspnoea",         1,
    "efi",                             "Falls",         1,
    "efi",                     "Foot problems",         1,
    "efi",                "Fragility fracture",         3,
    "efi",                "Hearing impairment",         1,
    "efi",                     "Heart failure",         3,
    "efi",               "Heart valve disease",         3,
    "efi",                        "Housebound",         1,
    "efi",                      "Hypertension",         3,
    "efi",             "Hypotension / syncope",         1,
    "efi",           "Ischaemic heart disease",         3,
    "efi",     "Memory and cognitive problems",         3,
    "efi",    "Mobility and transfer problems",         1,
    "efi",                      "Osteoporosis",         3,
    "efi",           "Parkinsonism and tremor",         3,
    "efi",                      "Peptic ulcer",         1,
    "efi",       "Peripheral vascular disease",         3,
    "efi",                      "Polypharmacy",         1,
    "efi",              "Requirement for care",         1,
    "efi",               "Respiratory disease",         3,
    "efi",                        "Skin ulcer",         1,
    "efi",                 "Sleep disturbance",         3,
    "efi",              "Social vulnerability",         1,
    "efi",                   "Thyroid disease",         3,
    "efi",              "Urinary incontinence",         1,
    "efi",            "Urinary system disease",         1,
    "efi",                 "Visual impairment",         1,
    "efi",          "Weight loss and anorexia",         1
)

usethis::use_data(lb)
