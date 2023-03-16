# efi concepts

efi_concepts = read.csv("~/Downloads/efi_scores_dictionary.csv") %>%
    rename(category = efi_category, concept_id = efi_concept_id)

usethis::use_data(efi_concepts, overwrite = TRUE)

