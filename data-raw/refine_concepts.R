library(tidyverse)

efi = aouFI::efi_concepts
vafi = aouFI::vafi_concepts
efragicap = aouFI::efragicap_concepts
hfrs = aouFI::hfrs_concepts

vafi2 = vafi |>
    select(category = vafi_category,
           concept_id = vafi_concept_id) |>
    distinct() |>
    mutate(fi = "vafi", score = 1)

efragicap2 = efragicap |>
    select(category, concept_id = efi_concept_id) |>
    distinct() |>
    mutate(fi = "efragicap", score = 1)

efi2 = efi |>
    select(category, concept_id) |>
    distinct() |>
    mutate(fi = "efi", score = 1)

hfrs2 = hfrs |>
    select(category = hfrs_category, concept_id = hfrs_concept_id, score = hfrs_score) |>
    distinct() |>
    mutate(fi = "hfrs")

fi_indices = bind_rows(
    vafi2, efragicap2, efi2, hfrs2
)

usethis::use_data(fi_indices)
