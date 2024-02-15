library(tidyverse)
library(ohdsilab)
library(keyring)
library(DatabaseConnector)
library(CDMConnector)
library(tidyverse)
library(ohdsilab)
library(aouFI)
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



# DB Connections
cdm_schema = "omop_cdm_53_pmtx_202203"
my_schema = paste0("work_", keyring::key_get("lab_user"))

# Create the connection
con =  DatabaseConnector::connect(dbms = "redshift",
                                  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
                                  port = 5439,
                                  user = keyring::key_get("lab_user"),
                                  password = keyring::key_get("lab_password"))

# defaults to help with querying
options(con.default.value = con)
options(schema.default.value = cdm_schema)
options(write_schema.default.value = my_schema)




options(scipen=999)
efi_sno <- read.csv("~/Downloads/efi_snomed.csv", colClasses = "character") |>
    select(1:4) |>
    mutate(Code = readr::parse_number(Code))

ohdsilab::map2omop(db_con = con, codes = efi_sno$Code, cdm_schema = cdm_schema, collect = TRUE, translate_from = "SNOMED") -> test

efi_sno |>
    mutate(source_code = as.character(Code)) |>
    left_join(test, by = c("source_code")) |>
    select(category = Codeset, concept_id) |>
    mutate(fi = "efi_sno", score = 1) -> efi_sno2


efi_scoring <- tbl(con, inDatabaseSchema(cdm_schema, "concept_ancestor")) %>%
    filter(ancestor_concept_id %in% !!efi_sno2$concept_id) %>%
    select(ancestor_concept_id, concept_id = descendant_concept_id) %>%
    collect() %>%
    left_join(efi_sno2, by = c("ancestor_concept_id" = "concept_id")) %>%
    select(2:5) %>%
    distinct() %>%
    mutate(fi = "efi_sno_expanded")

fi_indices = bind_rows(
    vafi2, efragicap2, efi2, hfrs2, efi_sno2, efi_scoring
)

usethis::use_data(fi_indices, overwrite = TRUE)

insertTable_chunk(aouFI::fi_indices, n = 1000, table_name = "fi_indices")

