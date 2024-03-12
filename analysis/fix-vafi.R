


# Generate a revised VAFI
# start with the VAFI codes on github
# map them from ICD10 to OMOP
# use Lily's document to weed on bad codes
# weed out some more bad codes for diabetes
# weed out some generalist bad codes by hand
# save new vafi to databse.
# next script (icd_fi.R) will do the following:
# find FI codes searching via the ICD10 codes and separately using the OMOP codes
# check correlations and agreements (scatterplot) between ICD and OMOP
# FI in a sample of 50k from the P+ frailty cohort
# once satisfied, do it again with the full frailty cohort.
#

library(tidyverse)
library(ohdsilab)
library(DBI)
library(DatabaseConnector)
library(CDMConnector)
library(keyring)

#source(here::here("analysis", "connection_setup.R"))

con <- ohdsilab::ohdsilab_connect(username = key_get("db_username"), password = key_get("db_password"))
cdm_schema <- getOption("schema.default.value")
write_schema <- getOption("write_schema.default.value")

# tbl(con, inDatabaseSchema(write_schema, "vafi_rev")) %>% dbi_collect() -> vafi
# write.csv(vafi, "results/vafi_rev_03-12-24.csv", row.names = FALSE)


vafi = read.csv("https://raw.githubusercontent.com/bostoninformatics/va_frailty_index/main/icd_code_vafi_mapping.csv", stringsAsFactors = TRUE) |>
    mutate(Code = str_remove(Code, "[.]$")) |> filter(CodeType == "ICD10")

vafi_proc = read.csv("https://raw.githubusercontent.com/bostoninformatics/va_frailty_index/main/procedure_code_vafi_mapping.csv", stringsAsFactors = TRUE)|>
    mutate(Code = str_remove(Code, "[.]$")) |> filter(CodeType != "ICD9Procedure")

all_deficit_codes <- bind_rows(vafi, vafi_proc) %>%
    distinct()

# ohdsilab::insertTable_chunk(all_deficit_codes, "vafi")

testtbl = tbl(con, inDatabaseSchema(write_schema, "vafi"))

# get icd concept id
source_codes <- dplyr::tbl(con, inDatabaseSchema(cdm_schema, "concept")) %>%
    dplyr::filter(vocabulary_id == "ICD10CM" | vocabulary_id == "ICD10PCS" ) %>%
    dplyr::left_join(testtbl,
                     sql_on = "concepts.concept_code LIKE my.code",
                     x_as = "concepts", y_as = "my") %>%
    dplyr::filter(!is.na(code)) %>%
    dplyr::select(concept_id, source_concept_name = concept_name, source_vocabulary_id = vocabulary_id,
                  source_code = concept_code, code, deficit)

source_codes |> head()

# just the snomed codes
target_codes <- dplyr::tbl(con, inDatabaseSchema(cdm_schema, "concept")) %>%
    dplyr::filter(vocabulary_id == "SNOMED") %>%
    dplyr::select(concept_id, target_concept_name = concept_name, target_vocabulary_id = vocabulary_id)

target_codes |> head()

relationships <- tbl(con, inDatabaseSchema(cdm_schema, "concept_relationship")) %>%
    filter(relationship_id == "Maps to") %>%
    inner_join(source_codes, by = c("concept_id_1" = "concept_id"), x_as = "relationships", y_as = "source") %>%
    inner_join(target_codes, by = c("concept_id_2" = "concept_id"), y_as = "target") %>%
    rename(concept_id = concept_id_2,
           orig_concept_id = concept_id_1) %>%
    collect() %>%
    select(orig_concept_id,  source_concept_name, vafi_code = code, deficit, concept_id, target_concept_name)# %>%
    # in case there were any codes that happened to match but shouldn't have
    #inner_join(all_deficit_codes, by = c("source_code" = "Code", "source_vocabulary_id" = "vocabulary_id"))

relationships |> head()

# fix for eye codes in diabetes
fix_dm <- relationships %>%
    filter(deficit == "Diabetes", str_detect(target_concept_name, "diabetes|Diabetic|diabetic|Diabetes|Hyperosmolarity|hyperosmolarity", negate = TRUE)) %>%
    mutate(bad_match = 1) %>%
    distinct(concept_id, deficit, bad_match)

# fix for codes reviewed by Lily
bad_matches = readxl::read_excel(here::here("vafi_multi_reviewed.xlsx")) %>%
    filter(bad_match == 1) %>%
    distinct(concept_id, deficit, bad_match) %>%
    bind_rows(fix_dm)%>%
    distinct(concept_id, deficit, bad_match)

# manual fixes for annoying and uninformative OMOP Codes
vafi_fixed <- relationships %>%
    left_join(bad_matches, by = c("concept_id", "deficit")) %>%
    filter(is.na(bad_match),
           concept_id != 4215685, # Past history of procedure
           concept_id != 4214956, # History of clinical finding in subject
           concept_id != 4203722, # patient encounter procedure
           concept_id != 4081759, # transplant followup procedure
           concept_id != 4322175, # late effects of complications of procedure
           concept_id != 372448,  # LOC
           concept_id != 4306655, # death
           concept_id != 4323344, # no LOC
           concept_id != 381966,  # moderate LOC
           concept_id != 4153217, # patienet condition resolved
           concept_id != 435119,  # late effect of injury
           concept_id != 380844   # prolonged loc
    ) %>%
    select(-bad_match)

vafi_final = vafi_fixed |>
    distinct(category = deficit, concept_id) |>
    mutate(score = 1, fi = "vafi")

ohdsilab::insertTable_chunk(vafi_final, "vafi_rev")






