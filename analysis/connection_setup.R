# Packages
library(keyring)
library(DatabaseConnector)
library(CDMConnector)
library(tidyverse)
library(ohdsilab)
library(aouFI)
library(here)

# Credentials

# Either set here, or just save as strings as preferred. This is for the db connection
# usr = keyring::key_set("lab_user")
# pw  = keyring::key_set("lab_password")

usr = keyring::key_get("lab_user")
pw  = keyring::key_get("lab_password")

# DB Connections
base_url = "https://atlas.roux-ohdsi-prod.aws.northeastern.edu/WebAPI"
cdm_schema = "omop_cdm_53_pmtx_202203"
my_schema = paste0("work_", keyring::key_get("lab_user"))

# Create the connection
con =  DatabaseConnector::connect(dbms = "redshift",
                                  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
                                  port = 5439,
                                  user = keyring::key_get("lab_user"),
                                  password = keyring::key_get("lab_password"),
                                  pathToDriver = "D:/Users/r.cavanaugh/Documents"
                                  )


# check connection
class(con)

# defaults to help with querying
options(con.default.value = con)
options(schema.default.value = cdm_schema)
options(write_schema.default.value = my_schema)

