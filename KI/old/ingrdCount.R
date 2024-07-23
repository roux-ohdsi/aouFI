
library(DatabaseConnector)
library(SqlRender)
library(dplyr)

#source(here::here("analysis", "connection_setup.R"))

con <- ohdsilab::ohdsilab_connect(username = key_get("db_username"), password = key_get("db_password"))
cdm_schema <- getOption("schema.default.value")
my_schema <- getOption("write_schema.default.value")


# cdm_schema = "imrd_emis_202212"
# cdm_schema = "imrd_uk_202209"
# cdm_schema = "ukbb_202303"
# my_schema <- paste0(cdm_schema, "_work")

exclude.df <- read.csv("KI/excludedIngrds.csv")
excluded_ingrd <- paste(exclude.df$concept_id, collapse = ", ")

sql <- readSql("KI/ingrdCount.sql")
sql <- render(sql, cdm_schema = cdm_schema,
              my_schema = my_schema,
              lookback_days = 365,
              excluded_ingrd = excluded_ingrd)

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                user = key_get("db_username"),
                                                                password = key_get("db_password"))

sql <- translate(sql, targetDialect = connectionDetails$dbms)

res <- querySql(connection = con, sql = sql)

frailty_cohort <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) |>
    select(person_id, age) |>
    dbi_collect()

disconnect(con)

colnames(res) <- tolower(colnames(res))

res <- merge(frailty_cohort, res, by = "person_id", all = T)
res[is.na(res)] <- 0

summary(res$cnt)

pp <- res %>%
    mutate(
        age_group = cut(age,
                        breaks = c(40, 45, 50, 55, 60, 65, 70, 75, 80, 120),
                        # breaks = c(40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 120),
                        right = FALSE,
                        include.lowest = TRUE),
        polypharmacy = ifelse(cnt < 5, 0, 1)
    )


pp_summary <- pp %>%
    group_by(age_group) %>%
    dplyr::summarize(pp_ratio = mean(polypharmacy, na.rm=TRUE))

write.csv(pp_summary, "KI/pp_summary.csv")
