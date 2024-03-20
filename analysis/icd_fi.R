
library(tidyverse)
library(ohdsilab)
library(DBI)
library(DatabaseConnector)
library(CDMConnector)

source(here::here("analysis", "connection_setup.R"))

# tbl(con, inDatabaseSchema(my_schema, "vafi_rev")) |> collect() -> vafi_rev
# vafi_rev = vafi_rev %>% mutate(concept_id = as.integer(concept_id))
# usethis::use_data(vafi_rev, overwrite = TRUE)

# Summary Functions. now in separate script
source(here::here("analysis", "summary_functions.R"))

# cdm_schema is the omop db
# my_schema is the user write schema

# ============================================================================
# COHORT

cohort_ids <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) |>
    distinct(person_id) |>
    head(50000)

cohort <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) %>%
    mutate(age = ifelse(yob_imputed == 1, 84, age)) %>%
    mutate(
        is_female = ifelse(gender_concept_id == 8507, 0, 1),
        age_group = cut(age,
                        breaks = c(40,  45,  50,  55, 60,  65,  70,  75,  80,  100),
                        right = FALSE,
                        include.lowest = TRUE),
        visit_lookback_date = !!CDMConnector::dateadd("index_date", -1, interval = "year")
    ) |>
    select(person_id, is_female, age_group, visit_lookback_date, index_date, yob_imputed)|>
    inner_join(cohort_ids, by = "person_id") |>
    group_by(person_id) |>
    filter(index_date == min(index_date))

# ============================================================================
# ICD VERSION

dat <- icd_fi()

dat2 = dat |>
    distinct(person_id, age_group, is_female, category = deficit) |>
    mutate(score = 1)

dat %>% distinct(deficit) %>% collect() -> u

#icd_c = dbi_collect(dat2)
# add robust individuals back
icd_all <- fi_with_robust(fi_query = dat2,
                           cohort = cohort,
                           denominator = 30, lb = 0.11, ub = 0.21)

# ============================================================================
# VAFI OMOP VERSION

vafi <- aouFI::omop2fi(con = con,
                       schema = cdm_schema,
                       index = "vafi",
                       .data_search = cohort,
                       search_person_id = "person_id",
                       search_start_date = "visit_lookback_date",
                       search_end_date = "index_date",
                       keep_columns = c("age_group", "is_female"),
                       collect = FALSE,
                       unique_categories = TRUE,
                       concept_location = tbl(con, inDatabaseSchema(my_schema, "vafi_rev"))
) |>
    distinct(person_id, age_group, is_female, score, category)


# save result of query as intermediate step #2
# CDMConnector::computeQuery(vafi, "vafi_fi",
#                            temporary = TRUE,
#                            schema = my_schema, overwrite = TRUE)

# add robust individuals back
vafi_all <- fi_with_robust(fi_query = vafi,
                           cohort = cohort,
                           denominator = 30, lb = 0.11, ub = 0.21)
# ============================================================================
# PLOT DIFFERENCES

icd_cats = dat2 |>
    summarize(sum_score = sum(score), .by = category) |>
    collect()

omop_cats = vafi |>
    summarize(sum_score = sum(score), .by = category) |>
    collect()

df_plot = left_join(omop_cats, icd_cats, by = c("category")) #%>% filter(category != "HTN")

plotly::ggplotly(df_plot |>
                     ggplot(aes(x = sum_score.x, y = sum_score.y, color = category)) +
                     geom_point() +
                     geom_abline() +
                     labs(x = "omop", y = "icd"))


df_plot_noHTN = left_join(omop_cats, icd_cats, by = c("category")) %>% filter(category != "HTN")

cor(df_plot$sum_score.x, df_plot$sum_score.y, use = "complete.obs")
cor(df_plot_noHTN$sum_score.x, df_plot_noHTN$sum_score.y, use = "complete.obs")

irr::icc(df_plot %>% select(-category), model = "oneway", type = "agreement")
irr::icc(df_plot_noHTN %>% select(-category), model = "oneway", type = "agreement")



# ============================================================================
# Do it with everyone just omop VAFI
cohort_ids <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) |>
    distinct(person_id)

cohort_all <- tbl(con, inDatabaseSchema(my_schema, "frailty_cohort")) %>%
    mutate(age = ifelse(yob_imputed == 1, 84, age)) %>%
    mutate(
        is_female = ifelse(gender_concept_id == 8507, 0, 1),
        age_group = cut(age,
                        breaks = c(40,  45,  50,  55, 60,  65,  70,  75,  80,  100),
                        right = FALSE,
                        include.lowest = TRUE),
        visit_lookback_date = !!CDMConnector::dateadd("index_date", -1, interval = "year")
    ) |>
    select(person_id, is_female, age_group, visit_lookback_date, index_date, yob_imputed)|>
    inner_join(cohort_ids, by = "person_id") |>
    group_by(person_id) |>
    filter(index_date == min(index_date))

vafi_all <- aouFI::omop2fi(con = con,
                       schema = cdm_schema,
                       index = "vafi",
                       .data_search = cohort_all,
                       search_person_id = "person_id",
                       search_start_date = "visit_lookback_date",
                       search_end_date = "index_date",
                       keep_columns = c("age_group", "is_female"),
                       collect = FALSE,
                       unique_categories = TRUE,
                       concept_location = tbl(con, inDatabaseSchema(my_schema, "vafi_rev"))
) |>
    distinct(person_id, age_group, is_female, score, category)


# save result of query as intermediate step #2
# CDMConnector::computeQuery(vafi_all, "vafi_fi",
#                            temporary = TRUE,
#                            schema = my_schema, overwrite = TRUE)

counts = cohort_all %>% ungroup() %>% count(is_female, age_group)

vafi_all %>%
    group_by(category, is_female, age_group) %>%
    summarize(total = sum(score)) %>%
    left_join(counts, by = c("is_female", "age_group")) %>%
    collect() -> test

categories = test %>% #filter(category %in% cats) %>%
    mutate(is_female = as.factor(is_female),
           prop = total/n)

categories %>%
    ggplot(aes(x = age_group, y = prop, fill = is_female, color = is_female, group = is_female)) +
    geom_point() + geom_line() + facet_wrap(~category) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
    scale_y_continuous(labels = scales::label_percent())

write.csv(categories, "KI/2024-03-13_pharmetrics_fi_categories.csv", row.names = FALSE)

# stopped here...

# add robust individuals back
vafi_all_summary <- fi_with_robust(
                            fi_query = vafi_all,
                           cohort = cohort,
                           denominator = 31, lb = 0.11, ub = 0.21)

# summarize
t = summarize_fi(vafi_all_summary) %>% collect()

write.csv(t, "results/2024-03-13_vafi_pharmetrics.csv", row.names = FALSE)
# ============================================================================

t2 = read.csv("results/2024-02-15_vafi_pharmetrics.csv")

t3 = left_join(t, t2, by = c("age_group", "is_female")) %>%
    pivot_longer(names_from = c())


left_join(
    t %>% pivot_longer(cols = n:frail),
    t2 %>% pivot_longer(cols = n:frail),
    by = c("age_group", "is_female", "name")) %>%
    rename("new" = value.x, "old" = value.y) %>%
    filter(name != "n") %>%
    pivot_longer(cols = new:old, names_to = "type", values_to = "value") %>%
    mutate(name = factor(name, levels = c("prefrail", "frail")),
           is_female = as.factor(is_female)) %>%
    ggplot(aes(x = age_group, y = value, color = type)) +
    geom_point() +
    geom_line() +
    facet_wrap(name ~ is_female)


# OTHJER STIUFF...trying to track down specific codes that are issues...har dto do
# because of the many to many relationships.

# make sure to keep concept_id in omop2fi() to get this to work.
vafi <- omop2fi2(con = con,
                 schema = cdm_schema,
                 index = "vafi",
                 .data_search = cohort,
                 search_person_id = "person_id",
                 search_start_date = "visit_lookback_date",
                 search_end_date = "index_date",
                 keep_columns = c("age_group", "is_female"),
                 collect = FALSE,
                 unique_categories = FALSE,
                 concept_location = tbl(con, inDatabaseSchema(my_schema, "vafi_rev"))
) |>
    select(person_id, category, concept_id)

icd_cats_count = dat %>% count(source_value, deficit, concept_id) %>% collect()
omop_cats_count = vafi %>% count(concept_id, category) %>% collect()

counts <- full_join(icd_cats_count %>% rename(category = deficit), omop_cats_count, by = c("concept_id", "category")) %>%
    mutate(icd_vs_omop = n.x-n.y) %>%
    rename(icd_count = n.x, omop_count = n.y) %>%
    add_count(concept_id, category) %>%
    group_by(concept_id, category) %>%
    summarize(total_omop = sum(omop_count),
              total_icd = sum(icd_count),
              n = mean(n)) %>%
    ungroup() %>%
    mutate(diff = total_omop/n-total_icd)

