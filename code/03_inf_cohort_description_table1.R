########################################################################################
## TITLE: Table 1 for CCU002_07 project
##
## Description: Children characteristics in both cohorts
##
## Code developed and updated by Alexia Sampri
## 
#######################################################################################

library(utils)
library(dplyr)
library(readr)
library(tidyverse)
library(lubridate)
library(survival)
library(broom)
library(scales)

# setwd("D:/PhotonUser/My Files/Home Folder/CCU002_07/infection/riskratio_inf")
# load =========================================================================

#input_inf <- readRDS("./cox_inf_clean.rds")
#input_inf <- readRDS("./cox_inf_ecds_clean.rds")
input_inf <-  readRDS("/db-mnt/databricks/rstudio_collab/CCU002_07/infection/riskratio_wen/cox_infec_clean_ecds240118_rmAge18AfterMar22.rds")

# Create output directory
output_dir <- "./output/"
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}


input_inf <- as.data.frame(input_inf)

d_sample <- input_inf %>% select(cov_cat_sex,
                            cov_cat_4age,
                            cov_cat_ethnicity,
                            cov_cat_deprivation,
                            cov_cat_region,
                            cov_cat_consultations,
                            cov_cat_medications,
                            sub_bin_covid19_confirmed_history
                            
)

xvars <- c("cov_cat_sex",
           "cov_cat_4age",
           "cov_cat_ethnicity",
           "cov_cat_deprivation",
           "cov_cat_region",
           "cov_cat_consultations",
           "cov_cat_medications",
           "first_covid19_diagnosis"
)


d_sample$sub_bin_covid19_confirmed_history <- as.factor(d_sample$sub_bin_covid19_confirmed_history)
# set reference categories =====================================================
cat("set reference categories\n")

d_sample <-
  d_sample %>%
  mutate(
    sex                   = fct_relevel(cov_cat_sex,              "Female"),
    age                   = fct_relevel(cov_cat_4age,              "5-11"),
    ethnicity             = fct_relevel(cov_cat_ethnicity,        "White"),
    region                = fct_relevel(cov_cat_region,           "London"),
    deprivation           = fct_relevel(cov_cat_deprivation,      "1-2 (most deprived)"),
    GP_consultations      = fct_relevel(cov_cat_consultations,      "0"),
    medications           = fct_relevel(cov_cat_medications, "0"),
    first_covid19_diagnosis = fct_relevel(sub_bin_covid19_confirmed_history, "FALSE"),
  )

# describe =====================================================================
cat("describe\n")

describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      covid_1st_diagnosis_n = sum(first_covid19_diagnosis == "TRUE")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      covid_1st_diagnosis_p = covid_1st_diagnosis_n / sum(covid_1st_diagnosis_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      covid_1st_diagnosis_n,
      covid_1st_diagnosis_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
d_sample <- d_sample %>% mutate(total = factor("total"))
d_sample$cov_cat_ethnicity <- factor( d_sample$cov_cat_ethnicity , ordered = FALSE )
d_sample$cov_cat_deprivation <- factor( d_sample$cov_cat_deprivation , ordered = FALSE )

covid_1st_diagnosis_overall_np <-
  d_sample %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_1st_diagnosis_00_04_np <-
  d_sample %>%
  filter(cov_cat_4age == "0-4") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_1st_diagnosis_05_11_np <-
  d_sample %>%
  filter(cov_cat_4age == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_1st_diagnosis_12_15_np <-
  d_sample %>%
  filter(cov_cat_4age == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_1st_diagnosis_16_17_np <-
  d_sample %>%
  filter(cov_cat_4age == "16-17") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

cat("print\n")

# covid_1st_diagnosis_overall_np %>%
#   kable(
#     digits = 1,
#     format.args = list(big.mark = ",")
#   ) %>%
#   kable_styling(
#     bootstrap_options = "striped",
#     full_width = FALSE
#   ) %>%
#   print()


t_covid_1st_diagnosis_overall_np <-
  covid_1st_diagnosis_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_1st_diagnosis_n = round(covid_1st_diagnosis_n/5.0)*5,
    covid_1st_diagnosis_p = if_else(is.na(covid_1st_diagnosis_n), NA_real_, round(covid_1st_diagnosis_p, 1))
  )%>%
  write_csv(
    file = "output/t_inf_overall_np_4age.csv"
  )

# t_covid_1st_diagnosis_overall_np %>%
#   kable(
#     format.args = list(big.mark = ",")
#   ) %>%
#   kable_pretty()
# 
# write_csv(
#   t_covid_1st_diagnosis_overall_np,
#   file = "output/t_covid_1st_diagnosis_overall_np.csv"
# )

# 0-4
covid_1st_diagnosis_00_04_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_1st_diagnosis_n = round(covid_1st_diagnosis_n/5.0)*5,
    covid_1st_diagnosis_p = if_else(is.na(covid_1st_diagnosis_n), NA_real_, round(covid_1st_diagnosis_p, 1))
  ) %>%
  write_csv(
    file = "output/t_inf_overall_00_04_np_4age.csv"
  )

# 5-11
covid_1st_diagnosis_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_1st_diagnosis_n = round(covid_1st_diagnosis_n/5.0)*5,
    covid_1st_diagnosis_p = if_else(is.na(covid_1st_diagnosis_n), NA_real_, round(covid_1st_diagnosis_p, 1))
  ) %>%
  write_csv(
    file = "output/t_inf_overall_05_11_np_4age.csv"
  )

# 12-15
covid_1st_diagnosis_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_1st_diagnosis_n = round(covid_1st_diagnosis_n/5.0)*5,
    covid_1st_diagnosis_p = if_else(is.na(covid_1st_diagnosis_n), NA_real_, round(covid_1st_diagnosis_p, 1))
  ) %>%
  write_csv(
    file = "output/t_inf_overall_12_15_np_4age.csv"
  )

# 16-17
covid_1st_diagnosis_16_17_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_1st_diagnosis_n = round(covid_1st_diagnosis_n/5.0)*5,
    covid_1st_diagnosis_p = if_else(is.na(covid_1st_diagnosis_n), NA_real_, round(covid_1st_diagnosis_p, 1))
  ) %>%
  write_csv(
    file = "output/t_inf_overall_16_17_np_4age.csv"
  )



df_cohort <- input_vax %>% select(patient_id,
                                  cov_cat_sex,
                                  cov_cat_4age,
                                  cov_cat_ethnicity,
                                  cov_cat_deprivation,
                                  cov_cat_region,
                                  cov_cat_consultations,
                                  cov_cat_medications,
                                  vaccination,
                                  vax_date_covid_1,
                                  vax_date_covid_2,
                                  death_date
                                  
)
#df_cohort$vax_date1_covid <- as.Date(df_cohort$vax_date1_covid)
#df_cohort$vax_date2_covid <- as.Date(df_cohort$vax_date2_covid)
#df_cohort$death_date <- as.Date(df_cohort$death_date)

#### Vaccine uptake visualisations

study_start <- as.Date("2021-08-06")
#study_end <- as.Date("2022-12-31")
study_end <- as.Date("2022-03-31")

## Vaccinations by age group and week

d_study_weeks <- seq(
  from = floor_date(study_start, "week"),
  to   = floor_date(study_end, "week"),
  by   = "1 week"
)

d_dummy_weeks <- expand_grid(
  cov_cat_4age = sort(unique(df_cohort$cov_cat_4age)),
  dose_num = c("1", "2"),
  dose_week = d_study_weeks
)

d_vacc_week <-
  df_cohort %>%
  select(
    cov_cat_4age,
    `1` = vax_date_covid_1,
    `2` = vax_date_covid_2
  ) %>%
  pivot_longer(
    cols           = c("1", "2"),
    names_to       = "dose_num",
    values_to      = "dose_date",
    values_drop_na = TRUE
  ) %>%
  mutate(dose_week = floor_date(dose_date, "week")) %>%
  filter(dose_week <= study_end) %>%
  count(cov_cat_4age, dose_num, dose_week) %>%
  full_join(d_dummy_weeks, by = c("cov_cat_4age", "dose_num", "dose_week")) %>%
  mutate(n = replace_na(n, as.integer(0))) %>%
  arrange(cov_cat_4age, dose_num, dose_week)


d_vacc_week %>%
  # Suppression
  # Alter this depending on what the rules for TRE are
  mutate(n = if_else(1 <= n & n < 5, as.integer(5), n)) %>%
  ggplot(aes(x = dose_week, y = n, colour = dose_num)) +
  facet_wrap(~cov_cat_4age, ncol = 1, scales = "free_y") +
  geom_line() +
  xlab("") +
  ylab("") +
  labs(color = "Dose") +
  scale_y_continuous(labels = scales::comma) +
  xlab("Date") +
  ylab("Count")

ggsave(paste0(output_dir, "vacc_count_by_week_dose_4age_group_dose1dose2.png"), height = 10)


## Time between vaccinations
d_vacc_diff <-
  df_cohort %>%
  select(
    patient_id,
    cov_cat_4age,
    dose1    = vax_date_covid_1,
    dose2    = vax_date_covid_2
  ) %>%
  # remove events after study window
  mutate(across(
    .cols = where(is.Date),
    .fns  = ~ if_else(.x <= study_end, .x, NA_Date_)
  )) %>%
  mutate(
    # weeks between doses
    `Dose 1 to 2` = interval(dose1, dose2) / dweeks(),
    # round off
    `Dose 1 to 2` = floor(`Dose 1 to 2`)
  ) %>%
  select(
    cov_cat_4age,
    `Dose 1 to 2`
  ) %>%
  pivot_longer(
    cols           = -cov_cat_4age,
    names_to       = "dose_to_dose",
    values_to      = "diff_weeks",
    values_drop_na = TRUE
  ) %>%
  count(cov_cat_4age, dose_to_dose, diff_weeks)

d_vacc_diff %>%
  # Suppression
  # Alter this depending on what the rules for  TRE are
  mutate(n = if_else(1 <= n & n < 5, as.integer(5), n)) %>%
  ggplot(aes(
    x = diff_weeks,
    y = n,
  )) +
  facet_grid(cov_cat_4age ~ dose_to_dose, scales = "free") +
  geom_col() +
  scale_y_continuous(labels = scales::comma) +
  xlab("Weeks") +
  ylab("Count")

ggsave(paste0(output_dir, "time_between_4age_doses1and2.png"), height = 10, width = 10)


### Uptake cumulative incidence

# first code for uptake_cumulative_incidence
date_vacc_start <- as.Date("2021-08-06")

df <- df_cohort %>%
  select(patient_id, cov_cat_4age, vax_date_covid_1, vax_date_covid_2, death_date) %>%
  mutate(surv_date = pmin(study_end, death_date, na.rm = TRUE)) %>%
  mutate(surv_time = as.numeric(surv_date - (date_vacc_start))) %>%
  mutate(event = 0) %>%
  # Some people died before study start and will therefore have negative survival time. Remove them.
  filter(surv_time >= 0) %>%
  # If they had an event on day of vaccination, assume that the event happened shortly after vaccination
  mutate(surv_time = ifelse(surv_time == 0, 0.001, surv_time))

df <- tmerge(df, df, id = patient_id, event = event(surv_time, event))

# dataframe of start times for different vaccination status
df_vs <- df %>%
  mutate(
    Unvaccinated = 0,
    `Dose 1` = as.numeric(vax_date_covid_1 - date_vacc_start),
    `Dose 2` = as.numeric(vax_date_covid_2 - date_vacc_start),
    Death = as.numeric(death_date - date_vacc_start)
  ) %>%
  select(-vax_date_covid_1, -vax_date_covid_2)

df_vs <- pivot_longer(df_vs,
                      cols = c("Unvaccinated", "Dose 1", "Dose 2", "Death"),
                      names_to = "vs", values_to = "time"
) %>%
  filter(!is.na(time))

# Add in vaccination status as a time dependent variable
df <- tmerge(df, df_vs, id = patient_id, dose_event = event(time, vs)) %>%
  mutate(dose_event = factor(dose_event))

# Cumulative incidence curve
plot <- survfit(
  Surv(tstart, tstop, dose_event) ~ cov_cat_4age,
  data = df,
  id = patient_id
) %>%
  tidy() %>%
  mutate(
    strata = strata %>%
      str_replace("cov_cat_4age=", "") %>%
      factor() %>%
      fct_relevel("5-11", "12-15", "16-17"),
    State = state %>%
      factor() %>%
      fct_recode("Unvaccinated" = "(s0)") %>%
      fct_relevel("Death")
  ) %>%
  # Convert time to date
  mutate(
    time = date_vacc_start + time
  ) %>%
  ggplot(aes(
    x = time,
    y = estimate,
    fill = State
  )) +
  facet_wrap(~strata, ncol = 1) +
  geom_area() +
  scale_fill_brewer(type = "qual") +
  ylab("Cumulative incidence") +
  xlab("Day")

ggsave(paste0(output_dir, "uptake_cumulative_incidence.png"), plot, height = 10)


# second code for uptake_cumulative_incidence

lkp_events = c(
  "Unvaccinated",
  "Dose 1",
  "Dose 2",
  "Death",
  "study_end"
)

# make multi-state date set for dose

d_mstate_vacc =
  df_cohort %>%
  mutate(
    Death = death_date,
    study_end = study_end
  ) %>%
  select(
    patient_id,
    cov_cat_4age,
    `Dose 1`    = vax_date_covid_1,
    `Dose 2`    = vax_date_covid_2,
    Death,
    study_end
  ) %>%
  # any first day events just add half a day
  mutate(across(
    .cols = where(is.Date),
    .fns  = ~ if_else(.x == study_start, .x + ddays(0.5), as_datetime(.x))
  )) %>%
  # remove events after someone has either moved out, died, or reached the
  # end of the study window
  mutate(
    end_follow_up = pmin(Death, study_end, na.rm = TRUE)
  ) %>%
  mutate(across(
    .cols = where(is.POSIXct),
    .fns  = ~ if_else(.x <= end_follow_up, .x, NA_POSIXct_)
  )) %>%
  select(-end_follow_up) %>%
  pivot_longer(
    cols           = c(-patient_id, -cov_cat_4age),
    names_to       = "event_name",
    values_to      = "event_date",
    values_drop_na = TRUE
  ) %>%
  # add row number for alf
  lazy_dt() %>%
  arrange(patient_id, event_date) %>%
  group_by(patient_id) %>%
  mutate(alf_seq = row_number()) %>%
  as_tibble() %>%
  # define survival columns
  mutate(
    tstart     = if_else(alf_seq == 1, as_datetime(study_start), lag(event_date)),
    tstart     = interval(as_datetime(study_start), tstart) / ddays(),
    state_from = if_else(alf_seq == 1, "Unvaccinated", lag(event_name)),
    state_from = factor(state_from, lkp_events) %>% fct_drop(),
    tstop      = event_date,
    tstop      = interval(as_datetime(study_start), tstop) / ddays(),
    state_to   = factor(event_name, lkp_events) %>% fct_drop()
  ) %>%
  select(
    -event_name,
    -event_date
  ) %>%
  # anyone who has event on last day, remove row for that transition from event to study end
  filter(!(tstart > 0 & tstart == tstop & state_to == "study_end")) %>%
  # any same day events that go from dose to death / move out just add half a day again
  mutate(
    tstop = if_else(
      condition = state_from %in% c("Dose 1", "Dose 2") &
        state_to  == "Death" &
        tstart == tstop,
      true = tstop + 0.5,
      false = tstop
    )
  ) %>%
  # finalise censored category
  mutate(
    state_to = state_to %>%
      fct_collapse("(censored)" = "study_end") %>%
      fct_relevel("(censored)")
  )

# explore
d_mstate_vacc %>% tabyl(state_from, state_to)

# fit mstate
mstate_vacc = survfit(
  formula = Surv(tstart, tstop, state_to) ~ cov_cat_4age,
  data = d_mstate_vacc,
  id = patient_id
)


mstate_vacc %>%
  tidy() %>%
  mutate(
    strata = strata %>%
      str_replace("cov_cat_4age=", "") %>%
      factor() %>%
      fct_relevel("5-11", "12-15", "16-17"),
    State = state %>%
      factor() %>%
      fct_recode("Unvaccinated" = "(s0)") %>%
      fct_relevel("Death")
  ) %>%
  # Convert time to date
  mutate(
    time = study_start + time
  ) %>%
  ggplot(aes(
    x = time,
    y = estimate,
    fill = State
  )) +
  facet_wrap(~strata, ncol = 1) +
  geom_area() +
  scale_fill_brewer( type = "qual") +
  ylab('Cumulative incidence') +
  xlab('Day')

ggsave(paste0(output_dir, 'uptake_cumulative_incidence.png'), height = 10)
