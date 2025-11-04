# Create output directory ------------------------------------------------------
# no package calld here
# fs::dir_create(here::here("data"))

# Exposure: COVID-19
# Create empty data frame ------------------------------------------------------

library(data.table)
df <- data.frame(outcome = character(),
                 outcome_variable = character(),
                 covariates = character(),
                 model = character(),
                 main = character(),
                 covid_pheno_hospitalised = character(),
                 covid_pheno_non_hospitalised = character(),
                 agegp_0_4 = character(),
                 agegp_5_12 = character(),
                 agegp_13_18 = character(),
                 sex_Male = character(),
                 sex_Female = character(),
                 ethnicity_White = character(),
                 ethnicity_Mixed = character(),
                 ethnicity_South_Asian = character(),
                 ethnicity_Black = character(),
                 ethnicity_Other = character(),
                 prior_history_expo_TRUE = character(),
                 prior_history_expo_FALSE = character(),
                 adjust_for_expo = character(),
                 prior_hist_expo_var = character(),
                 stringsAsFactors = FALSE)

# Add cardiovascular outcomes (any position) -----------------------------------

outcomes <- c("arterial thrombosis event",
              "venous thrombosis event",
              "thrombocytopenia",
              "myocarditis/pericarditis",
              "inflammatory diseases",
              "fracture")

outcomes_short <- c("out_ate_date","out_vte_date","out_thrombocytopenia_date","out_carditis_date","out_inflammatory_date","out_fracture_date")

for (i in 1:length(outcomes)) {
  df[nrow(df)+1,] <- c(outcomes[i],
                       outcomes_short[i],
                       "cov_cat_sex;cov_n_age;cov_cat_ethnicity;cov_cat_deprivation;cov_cat_region;cov_n_consultations;cov_n_medications",
                       "two",
                       rep(TRUE,1),
                       rep(FALSE,15),
                       "sub_bin_covid19_confirmed_history")
}


# Save active analyses list ----------------------------------------------------
setwd("~/collab/CCU002_07/infection/coxscripts")
saveRDS(df, file = "active_analyses_covid_expo_sub.rds")

# Exposure: Vaccination
# Create empty data frame ------------------------------------------------------

df <- data.frame(outcome = character(),
                 outcome_variable = character(),
                 covariates = character(),
                 model = character(),
                 main = character(),
                 agegp_5_11 = character(),
                 agegp_12_17 = character(),
                 sex_Male = character(),
                 sex_Female = character(),
                 ethnicity_White = character(),
                 ethnicity_Mixed = character(),
                 ethnicity_South_Asian = character(),
                 ethnicity_Black = character(),
                 ethnicity_Other = character(),
                 prior_history_expo_TRUE = character(),
                 prior_history_expo_FALSE = character(),
                 adjust_for_expo = character(),
                 prior_hist_expo_var = character(),
                 stringsAsFactors = FALSE)

# Add cardiovascular outcomes (any position) -----------------------------------

outcomes <- c("arterial thrombosis event",
              "venous thrombosis event",
              "thrombocytopenia",
              "myocarditis/pericarditis",
              "inflammatory diseases",
              "fracture")

outcomes_short <- c("out_ate_date","out_vte_date","out_thrombocytopenia_date","out_carditis_date","out_inflammatory_date","out_fracture_date")

for (i in 1:length(outcomes)) {
  df[nrow(df)+1,] <- c(outcomes[i],
                       outcomes_short[i],
                       "cov_cat_sex;cov_n_age;cov_cat_ethnicity;cov_cat_deprivation;cov_cat_region;cov_n_consultations;cov_n_medications",
                       "all",
                       rep(TRUE,13),
                       rep(FALSE,0),
                       "sub_bin_covid_confirmed_history")
}

# Save active analyses list ----------------------------------------------------

saveRDS(df, file = "active_analyses_vaccine_expo.rds")
