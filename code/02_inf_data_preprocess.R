# This script checks/cleans/prepares the covariates prior to infection analysis
# Author: Alexia Sampri


rm(list = setdiff(ls(), "con"))

# required packages

library(tidyverse)
library(lubridate)
library(data.table)
library(dplyr)


# detectCores()

################################################################################
###############                 load  data                    ##################
################################################################################
# set directory
setwd("/db-mnt/databricks/rstudio_collab/CCU002_07/infection")
#setwd("D:/PhotonUser/My Files/Home Folder")

# # input dataset
#dat_inf <- data.table::fread("./data/ccu002_07_out_inf_analysis20231004.csv.gz", data.table = FALSE)
#dat_inf <- data.table::fread("./data/ccu002_07_out_inf_analysis20231121.csv.gz", data.table = FALSE)
#dat_inf <- data.table::fread("./data/ccu002_07_out_inf_ecds_analysis20231121.csv.gz", data.table = FALSE)
dat_inf <- data.table::fread("./data/ccu002_07_out_inf_ecds_analysis20240117.csv.gz", data.table = FALSE)


# # Select a subset of the data to test the code (for CHUNK 1 for example)
# input_test <- subset(df, CHUNK == 1)

input <- dat_inf %>%
  rename(
    patient_id = PERSON_ID,
    birth_date = DOB,
    death_date = DOD,
    exp_date_covid19_confirmed = exp_covid_1st_date,
    sub_date_covid19_hospital = exp_covid_adm_date,
    vax_date_covid_1 = vacc_1st_date,
    cov_num_deprivation = IMD_2019_DECILES, 
    cov_cat_sex = SEX,
    cov_cat_ethnicity = ETHNIC_CAT,
    cov_cat_region = region,
    cov_n_medications = cov_n_unique_bnf_chapters,
    cov_num_age = baseline_age,
    censor_date_start_out = CENSOR_DATE_START_out,
    censor_date_end_out = CENSOR_DATE_END_out,
    censor_date_start_cov =CENSOR_DATE_START_cov,
    censor_date_end_cov = CENSOR_DATE_END_cov
  )

input$birthday_18 <- input$birth_date %m+% years(18)

# for infection 
input$cohort_start_date <- as.Date("2020-01-01")
input$sub_bin_covid19_confirmed_history <- ifelse(!is.na(input$exp_date_covid19_confirmed), TRUE, FALSE)
input$sub_bin_vaccine_confirmed_history <- ifelse(!is.na(input$vax_date_covid_1), TRUE, FALSE)
# 


## cov_cat_age

input <- input %>% 
  mutate(
    cov_cat_3age = cut(
      cov_num_age,
      breaks = c(0, 5, 12, Inf),
      labels = c("0-4","5-11", "12-17"),
      right = FALSE
    ),
    cov_cat_4age = cut(
      cov_num_age,
      breaks = c(0, 5, 12, 16, Inf),
      labels = c("0-4","5-11", "12-15", "16-17"),
      right = FALSE
    )
  )

## cov_cat_ethnicity

input$cov_cat_ethnicity <- as.factor(input$cov_cat_ethnicity)
levels(input$cov_cat_ethnicity) <- list( "Missing" = "", "Missing" = "Unknown" , "White" = "White", "Mixed" = "Mixed", "South Asian" = "Asian or Asian British", "Black" = "Black or Black British", "Other" = "Other")
input$cov_cat_ethnicity <- ordered(input$cov_cat_ethnicity, levels = c("White","Mixed","South Asian","Black","Other","Missing"))
input$cov_cat_ethnicity <- as.factor(input$cov_cat_ethnicity)

## cov_cat_deprivation
input$cov_cat_deprivation <- as.factor(input$cov_num_deprivation)
levels(input$cov_cat_deprivation)[levels(input$cov_cat_deprivation)==1 | levels(input$cov_cat_deprivation)==2] <-"1-2 (most deprived)"
levels(input$cov_cat_deprivation)[levels(input$cov_cat_deprivation)==3 | levels(input$cov_cat_deprivation)==4] <-"3-4"
levels(input$cov_cat_deprivation)[levels(input$cov_cat_deprivation)==5 | levels(input$cov_cat_deprivation)==6] <-"5-6"
levels(input$cov_cat_deprivation)[levels(input$cov_cat_deprivation)==7 | levels(input$cov_cat_deprivation)==8] <-"7-8"
levels(input$cov_cat_deprivation)[levels(input$cov_cat_deprivation)==9 | levels(input$cov_cat_deprivation)==10] <-"9-10 (least deprived)"
input$cov_cat_deprivation <- ordered(input$cov_cat_deprivation, levels = c("1-2 (most deprived)","3-4","5-6","7-8","9-10 (least deprived)"))

## cov_cat_region
input$cov_cat_region <- as.factor(input$cov_cat_region)
input$cov_cat_region = relevel(factor(input$cov_cat_region), ref = "London")

## cov_cat_sex
input$cov_cat_sex <- as.factor(input$cov_cat_sex)
levels(input$cov_cat_sex) <- list("Female" = "2", "Male" = "1")
input$cov_cat_sex <- relevel(input$cov_cat_sex, ref = "Female")

## cov_cat_consultations

input["cov_n_consultations"][is.na(input["cov_n_consultations"])] <- 0

input <- input %>% mutate(
  cov_cat_consultations = cut(as.numeric(cov_n_consultations),
                              breaks = c(0, 1, 7, Inf),
                              labels = c("0", "1-6", "7+"),
                              right = FALSE
  ))


## cov_cat_medications

input["cov_n_medications"][is.na(input["cov_n_medications"])] <- 0

input <- input %>% mutate(
  cov_cat_medications = cut(as.numeric(cov_n_medications),
                            breaks = c(0, 1, 3, Inf),
                            labels = c("0", "1-2", "3+"),
                            right = FALSE
  ))

# Define COVID-19 severity ---------------------------------------------------

input$sub_cat_covid19_hospital <- "no_infection"

input$sub_cat_covid19_hospital <- ifelse(!is.na(input$exp_date_covid19_confirmed),
                                         "non_hospitalised",input$sub_cat_covid19_hospital)

input$sub_cat_covid19_hospital <- ifelse(!is.na(input$exp_date_covid19_confirmed) & 
                                           !is.na(input$sub_date_covid19_hospital) &
                                           (input$sub_date_covid19_hospital-input$exp_date_covid19_confirmed>=0 &
                                              input$sub_date_covid19_hospital-input$exp_date_covid19_confirmed<29),
                                         "hospitalised",input$sub_cat_covid19_hospital)

input$sub_cat_covid19_hospital <- as.factor(input$sub_cat_covid19_hospital)
input[,c("sub_date_covid19_hospital")] <- NULL

#Re-level all binary covariates with TRUE as reference level
bin_factors <- covariates <- colnames(input)[grepl("cov_hx.+flag$",colnames(input))]
input <- input %>% mutate(across(bin_factors, ~replace_na(.,0)))
input[,bin_factors] <- lapply(input[,bin_factors], function(x) factor(x, levels = c("FALSE","TRUE")))

relev <- function(f) relevel(factor(f), ref = "FALSE")
input <- mutate_each(input, funs(relev), starts_with("_flag"))

# These covariates should be numeric
numeric_covars <- colnames(input)[grepl("cov_n_",colnames(input))]
input <- input %>% mutate(across(numeric_covars, as.numeric))

# Ensure all date variables are formatted as dates
date_covars <- colnames(input)[grepl("date",colnames(input))]
date_covars <- date_covars[! date_covars %in% '_fu_end_date_source']
print(str(input[,date_covars]))
input <- input %>% mutate(across(all_of(date_covars), as.Date))

input <- input %>%
  filter(is.na(exp_date_covid19_confirmed) | exp_date_covid19_confirmed >= birth_date)

input <- input %>%
  filter(is.na(exp_date_covid19_confirmed) | is.na(death_date) | exp_date_covid19_confirmed <= death_date)


print(paste0("Age levels: ", unique(input$cov_cat_4age)))
print(paste0("Ethnicity levels: ", unique(input$cov_cat_ethnicity)))
print(paste0("Deprivation levels: ", unique(input$cov_cat_deprivation)))
print(paste0("Region levels: ", unique(input$cov_cat_region)))
print(paste0("GP consultations levels: ", unique(input$cov_cat_consultations)))
print(paste0("Medication levels: ", unique(input$cov_cat_medications))) 

##------------------------------ any NAs left? ---------------------------------
print(any(is.na(input)))
print(colnames(input)[colSums(is.na(input)) > 0])
(colMeans(is.na(input)))*100




# save .rds  
#saveRDS(input, file = file.path("data", paste0("cox_inf.rds")))
saveRDS(input, file = file.path("data", paste0("cox_inf_ecds.rds")))

