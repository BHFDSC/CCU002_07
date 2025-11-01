## =============================================================================
## Pipeline (2): Reads in analysis-specific data, loads/sets any parameters,
## renames any variables used throughout the following scripts
## Authors: Alexia Sampri, Wen Shi
## =============================================================================

# ---------------------- READ IN DATA ------------------------------------------
# read in core analysis information
read_in_cols <- c(
                  "birth_date",
                  "death_date",
                   "age_18_date",
                  "cov_cat_sex", 
                   "cov_n_age",
                  
                  "cov_cat_region",
                  "baseline_date",
                  "cov_cat_ethnicity",
                  
                  event_name)


if(exposure == "covid"){
  read_in_cols <- append(read_in_cols,c("exp_date_covid19_confirmed","vax_date_covid_1","sub_cat_covid19_hospital",covar_names))
  if(active_analyses$prior_hist_expo_var != ""){
    read_in_cols <- append(read_in_cols,active_analyses$prior_hist_expo_var)
  }
}else if(exposure == "vaccine"){
  read_in_cols <- append(read_in_cols,c("exp_date_covid19_vaccine","vax_date_covid_2",covar_names))
  if(active_analyses$prior_hist_expo_var != ""){
    read_in_cols <- append(read_in_cols,active_analyses$prior_hist_expo_var)
  }
}

input <-readRDS(paste0(data_dir,clean_dataset))  %>% select(all_of(read_in_cols))


#---------------------------SPECIFY MAIN PARAMETERS-----------------------------
# specify study parameters
#For all analysis aside from age stratifed, analysis is performed across all ages 
agebreaks_all <- c(0, 18)
#When it splits the age groups it does it as [,) so upper age is not inclusive 
agelabels_all <- c("all")

#Age breaks and labels for age sub group analysis
if(exposure == "covid"){
  agebreaks_strata <- c(0,5,12,18)
  agelabels_strata <- c("0_4", "5_11", "12_17")
} else if(exposure == "vaccine"){
  agebreaks_strata <- c(5,12,18)
  agelabels_strata <- c("5_11", "12_17")
}

#These are the study start and end dates for the Delta era
if(exposure == "covid"){
  cohort_start_date <- as.Date("2020-01-01")
  cohort_end_date <- as.Date("2021-05-31")
} else if(exposure == "vaccine"){
  cohort_start_date <- as.Date("2021-08-06")
  cohort_end_date <- as.Date("2022-12-31")
}

total_follow_up <- as.numeric(cohort_end_date - cohort_start_date)+1

#Used to split time since COVID exposure; when there are time periods with no events then
#a reduced number of time periods is used (need to use +1 above as time periods are split using [ , ) and we need 
#the last day

if(exposure == "covid"){
  # cuts_days_since_expo <- c(1,7,28,182,365,total_follow_up) 
  cuts_days_since_expo <- c(1,28,182,total_follow_up) 
  
}else if(exposure == "vaccine"){
  cuts_days_since_expo <- c(7,28,182,total_follow_up) 
  cuts_days_since_expo_reduced <- c(28,total_follow_up) 
}

#Rename input variable names (by renaming here it means that these scripts can be used for other datasets without
## having to keep updating all the variable names throughout the following scripts)
setnames(input, 
         old = c("birth_date",
                 "death_date",
                  "age_18_date",
                 "cov_cat_sex", 
                 "cov_n_age",
                
                 "cov_cat_region",
                 "cov_cat_ethnicity",
                 event_name),
         
         new = c("birth_date",
                 "death_date",
                 "birthday_18",
                 "sex",
                 "age", 
                 "region_name",
                 "ethnicity",
                 "event_date"))


if(exposure == "covid"){
  input <- input %>% rename(expo_date = exp_date_covid19_confirmed, vax_date_1 = vax_date_covid_1, covid_expo_pheno = sub_cat_covid19_hospital)
}else if(exposure == "vaccine"){
  input <- input %>% rename(expo_date = exp_date_covid19_vaccine, vax_date_2 = vax_date_covid_2)
}

#Set the main cohort columns required to create the survival data 
#covariates are added later as these are loaded dependent on which model is being run

cohort_cols <- c("patient_id", 
                 "sex",
                 "ethnicity",
                 "age", 
                 "expo_date",
                 "region_name",
                 "follow_up_start",
                 "event_date",
                 "follow_up_end")

if(exposure == "covid" & "covid_pheno" %in% analyses_to_run$subgroup_cat){
  cohort_cols <- append(cohort_cols, c("hospitalised_follow_up_end","non_hospitalised_follow_up_end","hospitalised_covid_expo_censor","non_hospitalised_covid_expo_censor"))
}

#-----------------------CREATE EMPTY ANALYSES NOT RUN DF------------------------
analyses_not_run=data.frame(matrix(nrow=0,ncol = 7))
colnames(analyses_not_run)=c("event","subgroup","exposure", "any exposures?", "any exposure events?", "any non exposed?", "more than 50 post exposure events?")
