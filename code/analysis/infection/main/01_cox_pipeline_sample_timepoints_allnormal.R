## =============================================================================
## Pipeline (1): Control center, calls relevant analysis scripts, sets working 
## and saving directories, parallelises processes
## Author Alexia Sampri, Wen Shi
## Based on scripts written by Samantha Ip, see the following repo's for 
## original scripts: https://github.com/BHFDSC/CCU002_01 & https://github.com/BHFDSC/CCU002_03
## =============================================================================


library(data.table)

library(dplyr)
library(survival)
library(broom)
library(DBI)
library(ggplot2)
library(nlme)
library(tidyverse)
library(lubridate)
library(purrr)
library(parallel)
library(stats)
library(utils)
library(stringr)
library(readr)



e = c(
  'out_ate_date','out_vte_date','out_carditis_date','out_thrombocytopenia_date',
  'out_inflammatory_date'
  # ,'out_fracture_date'
  )


for (i in e) {
event_name <- i
exposure <- "covid"
controls_per_case <- 20
save_date <- "31_01_2025"
clean_dataset = 'cox_infec_clean_ecds.rds'

# Specify directories ----------------------------------------------------------
# change work dir to the folder you want to save the results
setwd("~/collab/CCU002_07/infection")
data_dir = "~/collab/CCU002_07/infection/riskratio_wen/"

raw_results_dir <- paste0("cox_wen/",save_date,"/raw_results")
compiled_results_dir <- paste0("cox_wen/",save_date,"/compiled_results")
scripts_dir <- "~/collab/CCU002_07/infection/coxscripts/"


dir.create(file.path(raw_results_dir), recursive =TRUE, showWarnings = FALSE)
dir.create(file.path(compiled_results_dir), recursive =TRUE, showWarnings = FALSE)

# Source relevant files --------------------------------------------------------

source(file.path(scripts_dir,"02_01_cox_analyses_to_run.R"))
source(file.path(scripts_dir,"02_02_cox_load_data_5timepoints_fo1.R")) 
source(file.path(scripts_dir,"02_03_follow_up_dates.R"))
source(file.path(scripts_dir,"06_cox_extra_functions.R"))

# Add time point parameter to analyses to run  ----------------------------

source(file.path(scripts_dir,"02_04_cox_timepoint_param_allnormal.R"))

# add reduced time point column

analyses_to_run$reduced_timepoint <- lapply(split(analyses_to_run,seq(nrow(analyses_to_run))),
                                            function(analyses_to_run) 
                                              get_timepoint(
                                                event=analyses_to_run$event,
                                                subgroup=analyses_to_run$subgroup,
                                                stratify_by_subgroup=analyses_to_run$stratify_by_subgroup,
                                                stratify_by=analyses_to_run$strata,
                                                input)
)


analyses_to_run$reduced_timepoint <-  as.character(analyses_to_run$reduced_timepoint)
analyses_to_run <- analyses_to_run %>% filter(reduced_timepoint != "remove")


# Source remainder of relevant files --------------------------------------------------------

source(file.path(scripts_dir,paste0("03_01_cox_subgrouping_sample.R"))) # Model specification

# ------------------------------------ LAUNCH JOBS -----------------------------



if(nrow(analyses_to_run>0)){
  lapply(split(analyses_to_run,seq(nrow(analyses_to_run))),
         function(analyses_to_run)
           cox_model_start(           
             event=analyses_to_run$event,           
             subgroup=analyses_to_run$subgroup,           
             stratify_by_subgroup=analyses_to_run$stratify_by_subgroup,           
             stratify_by=analyses_to_run$strata,           
             time_point=analyses_to_run$reduced_timepoint,       
             input,covar_names,cuts_days_since_expo,cuts_days_since_expo_reduced,mdl))
}


#Save csv of analyses not run
write.csv(analyses_not_run, paste0(compiled_results_dir,"/analyses_not_run_" , event_name ,"_",exposure,"_analysis.csv"),row.names = T)


#Combine all results into one .csv
source(file.path(scripts_dir, "05_cox_format_tbls_HRs.R"))
}


