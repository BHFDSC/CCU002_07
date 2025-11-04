## =============================================================================
## 1.Creates the base survival data that includes the event date for the outcome of interest
## 2.Stratify to relevant subgroup if necessary
## 3.Add follow up start and end dates
## Authors: Wen Shi, Alexia Sampri
## =============================================================================
source(file.path(scripts_dir,"04_01_(a)_cox_fit_model_sample.R"))
cox_model_start <- function(event,subgroup,stratify_by_subgroup,stratify_by,time_point,survival_data,covar_names,cuts_days_since_expo,cuts_days_since_expo_reduced,mdl){
  print(paste0("Working on subgroup: ", subgroup, " ", exposure))
  print(paste0("Using ",time_point," time point"))
  
  # Select the relevant cohort columns required to stratify by subgroup if necessary
  if(startsWith(subgroup,"prior_history")){
    survival_data <- input %>% dplyr::select(all_of(cohort_cols),all_of(stratify_by_subgroup))
  }else{
    survival_data <- input %>% dplyr::select(all_of(cohort_cols))
  }
  
  for(i in c("hospitalised","non_hospitalised")){
    if(stratify_by == i){
      survival_data$follow_up_end <- NULL
      setnames(survival_data, 
               old = c(c(paste0(i,"_follow_up_end")),
                       c(paste0(i,"_covid_expo_censor"))),
               
               new = c("follow_up_end",
                       "date_expo_censor"))
    }
  }
  
  # Stratify to the relevant subgroup if either sex/ethnicity/prior history subgroup
  # COVID pheno subgroup is filtered later in this script
  
  for(i in c("ethnicity","sex","prior_history")){
    if(startsWith(subgroup,i)){
      survival_data=survival_data%>%filter_at(stratify_by_subgroup,all_vars(.==stratify_by))
    }
  }
  
  # Filter for age group of interest -------------------------------------------
  
  # If a age group subgroup analysis then use the age subgroup otherwise analyse for all ages
  if(startsWith(subgroup,"agegp")){
    agebreaks=agebreaks_strata
    agelabels=agelabels_strata
  }else{
    agebreaks=agebreaks_all
    agelabels=agelabels_all
  }
  
  # no agegroup
  survival_data = copy(survival_data)[,agegroup := cut(age, 
                                          breaks = agebreaks, 
                                          right = FALSE, 
                                          labels = agelabels)]
  if(startsWith(subgroup,"agegp_")){
    survival_data=survival_data %>% filter(agegroup== stratify_by)
  }
  
  if(startsWith(subgroup,"covid_pheno_")){
    survival_data <- survival_data %>% mutate(expo_date = replace(expo_date, which(!is.na(date_expo_censor) & (expo_date >= date_expo_censor)),NA),
                                              event_date = replace(event_date, which(!is.na(date_expo_censor) & (event_date >= date_expo_censor)), NA)) %>%
      filter((follow_up_start != date_expo_censor)|is.na(date_expo_censor))
    
    setDT(survival_data)[follow_up_end == date_expo_censor, follow_up_end := follow_up_end-1]
    
    survival_data <- survival_data[survival_data$follow_up_end >= survival_data$follow_up_start, ]
  }
  
  if(time_point == "reduced"){
    res_vacc <- cox_fit_model(event,subgroup,stratify_by_subgroup,stratify_by,mdl, survival_data,input,cuts_days_since_expo=cuts_days_since_expo_reduced,cuts_days_since_expo_reduced,covar_names,time_point)
  }else{
    res_vacc <- cox_fit_model(event,subgroup,stratify_by_subgroup,stratify_by,mdl, survival_data,input,cuts_days_since_expo, cuts_days_since_expo_reduced,covar_names,time_point)
  }
  
  print(paste0("Finished working on subgroup: ", subgroup, " ", exposure))
  return(res_vacc)
}


