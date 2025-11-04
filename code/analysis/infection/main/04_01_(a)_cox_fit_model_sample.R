## =============================================================================
## 1.Calls the functions that formats the survival data into the relevant format
## to be used in the cox model
## 2.Defines the cox survival formula and fits the cox model
## 3.Format the results table
## Authors: Wen Shi, Alexia Sampri
## =============================================================================
source(file.path(scripts_dir,"04_01_(b)_cox_format_survival_data_sample.R"))


#------------------FORMAT SURVIVAL DATASET AND RUN COX MODEL--------------------

cox_fit_model <- function(event,subgroup,stratify_by_subgroup,stratify_by,mdl, survival_data,input,cuts_days_since_expo,cuts_days_since_expo_reduced,covar_names,time_point){

  list_data_surv <- format_surval_data(event,subgroup, stratify_by_subgroup, stratify_by,survival_data,cuts_days_since_expo,time_point)
  
  if(length(list_data_surv)==1){
    analyses_not_run <<- list_data_surv[[1]]
    # return(fit_model_reducedcovariates)
  }
  data_surv <- list_data_surv[[1]]
  interval_names <-list_data_surv[[2]]
  collapse_time_periods <- list_data_surv[[3]]
  cox_weight <- list_data_surv[[4]]
  
  if(collapse_time_periods==TRUE){
    list_data_surv <- format_surval_data(event,subgroup, stratify_by_subgroup, stratify_by,survival_data, cuts_days_since_expo=cuts_days_since_expo_reduced,time_point)
    data_surv <- list_data_surv[[1]]
    interval_names <-list_data_surv[[2]]
  }
  
  #Select covariates if using model mdl_max_adj
  if("mdl_max_adj_region" %in% mdl){
    # no cov_bin_vascular_disease, cov_bin_other_disease
    covars=input%>%dplyr::select(all_of(covar_names))
    
    
    covar_names = names(covars)[ names(covars) != "patient_id"]
    data_surv <- data_surv %>% left_join(covars)
  }
  
  # if(subgroup == "main" & adjust_for_expo == "TRUE"){
  #   # no adjust for expo covariate
  #   data_surv <- data_surv %>% left_join(input %>% select(patient_id,adjust_for_expo_covariate))
  # }
  
  # Fit model and prep output csv
  fit_model <- coxfit(data_surv, interval_names, covar_names, subgroup, mdl, cox_weight)
  # fit_model <- coxfit(data_surv, interval_names, covar_names, subgroup, mdl)
  fit_model$subgroup <- subgroup
  fit_model$event <- event
  fit_model$exposure <- exposure
  fit_model$time_points <- time_point
  fit_model$data_sampled <- ifelse(cox_weight == 1, "FALSE", "TRUE")
  fit_model$N_sample_size <- length(unique(data_surv$patient_id))
  
  write.csv(fit_model, paste0(raw_results_dir,"/tbl_hr_" , event, "_",subgroup,"_",time_point, "_time_periods_",exposure,"_analysis.csv"), row.names = T)
  print(paste0("Hazard ratios saved: ", raw_results_dir,"/tbl_hr_" , event, "_",subgroup,"_",time_point, "_time_periods_",exposure,"_analysis.csv"))
  return (fit_model)
}


#------------------------ GET SURV FORMULA & COXPH() ---------------------------
coxfit <- function(data_surv, interval_names, covar_names, subgroup, mdl,cox_weight){
# coxfit <- function(data_surv, interval_names, covar_names, subgroup, mdl){
  print("Working on cox model")
# error in function rm_lowvar_covars
  # if("mdl_max_adj" %in% mdl){
  #   covars_to_remove <- rm_lowvar_covars(data_surv)[!is.na((rm_lowvar_covars(data_surv)))]
  #   print(paste0("Covariates removed: ", covars_to_remove))
  #   data_surv <- data_surv %>% dplyr::select(!all_of(covars_to_remove))
  # 
  # 
  #    collapse_covars_list=collapse_categorical_covars(data_surv,subgroup)
  #    data_surv=collapse_covars_list[[1]]
  #    covars_collapsed=collapse_covars_list[[2]]
  #    covars_collapsed=unique(covars_collapsed[covars_collapsed %in% c("cov_cat_deprivation","cov_cat_smoking_status")])
  #    print(paste0("Categorical covariates collapsed: ", covars_collapsed))
  # }

  print("Post Exposure event counts split by covariate levels")
  
  covars_to_remove = c()
  
  if(!"mdl_max_adj_region" %in% mdl){
    print(covariate_exploration(data_surv, c()))
  }else if("mdl_max_adj_region" %in% mdl){
    covars_to_print <- covar_names[!covar_names %in% covars_to_remove]
    print(covariate_exploration(data_surv, c(covars_to_print,"ethnicity")))
  }
  
  covariates <- covar_names[covar_names %in% names(data_surv)] %>% sort()
  
  # get Survival formula ----
  covariates_excl_region_sex_age <- unique(c(interval_names, covariates))
  
  combined_results <- as.data.frame(matrix(ncol=9,nrow=0))
  colnames(combined_results) <- c("term","hr","conf_low","conf_high","se_ln_hr","robust_se_ln_hr","p_value", "covariates_removed","mdl")
  
  if(subgroup == "main" & adjust_for_expo == "TRUE"){
    mdl <- append(mdl,"mdl_max_adj_add_exposure_history")
  }
  
  for(model in mdl){
    #Base formula
    if(model %in% c( "mdl_age_sex","mdl_age_agesq_sex","mdl_age_sex_region")){
      surv_formula <- paste0(
        "Surv(tstart, tstop, event) ~ ",
        paste(interval_names, collapse="+"))
    }else if (model %in% c("mdl_max_adj","mdl_max_adj_agesq","mdl_max_adj_region","mdl_max_adj_add_exposure_history")){
      surv_formula <- paste0(
        "Surv(tstart, tstop, event) ~ ",
        paste(covariates_excl_region_sex_age, collapse="+"))
    }
    
    # Add in region as covariate for relevant models
    if (model %in% c("mdl_age_sex_region","mdl_max_adj_region")){
      surv_formula <- paste(surv_formula, "region_name", sep="+")
    }
    
    #If subgroup is not sex then add sex into formula
    if ((startsWith(subgroup,"sex"))==F & (!"sex" %in% covariates_excl_region_sex_age)){
      surv_formula <- paste(surv_formula, "sex", sep="+")
    }
    
    #If subgroup is not ethnicity then add ethnicity into formula
    if ((startsWith(subgroup,"ethnicity"))==F & (!"ethnicity" %in% covariates_excl_region_sex_age) & model %in% c("mdl_max_adj","mdl_max_adj_agesq","mdl_max_adj_region","mdl_max_adj_add_exposure_history")){
      surv_formula <- paste(surv_formula, "ethnicity", sep="+")
    }
    
    #If subgroup is not age then add in age and age_sq
    if ((startsWith(subgroup,"agegp_"))==F & model %in% c("mdl_age_sex","mdl_age_sex_region","mdl_max_adj","mdl_max_adj_region","mdl_max_adj_add_exposure_history")){
      surv_formula <- paste(surv_formula, "age", sep="+")
    }else if((startsWith(subgroup,"agegp_"))==F & model %in% c("mdl_age_agesq_sex","mdl_max_adj_agesq")){
      surv_formula <- paste(surv_formula, "age + age_sq", sep="+")
    }
    
    if(model == "mdl_max_adj_add_exposure_history"){
      surv_formula <- paste(surv_formula,adjust_for_expo_covariate, sep="+")
    }
    
    print(surv_formula)
    
    # fit cox model
    # dd <<- datadist(data_surv)
    # options(datadist="dd", contrasts=c("contr.treatment", "contr.treatment"))
    print("Fitting cox model")
    fit_cox_model <-survival::coxph(formula=as.formula(surv_formula),data=data_surv
                                    , weight=data_surv$cox_weights
                                    ,id=patient_id, robust = T)
    
    print("Cox output")
    print(fit_cox_model)
    print("Finished fitting cox model")
    
    # Results ----
    results <- tidy(fit_cox_model)
    if(cox_weight ==1){
       results$conf_low <- exp(results$estimate - qnorm(0.975)*results$std.error)
       results$conf_high <- exp(results$estimate + qnorm(0.975)*results$std.error)
     }else{
      results$conf_low <- exp(results$estimate - qnorm(0.975)*results$robust.se)
      results$conf_high <- exp(results$estimate + qnorm(0.975)*results$robust.se)
     }
    
    results$hr <- exp(fit_cox_model$coefficients)
    results <- results %>% rename(ln_hr = estimate,
                                   se_ln_hr = std.error,
                                   robust_se_ln_hr = robust.se,
                                   p_value = p.value) %>%
       select(term,hr,conf_low,conf_high,se_ln_hr,robust_se_ln_hr, p_value)
    
    
    if(model == "mdl_max_adj_region"){
      results$covariates_removed=paste0(covars_to_remove, collapse = ",")
      #results$cat_covars_collapsed=paste0(covars_collapsed, collapse = ",")
    }else{
      results$covariates_removed <- NA
      #results$cat_covars_collapsed <- NA
    }
    
    print("Print results")
    print(results)
    
    results$results_fitted <- ifelse(all(results$estimate < 200 & results$std.error <10 & results$robust.se <10),"fitted_successfully","fitted_unsuccessfully")
    df <- as.data.frame(matrix(ncol = ncol(results),nrow = 2))
    colnames(df) <- colnames(results)
    df$term <- c("days_pre", "all post expo")
    results <- rbind(df, results)
    
    results$model <- model
    combined_results <- rbind(combined_results,results)
  }
  
  print("Finised working on cox model")
  return(combined_results)
}




