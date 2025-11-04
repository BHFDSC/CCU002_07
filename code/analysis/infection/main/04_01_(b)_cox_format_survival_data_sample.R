## =============================================================================
## 1. Format the survival data for the cox model
## 2. Calculate pre/post exposure event counts
## =============================================================================

format_surval_data <- function(event,subgroup, stratify_by_subgroup, stratify_by, survival_data,cuts_days_since_expo,time_point){
  print(paste0("Starting survival data"))
  #------------------ RANDOM SAMPLE NON-CASES for IP WEIGHING ------------------
  set.seed(137)
  
  cases <- survival_data %>% filter(!is.na(event_date) & event_date == follow_up_end)
  
  print(paste0("Total number in survival data: ", nrow(survival_data)))
  print(paste0("Number of cases: ", nrow(cases)))
  
  non_cases <- survival_data %>% filter(!patient_id %in% cases$patient_id)
  
  if(nrow(cases)*controls_per_case < nrow(non_cases)){
    non_cases <- non_cases[sample(1:nrow(non_cases), nrow(cases)*controls_per_case,replace=FALSE), ]
   }else if (nrow(cases)*controls_per_case >= nrow(non_cases)){
   non_cases=non_cases
  }

  non_case_inverse_weight=(nrow(survival_data)-nrow(cases))/nrow(non_cases)
  survival_data <- bind_rows(cases,non_cases)
  noncase_ids <- unique(non_cases$patient_id)

  print(paste0("Number of controls: ", nrow(non_cases)))
  print(paste0("Controls weight: ", non_case_inverse_weight))

  # Add inverse probablity weights for non-cases
  survival_data$cox_weights <- ifelse(survival_data$patient_id %in% noncase_ids, non_case_inverse_weight, 1)

  survival_data$days_to_start <- as.numeric(survival_data$follow_up_start-cohort_start_date)
  survival_data$days_to_end <- as.numeric(survival_data$follow_up_end-cohort_start_date) +1
  
  #===============================================================================
  #   CACHE some features
  #-------------------------------------------------------------------------------  
  df_cache <- survival_data %>% dplyr::select(patient_id, sex, age, region_name, ethnicity 
                                              ,cox_weights
  )
  df_cache$age_sq <- df_cache$age^2
  
  #===============================================================================
  # WITH COVID
  #-------------------------------------------------------------------------------
  with_expo <- survival_data %>% filter(!is.na(expo_date))
  
  # Check whether there are any people with COVID exposure
  any_exposures <- nrow(with_expo)>0
  
  # Check whether there are any people with post-expo events
  any_exposed_events <- nrow(with_expo %>% filter(!is.na(event_date)))>0
  
  if(any_exposures==T & any_exposed_events ==T ){
    with_expo <- with_expo %>% 
      dplyr::select(patient_id, expo_date, follow_up_end, event_date, days_to_start, days_to_end) %>%  
      mutate(event_status = if_else((!is.na(event_date)), 1, 0)) 
    
    # ......................................
    # CHUNK UP FOLLOW-UP PERIOD by CHANGE OF STATE OF EXPOSURE
    
    # Incompatible methods ("-.IDate", "-.Date") for "-" 
    # with_expo$day_to_expo <- as.numeric(with_expo$expo_date - cohort_start_date)
    with_expo$day_to_expo <- as.numeric(with_expo$expo_date) - as.numeric(cohort_start_date)
    
    d1 <- with_expo %>% dplyr::select(patient_id, expo_date, event_date)
    d2 <- with_expo %>% dplyr::select(patient_id, days_to_start, day_to_expo, days_to_end, event_status)
    with_expo <- tmerge(data1=d1,
                        data2=d2,
                        id=patient_id,
                        event=event(days_to_end, event_status),
                        tstart=days_to_start,
                        tstop = days_to_end,
                        expo=tdc(day_to_expo)) 
    
    with_expo$id <- NULL
    
    # ----------------------- SPLIT POST-COVID TIME------------------------------
    with_expo_postexpo <- with_expo %>% filter(expo==1)
    
    ## Format tstart and tstop
    # renames? Error in rename(., t0 = tstart, t = tstop) : 
      # unused arguments (t0 = tstart, t = tstop)
    with_expo_postexpo <- with_expo_postexpo %>% dplyr::rename(t0=tstart, t=tstop) %>% mutate(tstart=0, tstop=t-t0)
    
    ## Implement post-exposure cut points
    with_expo_postexpo <- survSplit(Surv(tstop, event)~., 
                                    with_expo_postexpo,
                                    cut=cuts_days_since_expo,
                                    episode="episode"
    )
    
    with_expo_postexpo <- with_expo_postexpo %>% mutate(tstart=tstart+t0, tstop=tstop+t0) %>% dplyr::select(-c(t0,t))
    
    # Combine pre- and post-exposure time for the exposed ------------------------
    ## Filter to pre-exposure data
    with_expo_preexpo <- with_expo %>% filter(expo==0)
    
    ## Set pre-exposure to be episode 0
    with_expo_preexpo$episode <- 0
    
    ## Bind pre- and post-exposure time
    with_expo <- plyr::rbind.fill(with_expo_preexpo, with_expo_postexpo)
    
    # rm(list=c("with_expo_preexpo", "with_expo_postexpo", "d1", "d2", "non_cases", "cases"))
    rm(list=c("with_expo_preexpo", "with_expo_postexpo", "d1", "d2", "cases"))
    
    
  }
  
  # Set survival data for the unexposed ----------------------------------------
  
  without_expo <- survival_data %>%filter(is.na(expo_date)) 
  # any_no_expo <- nrow(with_expo)>0
  any_no_expo <- nrow(without_expo)>0
  any_noexpo_events = nrow(without_expo %>% filter(!is.na(event_date)))>0
  
  # if(any_no_expo == T & any_exposures== T & any_exposed_events == T ){
  if(any_no_expo == T &  any_noexpo_events == T ){
    without_expo <- without_expo %>%
      dplyr::select(patient_id, expo_date, follow_up_end, event_date, days_to_start, days_to_end) %>% 
      mutate(event = if_else((!is.na(event_date)),1,0))
    
    ## Rename variables to survival variable names
    
    without_expo <- dplyr::rename(without_expo,
                                  "tstart" = "days_to_start",
                                  "tstop" = "days_to_end")
    
    without_expo$expo<- c(0)
    without_expo$episode <- c(0)
    
    # Combine exposed and unexposed individuals ----------------------------------
    common_cols <- intersect(colnames(without_expo), colnames(with_expo))
    without_expo <- without_expo %>% dplyr::select(all_of(common_cols))
    with_expo <- with_expo %>% dplyr::select(all_of(common_cols))
    data_surv <-rbind(without_expo, with_expo)
    
    # Define episode labels --------------------------------------------------------
    episode_labels <- data.frame(episode = 0:length(cuts_days_since_expo),
                                 time_period = c("days_pre",paste0("days", c("0", cuts_days_since_expo[1:(length(cuts_days_since_expo)-1)]),"_", cuts_days_since_expo)),
                                 stringsAsFactors = FALSE)
    
    interval_names <- episode_labels$time_period[episode_labels$time_period != "days_pre"]
    
    
    # Add indicators for episode -------------------------------------------------
    
    for (i in 1:max(episode_labels$episode)) {
      
      preserve_cols <- colnames(data_surv) 
      
      data_surv$tmp <- as.numeric(data_surv$episode==i)
      
      colnames(data_surv) <- c(preserve_cols,episode_labels[episode_labels$episode==i,]$time_period)
      
    }
    
    
    # Join age, region, ethnicity onto data_surv ----------------------------------
    data_surv <- data_surv %>% left_join(df_cache)
    print(paste0("Finished survival data"))
    
    # Calculate number of events per episode -------------------------------------
    print("Calculating event counts")
    events <- data_surv[data_surv$event ==1, c("patient_id","episode")]
    
    events <- aggregate(episode ~ patient_id, data = events, FUN = max)
    
    events <- data.frame(table(events$episode), 
                         stringsAsFactors = FALSE)
    
    events <- dplyr::rename(events, "episode" = "Var1", "events_total" = "Freq")
    
    # Add number of events to episode info table ---------------------------------
    
    episode_info <- merge(episode_labels, events, by = "episode", all.x = TRUE)
    episode_info$events_total <- ifelse(is.na(episode_info$events_total),0,episode_info$events_total)
    episode_info[nrow(episode_info) + 1,] = c(max(episode_info$episode)+1,"all post expo",  sum(episode_info[which(episode_info$episode != 0),"events_total"]))
    
    # Calculate person-time in each episode --------------------------------------
    
    tmp <- data_surv[,c("episode","tstart","tstop","cox_weights")]
    # tmp <- data_surv[,c("episode","tstart","tstop")]
    
    tmp$person_time_days <- (tmp$tstop - tmp$tstart)
    tmp <- rbind(tmp,tmp %>% filter(episode !=0) %>% mutate(episode = max(episode_info$episode)))
    tmp[,c("tstart","tstop","cox_weights")] <- NULL
    # tmp[,c("tstart","tstop")] <- NULL
    
    tmp <- aggregate(person_time_days ~ episode, data = tmp, FUN = sum)
    
    episode_info <- merge(episode_info, tmp, by = "episode", all.x = TRUE)
    
    # Calculate incidence ------------------------------------------------------
    episode_info <- episode_info %>% mutate(across(c(person_time_days,events_total),as.numeric))
    episode_info <- episode_info %>% mutate("incidence rate (per 1000 person years)" = (events_total/(person_time_days/365.2))*1000 )
    
    # Calculate median person-time -----------------------------------------------
    #Median person time of those with an event within each time period used for plotting figures.
    
    tmp <- data_surv[data_surv$event ==1,c("patient_id","episode","tstart","tstop")]
    tmp$person_time <- tmp$tstop - tmp$tstart
    
    tmp[,c("patient_id","tstart","tstop")] <- NULL
    
    tmp <- tmp %>% group_by(episode) %>% 
      summarise(median_follow_up = median(person_time, na.rm = TRUE))
    
    episode_info <- merge(episode_info, tmp, by = "episode", all.x = TRUE)
    
    episode_info$event <- event
    episode_info$subgroup <- subgroup
    episode_info$exposure <- exposure
    episode_info$time_points <- time_point
    episode_info$events_total <- as.numeric(episode_info$events_total)
    
    #Any time periods with <=5 events? If yes, will reduce time periods
    #collapse_time_periods <- any((episode_info$events_total <= 5) & (!identical(cuts_days_since_expo, cuts_days_since_expo_reduced)))
    collapse_time_periods <- FALSE
    
    # If collapse_time_periods==TRUE then this script will re-run again with reduced time periods and
    # we only want to save the final event count file. For reduced time periods, collapse_time_periods will always be FALSE
    
    if(collapse_time_periods==FALSE){
      write.csv(episode_info, paste0(raw_results_dir,"/tbl_event_count_" ,event,"_", subgroup,"_",time_point,"_time_periods_",exposure,"_analysis.csv"), row.names = T)
      print(paste0("Event counts saved: ", raw_results_dir,"/tbl_event_count_" ,event,"_", subgroup,"_",time_point,"_time_periods_",exposure,"_analysis.csv"))
    }
    
    
    return(list(data_surv, interval_names, collapse_time_periods,non_case_inverse_weight))
    # return(list(data_surv, interval_names, collapse_time_periods))
    
  }else{
    analyses_not_run[nrow(analyses_not_run)+1,]<- c(event,subgroup,exposure,any_exposures,any_exposed_events,any_no_expo,"FALSE")
    
    return(list(analyses_not_run))
  }
  
  
}
