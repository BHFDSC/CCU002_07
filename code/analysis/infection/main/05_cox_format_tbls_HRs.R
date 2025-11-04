## =============================================================================
## Format results into combined HR and event counts files
## =============================================================================

print("Working on formating tables")

rm(list=setdiff(ls(), c("con","DATABRICKS_GUID","mdl","raw_results_dir","compiled_results_dir","scripts_dir","analyses_to_run","event_name","exposure")))

results_needed=analyses_to_run

results_done <- c()
results_missing=data.frame()
for (i in 1:nrow(results_needed)) {
  row <- results_needed[i,]
  fpath <- file.path(raw_results_dir,
                     tolower(paste0("tbl_hr_",
                            row$event, "_",
                            row$subgroup, "_",
                            row$reduced_timepoint,"_time_periods_",
                            exposure,"_analysis.csv")))
  
  if (!file.exists(fpath)) {
    results_missing <- rbind(results_missing, row)
  } else {
    results_done <- c(results_done, fpath)
  }
}

result_file_paths <- pmap(list(results_done), 
                          function(fpath){ 
                            df <- fread(fpath) 
                            return(df)
                          })

df_hr <- rbindlist(result_file_paths, fill=TRUE)
df_hr <- df_hr %>% mutate_if(is.numeric, round, digits=5)%>%select(-V1)
write.csv(df_hr, paste0(compiled_results_dir,"/compiled_HR_results_", event_name,"_",exposure,"_analysis_NOT_FOR_RELEASE.csv") , row.names=F)
print(paste0("Compiled HR's saved: ",compiled_results_dir,"/compiled_HR_results_", event_name,"_",exposure,"_analysis_NOT_FOR_RELEASE.csv"))

# =============================  R events count ================================

event_count_missing <- data.frame()
event_count_done <- c()

for (i in 1:nrow(results_needed)) {
  row <- results_needed[i,]
  fpath <- file.path(raw_results_dir,
                     tolower(paste0("tbl_event_count_",
                            row$event, "_",
                            row$subgroup, "_",
                            row$reduced_timepoint,"_time_periods_",
                            exposure,"_analysis.csv")))
  
  if (!file.exists(fpath)) {
    event_count_missing <- rbind(event_count_missing, row)
  } else {
    event_count_done <- c(event_count_done, fpath)
  }
}

event_counts_completed <- pmap(list(event_count_done), 
                               function(fpath){ 
                                 df <- fread(fpath) 
                                 return(df)
                               })


df_event_counts <- rbindlist(event_counts_completed, fill=TRUE)  %>% dplyr::select(!c("V1","episode"))
write.csv(df_event_counts, paste0(compiled_results_dir,"/compiled_event_counts_", event_name, "_", exposure,"_analysis_NOT_FOR_RELEASE.csv") , row.names=F)
print(paste0("Compiled event counts saved: ", compiled_results_dir,"/compiled_event_counts_", event_name,"_", exposure,"_analysis_NOT_FOR_RELEASE.csv"))

# Add in suppression for counts <=5
df_event_counts$redacted_results <- "NA"
df_event_counts$person_time_days <- NULL
df_event_counts$`incidence rate (per 1000 person years)` <- NULL

supressed_df_event_counts <- df_event_counts[0,]

for(i in 1:nrow(analyses_to_run)){
  subgroup_of_interest=tolower(analyses_to_run$subgroup[i])
  time_points_of_interest=analyses_to_run$reduced_timepoint[i]
  
  tmp <- df_event_counts %>% filter(subgroup == subgroup_of_interest & time_points == time_points_of_interest)
  
  tmp$events_total <- as.numeric(tmp$events_total)
  tmp <- tmp %>% 
    mutate(events_total = replace(events_total, time_period=="all post expo", sum(tmp[which(tmp$events_total >5 & !(tmp$time_period %in% c("days_pre", "all post expo"))),events_total])))   
  tmp <- tmp %>% 
    # mutate(median_follow_up = replace(median_follow_up,events_total <=5, "[Redacted]"),
           mutate(events_total = replace(events_total, events_total <=5, "[Redacted]"))
  
  tmp$events_total <- as.character(tmp$events_total)
  tmp$redacted_results <- ifelse(any(tmp$events_total == "[Redacted]", na.rm = T), "Redacted results", "No redacted results")
  supressed_df_event_counts <- rbind(supressed_df_event_counts,tmp)
}

supressed_df_event_counts$redacted_results <- factor(supressed_df_event_counts$redacted_results, levels = c("Redacted results",
                                                                                                            "No redacted results"))
supressed_df_event_counts <- supressed_df_event_counts[order(supressed_df_event_counts$redacted_results),]

# Combine event counts and hazard ratios

supressed_df_event_counts <- supressed_df_event_counts %>% rename(term=time_period) %>% select(!exposure)
df_hr=df_hr%>%left_join(supressed_df_event_counts, by=c("term","event","subgroup","time_points")) %>%
  mutate(across(where(is.numeric), as.character))

# df_hr[which(df_hr$events_total == "[Redacted]"),c("hr","conf_low","conf_high","se_ln_hr","robust_se_ln_hr","median_follow_up","p_value")] = "[Redacted]"


supressed_df_hr <- df_hr[0,]

for(i in 1:nrow(analyses_to_run)){
  subgroup_of_interest=tolower(analyses_to_run$subgroup[i])
  time_points_of_interest=analyses_to_run$reduced_timepoint[i]
  
  tmp <- df_hr %>% filter(subgroup==subgroup_of_interest & time_points == time_points_of_interest)
  tmp$redacted_results <- replace_na(tmp$redacted_results,"No redacted results")
  
  tmp$redacted_results <- ifelse(any(tmp$redacted_results == "Redacted results"),"Redacted results", "No redacted results")
  supressed_df_hr <- rbind(supressed_df_hr,tmp)
}

supressed_df_hr$redacted_results <- factor(supressed_df_hr$redacted_results, levels = c("Redacted results",
                                                                                        "No redacted results"))
supressed_df_hr <- supressed_df_hr[order(supressed_df_hr$redacted_results),]

supressed_df_hr=supressed_df_hr%>%select(event,exposure,subgroup,model,time_points,term,hr,conf_low,conf_high,se_ln_hr,robust_se_ln_hr,p_value,
                                         events_total, median_follow_up,results_fitted,covariates_removed,redacted_results)

write.csv(supressed_df_hr,paste0(compiled_results_dir,"/suppressed_compiled_HR_results_",event_name,"_", exposure,"_NOT_FOR_RELEASE.csv") , row.names=F)
print(paste0("Supressed HR with event counts saved: ", compiled_results_dir,"/suppressed_compiled_HR_results_",event_name,"_", exposure,"_NOT_FOR_RELEASE.csv"))

supressed_df_hr <- supressed_df_hr %>% select(!c("events_total")) %>%
  filter(!(term %in% c("days_pre","all post expo")))

write.csv(supressed_df_hr,paste0(compiled_results_dir,"/suppressed_compiled_HR_results_",event_name,"_", exposure,"_TO_RELEASE.csv") , row.names=F)
print(paste0("Supressed HR to release saved: ", compiled_results_dir,"/suppressed_compiled_HR_results_",event_name,"_", exposure,"_TO_RELEASE.csv"))

