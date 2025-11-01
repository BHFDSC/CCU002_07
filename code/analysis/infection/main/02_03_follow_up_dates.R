## =============================================================================
## Pipeline (3): Calculates follow-up dates
## Authors: Alexia Sampri, Wen Shi
## =============================================================================

# Determine follow up end dates
print("Determine follow-up start and end dates")

input$cohort_start_date <- cohort_start_date
input$cohort_end_date<- cohort_end_date

if(exposure=="covid"){
  input$follow_up_start <- apply(input[,c("cohort_start_date","birth_date")],1, max, na.rm=TRUE)
  input$follow_up_start <- as.Date(input$follow_up_start)
  
  # input$follow_up_end <- apply(input[,c("cohort_end_date","death_date","event_date","birthday_18","vax_date_1")],1, min, na.rm=TRUE)
  input$follow_up_end <- apply(input[,c("cohort_end_date","death_date","event_date","vax_date_1")],1, min, na.rm=TRUE)
  input$follow_up_end <- as.Date(input$follow_up_end)
  
}else if(exposure=="vaccine"){
  input$follow_up_start <- cohort_start_date
  input$follow_up_start <- as.Date(input$follow_up_start)
  
  input$follow_up_end <- apply(input[,c("cohort_end_date","death_date","event_date","birthday_18","vax_date_2")],1, min, na.rm=TRUE)
  input$follow_up_end <- as.Date(input$follow_up_end)
  
}

# Ensure follow-up end is after follow-up start
input <- input[input$follow_up_end >= input$follow_up_start, ]

# Remove exposures and outcomes outside follow-up ------------------------------
print("Remove exposures and outcomes outside follow-up")

cat('No of event outcomes before processing: ' , input[!is.na(event_date),.N],'\n','No of exposure events before processing: ' , input[!is.na(expo_date),.N],'\n',sep='')

input <- input %>% mutate(event_date = replace(event_date, which(event_date>follow_up_end | event_date<follow_up_start), NA),
                          expo_date = replace(expo_date, which(expo_date>follow_up_end | expo_date<follow_up_start), NA))

cat('No of event outcomes after processing: ' , input[!is.na(event_date),.N],'\n','No of exposure events after processing: ' , input[!is.na(expo_date),.N],'\n',sep='')

if(exposure == "covid" & "covid_pheno" %in% analyses_to_run$subgroup_cat){
  print("Determine hospitalised and non-hospitalised covid end dates")
  # Update COVID phenotypes after setting COVID exposure dates to NA that lie outside follow up
  input=input %>% mutate(expo_pheno = replace(covid_expo_pheno, which(is.na(expo_date)),"no_infection"))
  
  # Get COVID pheno specific dataset if necessary
  # Adds in variable covid_expo_censor which is the COVID exposure date for the phenotype  not of interest
  # We want to be able to include follow up time prior to exposure for the pheno no of interest which uses date_expo_censor
  # to find this time period
  for(pheno in c("hospitalised","non_hospitalised")){
    input$covid_expo_censor <- as.Date(ifelse(!(input$covid_expo_pheno %in% pheno),
                                              input$expo_date, 
                                              NA), origin='1970-01-01')
    setnames(input,
             old="covid_expo_censor",
             new=paste0(pheno,"_covid_expo_censor"))
  }
  
  input$hospitalised_follow_up_end <- apply(input[,c("follow_up_end","hospitalised_covid_expo_censor")],1, min,na.rm=TRUE)
  input$non_hospitalised_follow_up_end <- apply(input[,c("follow_up_end","non_hospitalised_covid_expo_censor")],1, min,na.rm=TRUE)
  
  input$hospitalised_follow_up_end <- as.Date(input$hospitalised_follow_up_end)
  input$non_hospitalised_follow_up_end <- as.Date(input$non_hospitalised_follow_up_end)
  
}






