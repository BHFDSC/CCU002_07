# library(ggplot2)
# library(dplyr)
# library(purrr)
# library(readr)  # For read_csv, which is faster and handles data types better than read.csv


#### Calculate AER for infection ####

library(data.table)
library(survival)
library(tidyverse)
library(lubridate)
library(ggplot2)
library(ggthemes)


summary_stat<-function(dataset){
  dimension=dim(dataset)
  str=sapply(dataset, class)
  summarised=summary(dataset)
  my_return_list<-list(dimension=dimension,str=str,summarised=summarised)
  return(my_return_list)
}

checks<-function(dataset){
  date_columns<-dataset %>%
    select_if(is.Date)
  
  result1<-sapply(date_columns, function(x) any(x<start_date, na.rm = T)) #start_date is the cohort follow-up start date 
  result2<-sapply(date_columns, function(x) any(x>end_date, na.rm = T)) #end_date is the cohort follow-up end date
  result_list<-list(any_before_start_date=result1, any_after_end_date=result2)
  return(result_list)
}

count_columns<-function(dataset,column_names){
  date_columns<-dataset %>%
    select(all_of(column_names))
  
  return(sapply(date_columns, function(x)sum(!is.na(x)))) 
  
}

to_calculate_futime<-function(cohort_start, covid_date, death_date, disease_event_date, max_follow_up_time){
  futime<-ifelse(is.na(covid_date) & is.na(death_date) & is.na(disease_event_date), max_follow_up_time, 
                 ifelse(is.na(covid_date)& !is.na(disease_event_date), as.numeric(disease_event_date - cohort_start)+1,
                        ifelse(is.na(covid_date) & !is.na(death_date), as.numeric(death_date-cohort_start)+1,
                               ifelse(!is.na(covid_date) & is.na(disease_event_date), as.numeric(covid_date-cohort_start),
                                      ifelse(!is.na(covid_date) & disease_event_date<covid_date, as.numeric(disease_event_date-cohort_start)+1,
                                             ifelse(!is.na(covid_date) & disease_event_date>covid_date, as.numeric(covid_date-cohort_start), 
                                                    ifelse(!is.na(covid_date) & disease_event_date==covid_date, as.numeric(covid_date-cohort_start),'to check')))))))
  
  futime<-as.numeric(futime)
  futime<-ifelse(futime>max_follow_up_time, max_follow_up_time, futime)
  
  return(futime)
  
}

average_daily_incidence<-function(dataset, disease_event_date, covid_date,futime,max_follow_up_time){
  
  #Daily incidence formula in the unexposed period: #number of disease-of-interest events occurred during the day in the unexposed period/#number of disease-free (at risk) individuals at the start of the day in the unexposed period
  
  #Note:as we taking 'day' as the unit, the daily incidence calculated here is both daily incidence proportion and daily incidence rate
  
  
  #First, define disease status for each individual in the unexposed period: code 1 for the disease of interest occurred in the unexposed period and 0 otherwise
  disease_status<-ifelse(is.na(disease_event_date),0,
                         ifelse(!is.na(disease_event_date) & is.na(covid_date),1,
                                ifelse(!is.na(disease_event_date) & disease_event_date>=covid_date,0,1))) 
  
  dataset$disease_status<-as.integer(disease_status)
  
  #Next, use the survfit() function in the survival package and the follow-up time calculated using to_calculate_futime() to summarise the number of daily disease event (nominator of the daily incidence formula) and the number of daily at risk individuals (denominator of the daily incidence formula) in the unexposed period
  fit<-survfit(Surv(futime,dataset$disease_status) ~1) 
  
  #Put the date, numbers of daily disease-event (nominator) and numbers of individuals at risk (denominator) into a new dataframe 
  daily_incidence<-data.frame(time=fit$time,
                              n_event=fit$n.event,
                              n_risk=fit$n.risk)
  
  #calculate daily incidence for disease of interest in the unexposed period
  daily_incidence$incidence_proportion<-daily_incidence$n_event/daily_incidence$n_risk
  
  #In the last step, average the daily incidence across the cohort follow-up period to get an average daily incidence for disease of interest 
  average_daily_incidence<-sum(daily_incidence$incidence_proportion)/max_follow_up_time 
  
  #two other useful summary level information to put in the output of this function
  event_count_unexposed<-sum(dataset$disease_status==1) #disease of interest counts in the unexposed period
  exposed_individual<-sum(!is.na(dataset$exp_date_covid19_confirmed))  #number of individuals (exposed) with a COVID-19 diagnosis during cohort follow-up
  
  return_list<-c(event_count_unexposed=event_count_unexposed, 
                 average_daily_incidence=average_daily_incidence,
                 number_of_exposed_individuals=exposed_individual)
  
  return(return_list)
}

incidence_rate<-function(disease_event_date,covid_date,futime){
  
  #calculates the nominator
  total_event<-sum(!is.na(disease_event_date)) 
  event_exposed<-sum(disease_event_date>=covid_date,na.rm = T)
  event_unexposed<-total_event-event_exposed
  
  #calculates the denominator
  follow_up_time<-sum(futime)
  
  #calculates the incidence rate
  incidence_rate<-event_unexposed/follow_up_time
  
  return(incidence_rate)
}


AER_calculation<-function(dataframe_name,event_name,subgroup_name,q,HR,HR_LB,HR_UB,covid_infected_population,total_num_rows_for_life_table,weeks_since_covid){
  event<-rep(event_name,total_num_rows_for_life_table) 
  subgroup<-rep(subgroup_name,total_num_rows_for_life_table)
  x_days_since_covid<-c(1:total_num_rows_for_life_table)
  weeks_since_covid<-weeks_since_covid
  q_estimated_average_incidence_rate<-rep(q, total_num_rows_for_life_table)
  life_table<-data.frame(event,subgroup,x_days_since_covid,weeks_since_covid,q_estimated_average_incidence_rate,HR,HR_LB,HR_UB)
  life_table$one_minus_q<-1-life_table$q_estimated_average_incidence_rate
  life_table$S<-cumprod(life_table$one_minus_q)
  life_table$qh<-life_table$q_estimated_average_incidence_rate*life_table$HR
  life_table$qh_LB<-life_table$q_estimated_average_incidence_rate*life_table$HR_LB
  life_table$qh_UB<-life_table$q_estimated_average_incidence_rate*life_table$HR_UB
  life_table$one_minus_qh<-1- life_table$qh
  life_table$SC<-cumprod(life_table$one_minus_qh)
  life_table$one_minus_qh_LB<-1- life_table$qh_LB
  life_table$SC_LB<-cumprod(life_table$one_minus_qh_LB)
  life_table$one_minus_qh_UB<-1- life_table$qh_UB
  life_table$SC_UB<-cumprod(life_table$one_minus_qh_UB)
  life_table$AER<-life_table$S- life_table$SC
  life_table$AER_percent<-life_table$AER*100
  life_table$AER_LB<-life_table$S- life_table$SC_LB
  life_table$AER_percent_LB<-life_table$AER_LB*100
  life_table$AER_UB<-life_table$S- life_table$SC_UB
  life_table$AER_percent_UB<-life_table$AER_UB*100
  assign(x = dataframe_name, value = life_table, envir = globalenv())
  excess_mean<-covid_infected_population*life_table[total_num_rows_for_life_table,20]
  excess_LB<-covid_infected_population*life_table[total_num_rows_for_life_table,22]
  excess_UB<-covid_infected_population*life_table[total_num_rows_for_life_table,24]
  return_list<-list(excess_events_mean=excess_mean, excess_events_LB=excess_LB, excess_events_UB=excess_UB)
  return(return_list) #this function returns changes in AER, total number of excess disease events attributable to COVID-19 infection and the 95% confidence intervals for excess events
}

plot_graph<-function(num_subgroups, life_table1,lifetable2,lifetable3,lifetable4,lifetable5, graph_title,graph_name,levels,legend_title,color_value,linetype){
  library(ggplot2)
  #library(ggthemes) optional package. If working in the English DAE (Data Access Environment), it does not have ggthemes package installed --> admin required to install this package 
  if(num_subgroups==0){#if there is no subgroup, i.e. plotting for all and not split by sex for example, put num_subgroups=0 in the function argument.
    
    life_table1$x_days_since_covid_converted<-life_table1$x_days_since_covid/7
    
    p_line<-ggplot(life_table1,
                   aes(x=x_days_since_covid_converted,
                       y=AER_percent,
                       lty = 'All')) +
      geom_line(size=1.5,colour=color_value)+
      # scale_x_continuous(breaks = c(0,10,20,30,40,50),limits = c(0,50))+
      scale_x_continuous(breaks = seq(0,72,4),limits = c(0,72))+
      
      scale_color_manual(values = color_value)+
      labs(x='Weeks since COVID-19 diagnosis',y='Difference in cumulative absolute risk (%)',
           title = graph_title)+
      theme(plot.title = element_text(hjust = 0.5))+
      #theme_hc()+ this background requires ggthemes package
      scale_linetype('')
    
    assign(x = graph_name, value = p_line, envir = globalenv())
    
    return(p_line)
  }
  
  if(num_subgroups==2){
    combined<-rbind(life_table1,lifetable2)
  }
  
  if(num_subgroups==3){
    combined<-rbind(life_table1,lifetable2,lifetable3)
  }
  
  if(num_subgroups==4){
    combined<-rbind(life_table1,lifetable2,lifetable3, lifetable4)
  }
  if(num_subgroups==5){
    combined<-rbind(life_table1,lifetable2,lifetable3,lifetable4, lifetable5)
  }  
  
  combined$x_days_since_covid_converted<-combined$x_days_since_covid/7
  combined$subgroup<-factor(combined$subgroup,levels = levels)
  
  p_line<-ggplot(combined,
                 aes(x=x_days_since_covid_converted,
                     y=AER_percent,
                     colour=subgroup)) +
    geom_line(aes(linetype=subgroup, colour=subgroup), size=1)+
    scale_linetype_manual(values = linetype)+
    scale_color_manual(values = color_value)+
    # scale_x_continuous(breaks = c(0,10,20,30,40,50),limits = c(0,50))+
    scale_x_continuous(breaks = seq(0,72,4),limits = c(0,72))+
    
    labs(x='Weeks since COVID-19 diagnosis',y='Difference in cumulative absolute risk (%)',
         title = graph_title)+
    theme(plot.title = element_text(hjust = 0.5))+
    #theme_hc()+ this background requires ggthemes package
    theme(legend.key.size = unit(1.2,'cm'), legend.key = element_blank())+
    labs(color=legend_title,linetype=legend_title)
  
  assign(x = graph_name, value = p_line, envir = globalenv())
  
  return(p_line)
}

save_plot<-function(file_location,data,filename,dpi, width,height){
  setwd(file_location)
  ggsave(plot=data, filename = paste0(filename, ".png"), dpi=dpi,width = width,height = height)
  
}

# ==============================================================================================
setwd("/db-mnt/databricks/rstudio_collab/CCU002_07/infection/riskratio_wen")
dataset = readRDS('cox_infec_ecds_050725_rmhistVaxInf.rds')
# library(ggplot2)
# library(dplyr)
# library(purrr)
# library(readr)  # For read_csv, which is faster and handles data types better than read.csv


#### Calculate AER for infection ####

library(data.table)
library(survival)
library(tidyverse)
library(lubridate)
library(ggplot2)
library(ggthemes)


summary_stat<-function(dataset){
  dimension=dim(dataset)
  str=sapply(dataset, class)
  summarised=summary(dataset)
  my_return_list<-list(dimension=dimension,str=str,summarised=summarised)
  return(my_return_list)
}

checks<-function(dataset){
  date_columns<-dataset %>%
    select_if(is.Date)
  
  result1<-sapply(date_columns, function(x) any(x<start_date, na.rm = T)) #start_date is the cohort follow-up start date 
  result2<-sapply(date_columns, function(x) any(x>end_date, na.rm = T)) #end_date is the cohort follow-up end date
  result_list<-list(any_before_start_date=result1, any_after_end_date=result2)
  return(result_list)
}

count_columns<-function(dataset,column_names){
  date_columns<-dataset %>%
    select(all_of(column_names))
  
  return(sapply(date_columns, function(x)sum(!is.na(x)))) 
  
}

to_calculate_futime<-function(cohort_start, covid_date, death_date, disease_event_date, max_follow_up_time){
  futime<-ifelse(is.na(covid_date) & is.na(death_date) & is.na(disease_event_date), max_follow_up_time, 
                 ifelse(is.na(covid_date)& !is.na(disease_event_date), as.numeric(disease_event_date - cohort_start)+1,
                        ifelse(is.na(covid_date) & !is.na(death_date), as.numeric(death_date-cohort_start)+1,
                               ifelse(!is.na(covid_date) & is.na(disease_event_date), as.numeric(covid_date-cohort_start),
                                      ifelse(!is.na(covid_date) & disease_event_date<covid_date, as.numeric(disease_event_date-cohort_start)+1,
                                             ifelse(!is.na(covid_date) & disease_event_date>covid_date, as.numeric(covid_date-cohort_start), 
                                                    ifelse(!is.na(covid_date) & disease_event_date==covid_date, as.numeric(covid_date-cohort_start),'to check')))))))
  
  futime<-as.numeric(futime)
  futime<-ifelse(futime>max_follow_up_time, max_follow_up_time, futime)
  
  return(futime)
  
}

average_daily_incidence<-function(dataset, disease_event_date, covid_date,futime,max_follow_up_time){
  
  #Daily incidence formula in the unexposed period: #number of disease-of-interest events occurred during the day in the unexposed period/#number of disease-free (at risk) individuals at the start of the day in the unexposed period
  
  #Note:as we taking 'day' as the unit, the daily incidence calculated here is both daily incidence proportion and daily incidence rate
  
  
  #First, define disease status for each individual in the unexposed period: code 1 for the disease of interest occurred in the unexposed period and 0 otherwise
  disease_status<-ifelse(is.na(disease_event_date),0,
                         ifelse(!is.na(disease_event_date) & is.na(covid_date),1,
                                ifelse(!is.na(disease_event_date) & disease_event_date>=covid_date,0,1))) 
  
  dataset$disease_status<-as.integer(disease_status)
  
  #Next, use the survfit() function in the survival package and the follow-up time calculated using to_calculate_futime() to summarise the number of daily disease event (nominator of the daily incidence formula) and the number of daily at risk individuals (denominator of the daily incidence formula) in the unexposed period
  fit<-survfit(Surv(futime,dataset$disease_status) ~1) 
  
  #Put the date, numbers of daily disease-event (nominator) and numbers of individuals at risk (denominator) into a new dataframe 
  daily_incidence<-data.frame(time=fit$time,
                              n_event=fit$n.event,
                              n_risk=fit$n.risk)
  
  #calculate daily incidence for disease of interest in the unexposed period
  daily_incidence$incidence_proportion<-daily_incidence$n_event/daily_incidence$n_risk
  
  #In the last step, average the daily incidence across the cohort follow-up period to get an average daily incidence for disease of interest 
  average_daily_incidence<-sum(daily_incidence$incidence_proportion)/max_follow_up_time 
  
  #two other useful summary level information to put in the output of this function
  event_count_unexposed<-sum(dataset$disease_status==1) #disease of interest counts in the unexposed period
  exposed_individual<-sum(!is.na(dataset$exp_date_covid19_confirmed))  #number of individuals (exposed) with a COVID-19 diagnosis during cohort follow-up
  
  return_list<-c(event_count_unexposed=event_count_unexposed, 
                 average_daily_incidence=average_daily_incidence,
                 number_of_exposed_individuals=exposed_individual)
  
  return(return_list)
}

incidence_rate<-function(disease_event_date,covid_date,futime){
  
  #calculates the nominator
  total_event<-sum(!is.na(disease_event_date)) 
  event_exposed<-sum(disease_event_date>=covid_date,na.rm = T)
  event_unexposed<-total_event-event_exposed
  
  #calculates the denominator
  follow_up_time<-sum(futime)
  
  #calculates the incidence rate
  incidence_rate<-event_unexposed/follow_up_time
  
  return(incidence_rate)
}


AER_calculation<-function(dataframe_name,event_name,subgroup_name,q,HR,HR_LB,HR_UB,covid_infected_population,total_num_rows_for_life_table,weeks_since_covid){
  event<-rep(event_name,total_num_rows_for_life_table) 
  subgroup<-rep(subgroup_name,total_num_rows_for_life_table)
  x_days_since_covid<-c(1:total_num_rows_for_life_table)
  weeks_since_covid<-weeks_since_covid
  q_estimated_average_incidence_rate<-rep(q, total_num_rows_for_life_table)
  life_table<-data.frame(event,subgroup,x_days_since_covid,weeks_since_covid,q_estimated_average_incidence_rate,HR,HR_LB,HR_UB)
  life_table$one_minus_q<-1-life_table$q_estimated_average_incidence_rate
  life_table$S<-cumprod(life_table$one_minus_q)
  life_table$qh<-life_table$q_estimated_average_incidence_rate*life_table$HR
  life_table$qh_LB<-life_table$q_estimated_average_incidence_rate*life_table$HR_LB
  life_table$qh_UB<-life_table$q_estimated_average_incidence_rate*life_table$HR_UB
  life_table$one_minus_qh<-1- life_table$qh
  life_table$SC<-cumprod(life_table$one_minus_qh)
  life_table$one_minus_qh_LB<-1- life_table$qh_LB
  life_table$SC_LB<-cumprod(life_table$one_minus_qh_LB)
  life_table$one_minus_qh_UB<-1- life_table$qh_UB
  life_table$SC_UB<-cumprod(life_table$one_minus_qh_UB)
  life_table$AER<-life_table$S- life_table$SC
  life_table$AER_percent<-life_table$AER*100
  life_table$AER_LB<-life_table$S- life_table$SC_LB
  life_table$AER_percent_LB<-life_table$AER_LB*100
  life_table$AER_UB<-life_table$S- life_table$SC_UB
  life_table$AER_percent_UB<-life_table$AER_UB*100
  assign(x = dataframe_name, value = life_table, envir = globalenv())
  excess_mean<-covid_infected_population*life_table[total_num_rows_for_life_table,20]
  excess_LB<-covid_infected_population*life_table[total_num_rows_for_life_table,22]
  excess_UB<-covid_infected_population*life_table[total_num_rows_for_life_table,24]
  return_list<-list(excess_events_mean=excess_mean, excess_events_LB=excess_LB, excess_events_UB=excess_UB)
  return(return_list) #this function returns changes in AER, total number of excess disease events attributable to COVID-19 infection and the 95% confidence intervals for excess events
}

plot_graph<-function(num_subgroups, life_table1,lifetable2,lifetable3,lifetable4,lifetable5, graph_title,graph_name,levels,legend_title,color_value,linetype){
  library(ggplot2)
  #library(ggthemes) optional package. If working in the English DAE (Data Access Environment), it does not have ggthemes package installed --> admin required to install this package 
  if(num_subgroups==0){#if there is no subgroup, i.e. plotting for all and not split by sex for example, put num_subgroups=0 in the function argument.
    
    life_table1$x_days_since_covid_converted<-life_table1$x_days_since_covid/7
    
    p_line<-ggplot(life_table1,
                   aes(x=x_days_since_covid_converted,
                       y=AER_percent,
                       lty = 'All')) +
      geom_line(size=1.5,colour=color_value)+
      # scale_x_continuous(breaks = c(0,10,20,30,40,50),limits = c(0,50))+
      scale_x_continuous(breaks = seq(0,72,4),limits = c(0,72))+
      
      scale_color_manual(values = color_value)+
      labs(x='Weeks since COVID-19 diagnosis',y='Difference in cumulative absolute risk (%)',
           title = graph_title)+
      theme(plot.title = element_text(hjust = 0.5))+
      #theme_hc()+ this background requires ggthemes package
      scale_linetype('')
    
    assign(x = graph_name, value = p_line, envir = globalenv())
    
    return(p_line)
  }
  
  if(num_subgroups==2){
    combined<-rbind(life_table1,lifetable2)
  }
  
  if(num_subgroups==3){
    combined<-rbind(life_table1,lifetable2,lifetable3)
  }
  
  if(num_subgroups==4){
    combined<-rbind(life_table1,lifetable2,lifetable3, lifetable4)
  }
  if(num_subgroups==5){
    combined<-rbind(life_table1,lifetable2,lifetable3,lifetable4, lifetable5)
  }  
  
  combined$x_days_since_covid_converted<-combined$x_days_since_covid/7
  combined$subgroup<-factor(combined$subgroup,levels = levels)
  
  p_line<-ggplot(combined,
                 aes(x=x_days_since_covid_converted,
                     y=AER_percent,
                     colour=subgroup)) +
    geom_line(aes(linetype=subgroup, colour=subgroup), size=1)+
    scale_linetype_manual(values = linetype)+
    scale_color_manual(values = color_value)+
    # scale_x_continuous(breaks = c(0,10,20,30,40,50),limits = c(0,50))+
    scale_x_continuous(breaks = seq(0,72,4),limits = c(0,72))+
    
    labs(x='Weeks since COVID-19 diagnosis',y='Difference in cumulative absolute risk (%)',
         title = graph_title)+
    theme(plot.title = element_text(hjust = 0.5))+
    #theme_hc()+ this background requires ggthemes package
    theme(legend.key.size = unit(1.2,'cm'), legend.key = element_blank())+
    labs(color=legend_title,linetype=legend_title)
  
  assign(x = graph_name, value = p_line, envir = globalenv())
  
  return(p_line)
}

save_plot<-function(file_location,data,filename,dpi, width,height){
  setwd(file_location)
  ggsave(plot=data, filename = paste0(filename, ".png"), dpi=dpi,width = width,height = height)
  
}

# ==============================================================================================
setwd("/db-mnt/databricks/rstudio_collab/CCU002_07/infection/riskratio_wen")
dataset = readRDS('cox_infec_ecds_050725_rmhistVaxInf.rds')
start_date = as.IDate("2020-01-01")
end_date = as.IDate("2021-05-31")

dataset = data.table(dataset)
dataset[,'baseline_date':=as.IDate(baseline_date)]
dataset[,'exp_date_covid19_confirmed':=as.IDate(exp_date_covid19_confirmed)]
dataset[,'death_date':=as.IDate(death_date)]
dataset[,'fu_end_date2':=as.IDate(fu_end_date2)]

dataset <- dataset[cov_cat_4age != "0-4"]

outname = list(ate='Arterial thrombotic events', vte='Venous thrombotic events',thrombocytopenia='Thrombocytopenia',carditis='Myocarditis/Pericarditis',inflammatory='Inflammatory conditions')

fu = as.numeric(dataset$fu_end_date2- dataset$baseline_date+1)

for (i in c('ate','vte','carditis','thrombocytopenia','inflammatory'))
{futime =to_calculate_futime(dataset$baseline_date,dataset$exp_date_covid19_confirmed,dataset$death_date,dataset[,get(paste0('out_',i,'_date'))],fu)

avg = average_daily_incidence(dataset,dataset[,get(paste0('out_',i,'_date'))],dataset$exp_date_covid19_confirmed,futime,max(fu))
rate = incidence_rate(dataset[,get(paste0('out_',i,'_date'))],dataset$exp_date_covid19_confirmed,futime)

# weeks_since_covid<-c(rep('1d',1), rep('2d-1w',6),rep('2-4w',21),rep('5-26w',154), rep('27w-365d',183)
#                      ,rep(paste0('1d-',max(fu),'d'),max(fu)-366+1))

weeks_since_covid<-c(rep('1d',1), rep('2d-4w',27),rep('5-26w',154)
                     ,rep(paste0('27w+-',max(fu),'d'),max(fu)-183+1))


hr_table = fread(paste0('~/collab/CCU002_07/infection/cox_wen/05_07_2025_1wave_4time/compiled_results/suppressed_compiled_HR_results_out_',i,'_date_covid_TO_RELEASE.csv'))
hr_table = hr_table[time_points=='normal'&model=='mdl_max_adj_region'&substr(term,1,4)=='days',.(hr,conf_low,conf_high)]
times =c(1,27,154,max(fu)-183+1)
HR = c()
HR_LB = c()
HR_UB = c()
for (j in 1:4)
{HRj = rep(hr_table$hr[j],times[j])
HR = c(HR,HRj)
HRlj = rep(hr_table$conf_low[j],times[j])
HR_LB = c(HR_LB,HRlj)
HRuj = rep(hr_table$conf_high[j],times[j])
HR_UB = c(HR_UB,HRuj)
}

AER_calculation(dataframe_name = paste0('life_table_',i), 
                event_name = outname[[i]],
                subgroup_name = 'All',
                weeks_since_covid = weeks_since_covid,
                q=avg['average_daily_incidence'],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = avg['number_of_exposed_individuals'], 
                total_num_rows_for_life_table = max(fu)) 
p_line = plot_graph(num_subgroups = 0,
                    life_table1 = get(paste0('life_table_',i)), 
                    graph_title = outname[[i]],
                    graph_name = i,
                    color_value = 'black')
# save_plot('/db-mnt/databricks/rstudio_collab/CCU002_07/infection',p_line,paste0(i,'_inf_period1_20250728'),700,10,10)
}

# Saving a data frame with readr's write_csv
write_csv(life_table_ate, "life_table_ate_inf_period1_20250729.csv")
write_csv(life_table_carditis, "life_table_carditis_inf_period1_20250729.csv")
write_csv(life_table_inflammatory, "life_table_inflammatory_inf_period1_20250729.csv")
write_csv(life_table_vte, "life_table_vte_inf_period_120250729.csv")
write_csv(life_table_thrombocytopenia, "life_table_thrombocytopenia_inf_period1_20250729.csv")

