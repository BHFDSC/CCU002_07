library(data.table)

#df_cohort;

# data = data.table(readRDS('cox_inf.rds'))
data = data.table(readRDS('/db-mnt/databricks/rstudio_collab/CCU002_07/infection/riskratio_wen/cox_infec_clean_ecds240118_rmAge18AfterMar22.rds'))
colnames(data)

# data[,length(unique(patient_id))]
# data[,unique(censor_date_end_out)]
# data[,unique(censor_date_start_out)]
# data[,.(min(censor_date_start_out,na.rm=TRUE),max(censor_date_start_out,na.rm=TRUE))]
# data[,unique(infection)]

# data[,length(unique(exp_date_covid19_confirmed))]
# expo = grep('^exp_[[:alnum:]_]+',colnames(data),value=TRUE)
# lapply(data[,..expo],class)
# data[is.na(exp_date_covid19_confirmed),.N]
# data[,length(unique(exp_covid_1st_phenotype))]
# data[,unique(exp_covid_1st_phenotype)]
# data[,summary(cov_num_age)]
# data[,.(min(censor_date_start_cov,na.rm=TRUE),max(censor_date_start_cov,na.rm=TRUE))]
# data[,.(min(censor_date_end_cov,na.rm=TRUE),max(censor_date_end_cov,na.rm=TRUE))]
# 
# flags = grep('^out_[[:alpha:]_]+_date$',colnames(data),value=TRUE)
# data[,lapply(.SD,class),.SDcols=flags]
# data[is.na(baseline_date),.N]
# data[,.(min(baseline_date),max(baseline_date))]
# data[!is.na(death_date),.N]
# data[death_date <baseline_date,.N]
# data[,unique(data_end_date)]
# data[data_end_date<baseline_date,.N]
# data[is.na(fu_end_date),.N]
# data[,max(fu_end_date)]
# data[fu_end_date>death_date,.N]
# data[fu_end_date<baseline_date,.N]

# data[pmin(death_date,data_end_date,na.rm=TRUE)!=fu_end_date,.N]


# unique(data[,'_fu_end_date_source'])
# data['covid_testing_end_date',unique(fu_end_date),on='_fu_end_date_source']
# data['covid_testing_end_date',max(fu_end_date),on='_fu_end_date_source']
# data['',length(unique(fu_end_date)),on='_fu_end_date_source']
# data['',max(unique(fu_end_date)),on='_fu_end_date_source']
# data['DOD',length(unique(fu_end_date)),on='_fu_end_date_source']
# data['DOD',max(unique(fu_end_date)),on='_fu_end_date_source']
# 
data[,infection:=0]
data[!is.na(exp_date_covid19_confirmed),infection:=1]

data[,data_end_date2 := as.IDate('2022-03-31')]
data[,fu_end_date2:=pmin(death_date,data_end_date2,na.rm=TRUE)]
# lapply(data[,.(baseline_date,fu_end_date2,exp_date_covid19_confirmed)],class)

# data[data_end_date2<baseline_date,.N]
# data[fu_end_date2<birth_date,.N]
# data[fu_end_date2<baseline_date,.N]

data = data[!baseline_date>as.IDate('2022-03-31')]

data[exp_date_covid19_confirmed<birth_date,.N]
# data3 = data[!(!is.na(exp_date_covid19_confirmed) & exp_date_covid19_confirmed<birth_date)]
# data3[exp_date_covid19_confirmed<baseline_date,.N]
# data3[exp_date_covid19_confirmed>death_date,.N]
# data3 = data3[!(!is.na(exp_date_covid19_confirmed) &!is.na(death_date) & exp_date_covid19_confirmed>death_date)]
# data3[exp_date_covid19_confirmed>fu_end_date2,.N]

# flags = grep('^out_[[:alpha:]_]+_date$',colnames(data3),value=TRUE)
# out = c('baseline_date','fu_end_date2',flags)
# outlier = data3[,lapply(.SD,function(x) return(ifelse(x-fu_end_date2>0|x-baseline_date<0,1,0))),.SDcols=out]
# length(which(outlier[,..flags]==1))
# outlier2 = unlist(lapply(transpose(outlier),function(x) ifelse(any(x==1,na.rm=TRUE),1,0)))
# outlier3 = data3[which(outlier2==1)]
# data4 = data3[!which(outlier2==1)]
# data4 = data3[!which(outlier2==1)]

# combine into composite outcomes (# thrombocytopenia , fracture already calculated in single outcome)
data[,c('ate','out_ate_date'):=.(0,as.IDate(''))]
data[!is.na(out_ami_date)|!is.na(out_stroke_date)|!is.na(out_other_arterial_embolism_date),c('ate','out_ate_date'):=.(1,pmin(out_ami_date, out_stroke_date,out_other_arterial_embolism_date,na.rm=TRUE ))]
data[,c('vte','out_vte_date'):=.(0,as.IDate(''))]
data[!is.na(out_pe_date)|!is.na(out_dvt_date)|!is.na(out_icvt_date)|!is.na(out_pvt_date)|!is.na(out_dvt_other_date),c('vte','out_vte_date'):=.(1,pmin(out_pe_date,out_dvt_date,out_dvt_other_date,out_pvt_date,out_icvt_date,na.rm=TRUE))]
data[,c('carditis','out_carditis_date'):=.(0,as.IDate(''))]
data[!is.na(out_myocarditis_date)|!is.na(out_pericarditis_date),c('carditis','out_carditis_date'):=.(1,pmin(out_myocarditis_date,out_pericarditis_date,na.rm=TRUE))]
data[,c('inflammatory','out_inflammatory_date'):=.(0,as.IDate(''))]
data[!is.na(out_kawasaki_date)|!is.na(out_sirs_date)|!is.na(out_pims_date)|!is.na(out_msis_date),c('inflammatory','out_inflammatory_date'):=.(1,pmin(out_kawasaki_date,out_sirs_date,out_pims_date,out_msis_date,na.rm=TRUE))]


flags = c('out_fracture_date','out_thrombocytopenia_date','out_ate_date','out_vte_date','out_carditis_date','out_inflammatory_date')

l = list()
for (i in flags)
{un_no1 = data[infection==0&!is.na(get(i)),.N]
inf_no_t = data[infection==1&!is.na(get(i)),c('exp_date_covid19_confirmed',..i)]
un_no2 = inf_no_t[get(i)<exp_date_covid19_confirmed,.N]
inf_no = inf_no_t[,.N] - un_no2
l[[i]] = data.table(uninf = un_no1+un_no2,inf = inf_no)}
t = rbindlist(l)
t = cbind(names(l),t)
colnames(t)[1] = 'disease_outcome'

lt = list()
for (i in flags)
{un_ft_t = data[infection==0,c('baseline_date','fu_end_date2',..i)]
inf_ft_t = data[infection==1&is.na(get(i)),c('baseline_date','exp_date_covid19_confirmed','fu_end_date2')]
inf_ft_t2 = data[infection==1&!is.na(get(i)),c('baseline_date','exp_date_covid19_confirmed',..i)]
un_ft1 = un_ft_t[!.(NA),sum(as.IDate(get(i))-as.IDate(baseline_date)),on=i] + un_ft_t[.(NA),sum(as.IDate(fu_end_date2) - as.IDate(baseline_date)),on=i]
un_ft2 = inf_ft_t[,sum(as.IDate(exp_date_covid19_confirmed) - as.IDate(baseline_date))]
un_ft3 = inf_ft_t2[get(i)<exp_date_covid19_confirmed,sum(as.IDate(get(i)) - as.IDate(baseline_date))]
un_ft4 = inf_ft_t2[get(i)>=exp_date_covid19_confirmed,sum(as.IDate(exp_date_covid19_confirmed) - as.IDate(baseline_date))]
inf_ft1 = inf_ft_t[,sum(as.IDate(fu_end_date2) - as.IDate(exp_date_covid19_confirmed))]
inf_ft2 = inf_ft_t2[get(i)>=exp_date_covid19_confirmed,sum(as.IDate(get(i)) - as.IDate(exp_date_covid19_confirmed))]
lt[[i]] = data.table(uninf = un_ft1 + un_ft2 + un_ft3 + un_ft4, inf = inf_ft1 + inf_ft2)}
inf_fu_t = cbind(names(lt),rbindlist(lt))
colnames(inf_fu_t)[1] = 'disease_outcome'

inf_fu_t[, c('u_years','i_years' ):= .(uninf/365,inf/365)]
inf_rate = cbind(t,inf_fu_t[,c(4,5)])
inf_rate[,c('u_rate','i_rate'):=.(uninf/(u_years/100000),inf/(i_years/100000))]
inf_rate[,'ratio':=i_rate/u_rate]
# inf_rate[,disease_outcome := sub('^out_','',disease_outcome)]

# fwrite(data,"/mnt/efs/dars_nic_391419_j3w9t_collab/CCU002_07/infection/riskratio_wen/cox_infec.csv")

# class(data$cov_cat_ethnicity)
# unique(data$cov_cat_ethnicity)
data[cov_cat_ethnicity=='South Asian',cov_cat_ethnicity:='South_Asian']
data[,cov_cat_ethnicity:=factor(cov_cat_ethnicity,levels=c("White","South_Asian","Black", "Missing", "Mixed","Other" ),ordered=FALSE)]
# class(data$cov_cat_sex)
# unique(data$cov_cat_sex)
data[,cov_cat_sex:=factor(cov_cat_sex,levels=c("Female","Male"))]
# class(data$cov_num_deprivation)
# unique(data$cov_num_deprivation)
data[,cov_cat_deprivation:=factor(cov_num_deprivation,levels=as.character(1:10),
                                  labels=rep(c("1_2_most_deprived","3_4","5_6","7_8","9_10_least_deprived"),each=2))]
#setwd("D:/PhotonUser/My Files/Home Folder/CCU002_07/infection/riskratio_inf")
#saveRDS(data,"cox_inf_clean.rds")
saveRDS(data,"cox_inf_ecds_clean.rds")

max_fu_days = data[,max(fu_end_date2 - baseline_date)]+1
# timeline = c(paste0(c(0,1,7,28,182,365),'d_',c(1,7,28,182,365,1096),'d'))
timeline = c(paste0('[',c(0,1,7,28,182,365),'d_',c(1,7,28,182,365,max_fu_days),'d',')'))

l2 = data.table()
for (i in flags)
{inf_no_t = data[infection==1&!is.na(get(i)),c('exp_date_covid19_confirmed',..i)][,out:= as.IDate(get(i)) - as.IDate(exp_date_covid19_confirmed)]

p1 = inf_no_t[out>=0 & out <1,.N]
p2 = inf_no_t[out>=1 & out <7,.N]

p3 = inf_no_t[out>=7 & out <28,.N]
p4 = inf_no_t[out>=28 & out <182,.N]
p5 = inf_no_t[out>=182 & out <365,.N]
# p5 = inf_no_t[out>=182 & out <365,.N]
p6 = inf_no_t[out>=365,.N]
l2 = rbind(l2,matrix(c(p1,p2,p3,p4
                       ,p5
                       ,p6),nrow=1))
}
inf_period_c = cbind(flags,l2)
colnames(inf_period_c) = c('disease_outcome',timeline)
d2 = inf_rate[inf_period_c,on='disease_outcome']

if(identical(apply(d2[,..timeline],1,sum),d2$inf))
{d2[,c('l_ci','h_ci') := .(ratio/exp(1.96*sqrt(1/uninf+1/inf)),ratio*exp(1.96*sqrt(1/uninf+1/inf)))]
  d2[,c('uninf','inf'):=.(round(uninf/5)*5,round(inf/5)*5)]
  for(i in c(paste0('[',c(0,1,7,28,182,365),'d_',c(1,7,28,182,365,max_fu_days),'d',')')))
    (d2[,c(i):=.(round(get(i)/5)*5)])
  for(i in c(paste0('[',c(0,1,7,28,182,365),'d_',c(1,7,28,182,365,max_fu_days),'d',')')))
    d2[get(i)<=5,c(i):='Redacted results']
  # setwd("~/collab/CCU002_07/infection/riskratio_inf")
  fwrite(d2,'inf_ratio.csv')} else return('Sum of period counts doesnot match')
