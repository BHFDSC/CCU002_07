# Databricks notebook source
# MAGIC %md # CCU002_07-D09b-covariates
# MAGIC  
# MAGIC **Description** This notebook creates the covariates. Covariates will be defined from the latest records before the study start date (with the exception of LSOA) for each individual as follows:
# MAGIC * LSOA: used to derive region and deprivation;
# MAGIC * Consultation rate: number of primary care contacts in the year prior to start date (GDPPR);
# MAGIC * Medications: total number of medications by BNF chapters prescribed within three months prior to the start date (primary_care_meds);
# MAGIC * Prior history of outcomes;
# MAGIC * Prior history of comorbidities;
# MAGIC  
# MAGIC **Authors** Alexia Sampri
# MAGIC
# MAGIC **Reviewers** Wen Shi
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run "/Repos/as3293@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU002_07-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

codelist_out = spark.table(path_out_codelist_outcome)
codelist_com = spark.table(path_out_codelist_comorbidity)

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_vacc_cohort""")
cohort       = spark.table(f'{dbc}.{proj}_out_vacc_cohort')

gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc_long = spark.table(path_cur_hes_apc_long)
pmeds        = extract_batch_from_archive(parameters_df_datasets, 'pmeds')

# COMMAND ----------

display(codelist_out)

# COMMAND ----------

tmpt = tab(codelist_out, 'Disease_group'); print()
tmpt = tab(codelist_out, 'Disease_group', 'name', var2_wide=0); print()
tmpt = tab(codelist_out, 'name', 'terminology', var2_wide=0); print()
tmpt = tab(codelist_out, 'name', 'code_type', var2_wide=0); print()

# COMMAND ----------

display(codelist_com)

# COMMAND ----------

tmpt = tab(codelist_com, 'Disease_group'); print()
tmpt = tab(codelist_com, 'Disease_group', 'name', var2_wide=0); print()
tmpt = tab(codelist_com, 'name', 'terminology', var2_wide=0); print()
tmpt = tab(codelist_com, 'name', 'code_type', var2_wide=0); print()

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# consider covariates for individuals born after 20210806- for which we have no history - take first month for conditions at birth?

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
individual_censor_dates = (
  cohort
  .select('PERSON_ID', 'DOB')
  # .where(f.col('DOB') < f.to_date(f.lit('2021-08-06')))  
  # .withColumn('CENSOR_DATE_END', f.to_date(f.lit('2021-08-06')))
  .withColumn('CENSOR_DATE_END', f.when(f.col('DOB') >= f.to_date(f.lit('2021-08-06')), f.col('DOB')).otherwise(f.to_date(f.lit('2021-08-06'))))
  .withColumnRenamed('DOB', 'CENSOR_DATE_START')
)

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()


# COMMAND ----------

display(individual_censor_dates)

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
_hes_apc = hes_apc_long.select(['PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE'])

# check 1
count_var(_hes_apc, 'PERSON_ID'); print()

# merge in individual censor dates
_hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()

# check 2
count_var(_hes_apc, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
_hes_apc = _hes_apc\
  .withColumn('_tmp1',\
    f.when((f.col('DATE').isNull()), 1)\
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)\
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)\
  )
tmpt = tab(_hes_apc, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
_hes_apc = _hes_apc\
  .where(f.col('_tmp1').isin([2]))\
  .drop('_tmp1')

# check 3
count_var(_hes_apc, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
_hes_apc = _hes_apc\
  .withColumn('_tmp2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
tmpt = tab(_hes_apc, '_tmp2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
_hes_apc = _hes_apc\
  .where(f.col('_tmp2').isin([1]))\
  .drop('_tmp2')

# check 4
count_var(_hes_apc, 'PERSON_ID'); print()
print(_hes_apc.limit(10).toPandas().to_string()); print()


# print('--------------------------------------------------------------------------------------')
# print('pmeds')
# print('--------------------------------------------------------------------------------------')
# reduce and rename columns
_pmeds = pmeds\
  .select(['Person_ID_DEID', 'ProcessingPeriodDate', 'PrescribedBNFCode'])\
  .withColumnRenamed('Person_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('ProcessingPeriodDate', 'DATE')\
  .withColumnRenamed('PrescribedBNFCode', 'CODE')

# check
count_var(_pmeds, 'PERSON_ID'); print()

# add censor dates
_pmeds = _pmeds\
  .join(individual_censor_dates, on='PERSON_ID', how='inner')

# check
count_var(_pmeds, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE - none
# ...
_pmeds = _pmeds\
  .withColumn('_tmp1',\
    f.when((f.col('DATE').isNull()), 1)\
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)\
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)\
  )
tmpt = tab(_pmeds, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# ...
_pmeds = _pmeds\
  .where(f.col('_tmp1').isin([2]))\
  .drop('_tmp1')

# check
count_var(_pmeds, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_END_less_180d
# ...
_pmeds = _pmeds\
  .withColumn('CENSOR_DATE_END_less_90d', f.date_add(f.col('CENSOR_DATE_END'), -90))\
  .withColumn('_tmp2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_END_less_90d')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_END_less_90d')), 2)\
  )
tmpt = tab(_pmeds, '_tmp2'); print()

# filter to on or after CENSOR_DATE_END_less_12m
# ...
_pmeds = _pmeds\
  .where(f.col('_tmp2').isin([1]))\
  .drop('_tmp2')

# check
count_var(_pmeds, 'PERSON_ID'); print()
print(_pmeds.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# temp save
_hes_apc = temp_save(df=_hes_apc, out_name=f'{proj}_tmp_covariates_hes_apc')
_pmeds = temp_save(df=_pmeds, out_name=f'{proj}_tmp_covariates_pmeds')

# COMMAND ----------

# MAGIC %md # 3 LSOA

# COMMAND ----------

# LSOA has already been added in cohort

# COMMAND ----------

# MAGIC %md 
# MAGIC # 4 Consultation rate

# COMMAND ----------

# Consultation rate: number of primary care contacts in the year prior to start date; GP;

# restricted to individuals with DOB before 2021-08-06
#   since for individuals with a DOB after this date, the baseline date is their DOB, 
#   and we would not expect to find any consultations before when they were born 


# gdppr - reduce and filter
# note: critically, cohort includes only those "in_gdppr", so no records in GDPPR => 0 consultations (although may be a small number who move out of and into practices not included in GDPPR)
_gdppr = (gdppr
          .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'RECORD_DATE')
          .where(f.col('PERSON_ID').isNotNull())
          .where(f.col('RECORD_DATE').isNotNull())
          .where((f.col('RECORD_DATE') > f.to_date(f.lit('2020-08-06'))) & (f.col('RECORD_DATE') <= f.to_date(f.lit('2021-08-06'))))
          .distinct())

# check
count_var(_gdppr, 'PERSON_ID'); print()

# merge
_n_consultations = (
  merge(_gdppr, individual_censor_dates.select('PERSON_ID'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0)
  .groupBy('PERSON_ID')\
  .agg(f.count(f.lit(1)).alias('cov_n_consultations'))); print()

# merge
# replace nulls with zeros when merging into cohort (i.e., for those with no records in GDPPR in previous year)
_n_consultations = (
  merge(_n_consultations, individual_censor_dates.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0)
  .na.fill(value=0, subset=['cov_n_consultations'])); print()

# check
count_var(_n_consultations, 'PERSON_ID'); print()
print(_n_consultations.limit(10).toPandas().to_string()); print()
tmpt = tab(_n_consultations, 'cov_n_consultations'); print()
tmpt = tabstat(_n_consultations, 'cov_n_consultations'); print()

# temp save
_n_consultations = temp_save(df=_n_consultations, out_name=f'{proj}_tmp_covariates_n_consultations'); print()

# COMMAND ----------

# MAGIC %md 
# MAGIC # 5 Medication history (Number of unique BNF chapters)

# COMMAND ----------

# check
count_var(_pmeds, 'PERSON_ID'); print()

# create
_unique_bnf_chapters = _pmeds\
  .withColumn('_bnf_chapter', f.substring(f.col('CODE'), 1, 2))\
  .select('PERSON_ID', '_bnf_chapter')\
  .groupBy('PERSON_ID')\
  .agg(f.countDistinct(f.col('_bnf_chapter')).alias('cov_n_unique_bnf_chapters'))

# merge
# replace nulls with zeros when merging into cohort (i.e., for those with no records in pmeds in last 3 months)
_unique_bnf_chapters = (
  merge(_unique_bnf_chapters, individual_censor_dates.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0)
  .na.fill(value=0, subset=['cov_n_unique_bnf_chapters'])); print()

# check
count_var(_unique_bnf_chapters, 'PERSON_ID'); print()
print(_unique_bnf_chapters.limit(10).toPandas().to_string()); print()
tmpt = tab(_unique_bnf_chapters, 'cov_n_unique_bnf_chapters'); print()
tmpt = tabstat(_unique_bnf_chapters, 'cov_n_unique_bnf_chapters'); print()

# temp save
_unique_bnf_chapters = temp_save(df=_unique_bnf_chapters, out_name=f'{proj}_tmp_covariates_unique_bnf_chapters'); print()

# COMMAND ----------

# MAGIC %md # 6 HX Outcomes

# COMMAND ----------

# MAGIC %md ## 6.1 Codelist

# COMMAND ----------

codelist_hx_out_hes_apc = codelist_out

# COMMAND ----------

# MAGIC %md ## 6.2 Create

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
_hx_out_in = {
  'hes_apc': ['_hes_apc', 'codelist_hx_out_hes_apc',  1]
}

# run codelist match and codelist match summary functions
_hx_out, _hx_out_1st, _hx_out_1st_wide = codelist_match(_hx_out_in, _name_prefix=f'cov_hx_out_'); print()
_hx_out_summ_name, _hx_out_summ_name_code = codelist_match_summ(_hx_out_in, _hx_out); print()

# temp save
# _hx_out_1st_wide = temp_save(df=_hx_out_1st_wide, out_name=f'{proj}_tmp_covariates_hx_out_1st_wide'); print()

# COMMAND ----------

# MAGIC %md ## 6.3 Check

# COMMAND ----------

count_var(_hx_out_1st_wide, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md ### 6.3.0 Display

# COMMAND ----------

# check result
display(_hx_out_1st_wide)

# COMMAND ----------

# plots prepare
_tmp = (
  merge(_hx_out_1st, cohort.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
  .withColumn('fu', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_END'))/365.25)
  .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
_tmpp = _tmp.toPandas()

# COMMAND ----------

# plot function
def plot_hx_1st(df, var, bin_min, bin_max, sharey, stacked, xlabel, byvar='source', row_height=2.4, out_com='out'):
  plt.rcParams.update({'font.size': 8})
  rows_of_5 = np.ceil(len(df['name'].drop_duplicates())/5).astype(int)
  fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,row_height*rows_of_5), sharex=True, sharey=sharey) # , sharey=True , dpi=100) # 
  colors = sns.color_palette("tab10", 2)
  vlist = list(df[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
  for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
    print(i, ax, v)
    tmp2d1 = df[(df[f'name'] == v)] # (_tmpp[f'{var}'] > -20) &     
    if(byvar!='source'):
      names = ['Male', 'Female']  
      s1 = list(tmp2d1[tmp2d1[f'{byvar}'] == '1'][f'{var}'])
      s2 = list(tmp2d1[tmp2d1[f'{byvar}'] == '2'][f'{var}'])
      ax.hist(s1, bins = list(np.linspace(0,20,100)), color=colors[0], label=names[0], alpha=0.5) # normed=True 
      ax.hist(s2, bins = list(np.linspace(0,20,100)), color=colors[1], label=names[1], alpha=0.5) # normed=True 
    else:
      names = ['hes_apc', 'deaths']  
      s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'{var}'])
      # s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'{var}'])
      if((bin_min == 0) & (bin_max == 0)): bins = 100
      else: bins = list(np.linspace(bin_min, bin_max, 100))
      ax.hist([s1], bins = bins, stacked=stacked, color=colors[0], label=names[0]) # normed=True
    ax.set_title(f'{v}')
    ax.set(xlabel=f'{xlabel}')
    ax.xaxis.set_tick_params(labelbottom=True)    
    if(var == 'DATE'): ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)    
    if(i==0): ax.legend(loc='upper left')
    
    if(out_com=='out'):
      axes[2,4].set_axis_off()
      for i in range(0,2):
        for j in range(0, 5):
          axes[i,j].xaxis.set_tick_params(labelbottom=True)
    elif(out_com=='com'):      
      axes[7,3].set_axis_off()
      axes[7,4].set_axis_off()
      for i in range(0,7):
        for j in range(0, 5):
          axes[i,j].xaxis.set_tick_params(labelbottom=True)          
  plt.tight_layout();
  return fig

# COMMAND ----------

# MAGIC %md ###  6.3.1 Plots - First event - Over follow-up time (years) by data source (stacked)

# COMMAND ----------

fig1 = plot_hx_1st(df=_tmpp, var='fu', bin_min=-20, bin_max=0, sharey=False, stacked=True, xlabel='\nFollow-up (years)\n')
display(fig1)

# COMMAND ----------

fig2 = plot_hx_1st(df=_tmpp, var='fu', bin_min=-20, bin_max=0, sharey=True, stacked=True, xlabel='\nFollow-up (years)\n')
display(fig2)

# COMMAND ----------

# MAGIC %md ### 6.3.2 Plots - First event - Over calendar time by data source (stacked)

# COMMAND ----------

fig3 = plot_hx_1st(df=_tmpp, var='DATE', row_height=2.8, bin_min=0, bin_max=0, sharey=False, stacked=True, xlabel='\nDate\n')
display(fig3)

# COMMAND ----------

fig4 = plot_hx_1st(df=_tmpp, var='DATE', row_height=2.8, bin_min=0, bin_max=0, sharey=True, stacked=True, xlabel='\nDate\n')
display(fig4)

# COMMAND ----------

# MAGIC %md ### 6.3.3 Plots - First event - Over age at event (years) by data source (stacked)

# COMMAND ----------

fig5 = plot_hx_1st(df=_tmpp, var='age', bin_min=0, bin_max=20, sharey=False, stacked=True, xlabel='\nAge (years)\n')
display(fig5)

# COMMAND ----------

fig6 = plot_hx_1st(df=_tmpp, var='age', bin_min=0, bin_max=20, sharey=True, stacked=True, xlabel='\nAge (years)\n')
display(fig6)

# COMMAND ----------

# MAGIC %md ### 6.3.4 Plots - First event - Over age at event (years) by sex (overlapping)

# COMMAND ----------

fig7 = plot_hx_1st(df=_tmpp, var='age', byvar='SEX', bin_min=0, bin_max=20, sharey=False, stacked=True, xlabel='\nAge (years)\n')
display(fig7)

# COMMAND ----------

fig8 = plot_hx_1st(df=_tmpp, var='age', byvar='SEX', bin_min=0, bin_max=20, sharey=True, stacked=True, xlabel='\nAge (years)\n')
display(fig8)

# COMMAND ----------

# MAGIC %md ### 6.3.5 Numerical summaries of plots

# COMMAND ----------

# check numerical summaries 
_tmps = (_tmp
        .withColumn('name_source', f.concat_ws('_', 'name', 'source'))
        .withColumn('name_sex', f.concat_ws('_', 'name', 'SEX'))); print()
tmpt = tabstat(_tmps, 'fu', byvar='name_source'); print()
tmpt = tabstat(_tmps, 'DATE', byvar='name_source', date=1); print()
tmpt = tabstat(_tmps, 'age',  byvar='name_source'); print()
tmpt = tabstat(_tmps, 'age',  byvar='name_sex'); print()

# COMMAND ----------

# MAGIC %md ### 6.3.6 Codelist match summaries

# COMMAND ----------

# check codelist match summary by name and source
display(_hx_out_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(_hx_out_summ_name_code)

# COMMAND ----------

# MAGIC %md # 7 HX Comorbidities

# COMMAND ----------

# MAGIC %md ## 7.1 Codelist

# COMMAND ----------

codelist_hx_com_hes_apc = codelist_com

# COMMAND ----------

# MAGIC %md ## 7.2 Create

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
_hx_com_in = {
  'hes_apc': ['_hes_apc', 'codelist_hx_com_hes_apc',  1]
}

# run codelist match and codelist match summary functions
_hx_com, _hx_com_1st, _hx_com_1st_wide = codelist_match(_hx_com_in, _name_prefix=f'cov_hx_com_'); print()
_hx_com_summ_name, _hx_com_summ_name_code = codelist_match_summ(_hx_com_in, _hx_com); print()

# temp save
_hx_com_1st_wide = temp_save(df=_hx_com_1st_wide, out_name=f'{proj}_tmp_covariates_hx_com_1st_wide'); print()

# COMMAND ----------

# MAGIC %md ### 7.2.1 Apply age restriction for specific Self-harm ICD10 codes

# COMMAND ----------

# Apply age restriction on _hx_com_all, before recreating _hx_com_1st and _hx_com_1st_wide

# COMMAND ----------

_hx_com_all = _hx_com['all']
display(_hx_com_all)

# COMMAND ----------

# ICD10 Codes used with an age restriction (age at death/admission must be 10 years or older): Y10-Y34, Y87.2

# Merge with DOB from cohort and create age at event
_hx_com_all_age_restrict = (
  merge(_hx_com_all, cohort.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
  .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25)
  .withColumn('flag_age_restriction', f.when( (f.col('code').rlike(r'^Y[12]\d') | (f.col('code').isin(["Y30","Y31","Y32","Y33","Y34","Y872"]))) & (f.col('age') < 10) , 1).otherwise(0)))
  
tmpt = tab(_hx_com_all_age_restrict, 'flag_age_restriction'); print()

# COMMAND ----------

display(codelist_com)

# COMMAND ----------

tmp = codelist_com\
  .withColumn('tmp', f.when((f.col('code').rlike(r'^Y[12]\d')) | (f.col('code').rlike(r'^Y[3][01234]')) | (f.col('code').rlike(r'^Y872')), 1).otherwise(0))
tmpt = tab(tmp.where(f.col('tmp') == 1), 'code', 'tmp')

# COMMAND ----------

_win = Window\
  .partitionBy(['PERSON_ID', 'name'])\
  .orderBy('DATE', 'sourcen', 'code')    
  
# _hx_com_1st
_hx_com_1st_age_restrict = _hx_com_all_age_restrict\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'DATE', 'name', 'source', 'code')\
  .orderBy('PERSON_ID', 'DATE', 'name')

# _hx_com_1st_wide
print(f'reshape long to wide')
  
  # join codelist names before reshape to ensure all covariates are created (when no code matches are found)
codelist_hx_com_hes_apc_tmp = codelist_hx_com_hes_apc\
  .select('name')\
  .distinct() 

# reshape long to wide  
_name_prefix = f'cov_hx_com_'
_hx_com_1st_wide_age_restrict = _hx_com_1st_age_restrict\
  .drop('code')\
  .join(codelist_hx_com_hes_apc_tmp, on='name', how='outer')\
  .withColumn('name', f.concat(f.lit(f'{_name_prefix}'), f.lower(f.col('name'))))\
  .groupBy('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')\
  .pivot('name')\
  .agg(f.first('DATE'))\
  .where(f.col('PERSON_ID').isNotNull())\
  .orderBy('PERSON_ID')  

# add flag and date columns
print(f'add flag and date')
vlist = []
for i, v in enumerate([col for col in list(_hx_com_1st_wide_age_restrict.columns) if re.match(f'^{_name_prefix}', col)]):
  print(' ' , i, v)
  _hx_com_1st_wide_age_restrict = _hx_com_1st_wide_age_restrict\
    .withColumnRenamed(v, v + '_date')\
    .withColumn(v + '_flag', f.when(f.col(v + '_date').isNotNull(), 1))
  vlist = vlist + [v + '_flag', v + '_date']
  
# reorder vars
_hx_com_1st_wide_age_restrict = _hx_com_1st_wide_age_restrict\
  .select(['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'] + vlist)   

# COMMAND ----------

display(_hx_com_1st)

# COMMAND ----------

# MAGIC %md ## 7.3 Check

# COMMAND ----------

count_var(_hx_com_1st_wide, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md ### 7.3.0 Display

# COMMAND ----------

# check result
display(_hx_com_1st_wide)

# COMMAND ----------

# plots prepare
_tmp_com = (
  merge(_hx_com_1st, cohort.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
  .withColumn('fu', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_END'))/365.25)
  .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
_tmpp_com = _tmp_com.toPandas()

# COMMAND ----------

# MAGIC %md ###  7.3.1 Plots - First event - Over follow-up time (years) by data source (stacked)

# COMMAND ----------

fig1c = plot_hx_1st(df=_tmpp_com, var='fu', bin_min=-20, bin_max=0, sharey=False, stacked=True, xlabel='\nFollow-up (years)\n', out_com='com')
display(fig1c)

# COMMAND ----------

fig2c = plot_hx_1st(df=_tmpp_com, var='fu', bin_min=-20, bin_max=0, sharey=True, stacked=True, xlabel='\nFollow-up (years)\n', out_com='com')
display(fig2c)

# COMMAND ----------

# MAGIC %md ### 7.3.2 Plots - First event - Over calendar time by data source (stacked)

# COMMAND ----------

fig3c = plot_hx_1st(df=_tmpp_com, var='DATE', row_height=2.8, bin_min=0, bin_max=0, sharey=False, stacked=True, xlabel='\nDate\n', out_com='com')
display(fig3c)

# COMMAND ----------

# MAGIC %md ### 7.3.3 Plots - First event - Over age at event (years) by data source (stacked)

# COMMAND ----------

fig5c = plot_hx_1st(df=_tmpp_com, var='age', bin_min=0, bin_max=20, sharey=False, stacked=True, xlabel='\nAge (years)\n', out_com='com')
display(fig5c)

# COMMAND ----------

# MAGIC %md ### 7.3.4 Plots - First event - Over age at event (years) by sex (overlapping)

# COMMAND ----------

fig7c = plot_hx_1st(df=_tmpp_com, var='age', byvar='SEX', bin_min=0, bin_max=20, sharey=False, stacked=True, xlabel='\nAge (years)\n', out_com='com')
display(fig7c)

# COMMAND ----------

# MAGIC %md ### 7.3.5 Numerical summaries of plots

# COMMAND ----------

# check numerical summaries 
_tmps_com = (_tmp_com
        .withColumn('name_source', f.concat_ws('_', 'name', 'source'))
        .withColumn('name_sex', f.concat_ws('_', 'name', 'SEX'))); print()
tmpt = tabstat(_tmps_com, 'fu', byvar='name_source'); print()
tmpt = tabstat(_tmps_com, 'DATE', byvar='name_source', date=1); print()
tmpt = tabstat(_tmps_com, 'age',  byvar='name_source'); print()
tmpt = tabstat(_tmps_com, 'age',  byvar='name_sex'); print()

# COMMAND ----------

# MAGIC %md ### 7.3.6 Codelist match summaries

# COMMAND ----------

# check codelist match summary by name
display(_hx_com_summ_name)

# COMMAND ----------

# check codelist match summary by name and code
display(_hx_com_summ_name_code)

# COMMAND ----------

# MAGIC %md # F Save

# COMMAND ----------

# merge _n_consultations, and _unique_bnf_chapters
tmp1 = merge(_n_consultations, _unique_bnf_chapters, ['PERSON_ID'], validate='1:1', indicator=0); print()

# merge _hx_out_1st_wide and _hx_com_1st_wide
tmp2 = merge(_hx_out_1st_wide, _hx_com_1st_wide, ['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'], validate='1:1', indicator=0); print()

# merge tmp1 and tmp2 
tmp3 = merge(tmp1, tmp2, ['PERSON_ID'], validate='1:1', indicator=0); print()

# merge tmp3 and cohort ID
tmp3 = merge(tmp3, cohort.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# check
count_var(tmp3, 'PERSON_ID'); print()
print(len(tmp3.columns)); print()
print(pd.DataFrame({f'_cols': tmp3.columns}).to_string()); print()

# COMMAND ----------

# check
count_var(tmp3, 'PERSON_ID'); print()
print(len(tmp3.columns)); print()
print(pd.DataFrame({f'_cols': tmp3.columns}).to_string()); print()

# COMMAND ----------

# check final
display(tmp3)

# COMMAND ----------

display(tmp3)

# COMMAND ----------

# save name
outName = f'{proj}_out_vacc_covariates'.lower()

# save
tmp3.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# save codelist match summary tables
list_tables = []
list_tables = list_tables + ['_hx_out_summ_name', '_hx_out_summ_name_code']
list_tables = list_tables + ['_hx_com_summ_name', '_hx_com_summ_name_code']

for i, table in enumerate(list_tables):
  print(i, table)
  outName = f'{proj}_out_vacc_codelist_match_cov{table}'.lower()
  tmp1 = globals()[table]
  tmp1.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
  #spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
  print(f'  saved {dbc}.{outName}')
