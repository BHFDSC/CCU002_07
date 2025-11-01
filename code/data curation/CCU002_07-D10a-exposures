# Databricks notebook source
# MAGIC %md # CCU002_07-D10a-exposures
# MAGIC  
# MAGIC **Description** This notebook creates the exposure: Covid-19 infection.
# MAGIC  
# MAGIC **Authors** Alexia Sampri, Tom Bolton
# MAGIC
# MAGIC **Reviewers** Genevieve Cezard
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Exposures for analyses on COVID-19 infection:**
# MAGIC <Time in weeks since any COVID-19 exposure:
# MAGIC +ve PCR test Pillar 1 and/or Pillar 2 COVID-19 infection laboratory testing data 
# MAGIC Primary care COVID-19 diagnosis; or
# MAGIC Hospital admission using HES APC & SUS and the ICD-10 code (U07.1).
# MAGIC
# MAGIC Time in weeks since any COVID exposure with hospitalisation:
# MAGIC Hospital admission with COVID-19 in primary position, and
# MAGIC Hospital admission within the first 28 days of COVID-19.
# MAGIC
# MAGIC Time in weeks since any COVID without hospitalisation 
# MAGIC
# MAGIC COVID and no hospitalisation within 28 days  -->

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

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

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_inf_cohort')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_cur_covid')
covid = spark.table(f'{dbc}.{proj}_cur_covid')

# COMMAND ----------

display(cohort)

# COMMAND ----------

display(covid)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# check
count_var(cohort, 'PERSON_ID'); print()
count_var(covid, 'PERSON_ID'); print()

# restrict to cohort

_covid = merge(covid, cohort.select('PERSON_ID', 'baseline_date'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()

# COMMAND ----------

# check
count_var(_covid, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 3 Infection

# COMMAND ----------

# MAGIC %md ## 3.1 Check

# COMMAND ----------

# check infection
count_var(_covid, 'PERSON_ID'); print()
tmpt = tabstat(_covid, 'DATE', date=1); print()
tmp1 = _covid.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
tmpt = tab(_covid, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(_covid, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Prepare

# COMMAND ----------

print('------------------------------------------------------------------------------')
print('confirmed COVID-19 (as defined for CCU002_01)')
print('------------------------------------------------------------------------------')
covid_confirmed = _covid\
  .where(\
    (f.col('covid_phenotype').isin([
      '01_Covid_positive_test'
      , '01_GP_covid_diagnosis'
      , '02_Covid_admission_any_position'
      , '02_Covid_admission_primary_position'
    ]))\
    & (f.col('source').isin(['sgss', 'gdppr', 'hes_apc', 'sus']))\
    & (f.col('covid_status').isin(['confirmed', '']))\
  )

# check
count_var(covid_confirmed, 'PERSON_ID'); print()
print(covid_confirmed.limit(10).toPandas().to_string(max_colwidth=50)); print()
tmpt = tabstat(covid_confirmed, 'DATE', date=1); print()
tmp1 = covid_confirmed.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
tmpt = tab(covid_confirmed, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(covid_confirmed, 'covid_phenotype', 'source', var2_unstyled=1); print()


print('------------------------------------------------------------------------------')
print('confirmed COVID-19 admission primary position (i.e., specific hospitalisation for COVID-19; as defined for CCU002_01)')
print('------------------------------------------------------------------------------')
covid_confirmed_adm_pri = covid_confirmed\
  .where(f.col('covid_phenotype') == '02_Covid_admission_primary_position')\
  .orderBy('PERSON_ID', 'DATE', 'source')

# check
count_var(covid_confirmed_adm_pri, 'PERSON_ID'); print()
print(covid_confirmed_adm_pri.limit(10).toPandas().to_string(max_colwidth=50)); print()
tmpt = tabstat(covid_confirmed_adm_pri, 'DATE', date=1); print()
tmpt = tab(covid_confirmed_adm_pri, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(covid_confirmed_adm_pri, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Create

# COMMAND ----------

print('------------------------------------------------------------------------------')
print('first (earliest) confirmed covid infection')
print('------------------------------------------------------------------------------')
# window for row number
_win_rownum = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('date', 'covid_phenotype', 'source')


# filter to first (earliest) confirmed covid infection
# note: ignore ties in covid_phenotype for now
covid_confirmed_1st = covid_confirmed\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .where(f.col('_rownum') == 1)\
  .withColumnRenamed('DATE', 'exp_covid_1st_date')\
  .orderBy('PERSON_ID')

# check
count_var(covid_confirmed_1st, 'PERSON_ID'); print()
tmpt = tabstat(covid_confirmed_1st, 'exp_covid_1st_date', date=1); print()
tmpt = tab(covid_confirmed_1st, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(covid_confirmed_1st, 'covid_phenotype', 'source', var2_unstyled=1); print()

# reduce
covid_confirmed_1st = covid_confirmed_1st\
  .select('PERSON_ID', 'exp_covid_1st_date', f.col('covid_phenotype').alias('exp_covid_1st_phenotype'))


print('------------------------------------------------------------------------------')
print('severity of the first (earliest) confirmed covid infection (hospitalised within 28 days)')
print('------------------------------------------------------------------------------')
# inner join first (earliest) confirmed covid infection table to the hospitalisations table  
# filter to hospitalisations on or after the date of first (earliest) confirmed covid infection
# filter to first (earliest) hospitalisation
# calculate the number of days from the date of first (earliest) confirmed covid infection to first (earliest) hospitalisation
# flag where this was within 28 days
covid_confirmed_severity = covid_confirmed_adm_pri\
  .join(covid_confirmed_1st.select('PERSON_ID', 'exp_covid_1st_date'), on='PERSON_ID', how='inner')\
  .where(f.col('DATE') >= f.col('exp_covid_1st_date'))\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .where(f.col('_rownum') == 1)\
  .withColumnRenamed('DATE', 'exp_covid_adm_date')\
  .withColumn('exp_covid_adm_days', f.datediff(f.col('exp_covid_adm_date'), f.col('exp_covid_1st_date')))\
  .withColumn('exp_covid_adm_days_le_28',\
    f.when(f.col('exp_covid_adm_days') <= 28, 1)\
    .when(f.col('exp_covid_adm_days') > 28, 0)\
  )\
  .select('PERSON_ID', 'exp_covid_adm_date', 'exp_covid_adm_days', 'exp_covid_adm_days_le_28')\
  .orderBy('PERSON_ID')

# check
count_var(covid_confirmed_severity, 'PERSON_ID'); print()
tmpt = tabstat(covid_confirmed_severity, 'exp_covid_adm_date', date=1); print()
tmpt = tab(covid_confirmed_severity, 'exp_covid_adm_days_le_28'); print()
tmpt = tabstat(covid_confirmed_severity, var='exp_covid_adm_days', byvar='exp_covid_adm_days_le_28'); print()
tmpt = tab(covid_confirmed_severity, 'exp_covid_adm_days', 'exp_covid_adm_days_le_28', var2_unstyled=1); print()


print('------------------------------------------------------------------------------')
print('merge')
print('------------------------------------------------------------------------------')
covid_confirmed_1st = merge(covid_confirmed_1st, covid_confirmed_severity, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()
  
# check
count_var(covid_confirmed_1st, 'PERSON_ID'); print()
print(covid_confirmed_1st.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 3.4 Check

# COMMAND ----------

# check 
display(covid_confirmed_1st)

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# merge 
tmp1 = merge(covid_confirmed_1st, cohort.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# check
count_var(tmp1, 'PERSON_ID'); print()

# COMMAND ----------

# check final
display(tmp1)

# COMMAND ----------

# save name
outName = f'{proj}_out_inf_exposures'.lower()

# save
tmp1.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
