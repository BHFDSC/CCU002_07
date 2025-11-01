# Databricks notebook source
# MAGIC %md
# MAGIC # CCU002_07-D08a-cohort
# MAGIC  
# MAGIC **Description** This notebook creates the cohort table for infection.
# MAGIC  
# MAGIC **Authors** Alexia Sampri, Tom Bolton
# MAGIC  
# MAGIC **Reviewers** Genevieve Cezard
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**

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

# MAGIC %run "/Repos/as3293@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU002_07-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# cohort = spark.table(path_cohort)
spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_inc_exc_inf_cohort""")
cohort = spark.table(f'{dbc}.{proj}_out_inc_exc_inf_cohort')

spark.sql(f"""REFRESH TABLE {path_cur_vacc_first}""")
vacc   = spark.table(path_cur_vacc_first)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# check
tmpt = tab(vacc, 'PERSON_ID_rownum'); print()
assert vacc.count() == vacc.where(f.col('PERSON_ID_rownum') == 1).count()

# reduce
_vacc = vacc.select('PERSON_ID','PRODUCT', f.col('DATE').alias('vacc_1st_date'))
print(_vacc.limit(10).toPandas().to_string()); print()


# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------


# create dates
# flag those with terminating events before baseline
tmp1 = (merge(cohort, _vacc, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0)
          .withColumn('baseline_date', f.to_date(f.col('baseline_date')))
          .withColumn('covid_testing_end_date', f.to_date(f.lit('2022-03-31')))
          .withColumn('data_end_date', f.to_date(f.lit('2022-08-31')))
          .withColumn('age_18_date', f.add_months(f.col('DOB'), 12*18))        
          .withColumn('_flag_DOD_lt_baseline', f.when(f.col('DOD') < f.col('baseline_date'), 1).otherwise(0))
          .withColumn('_flag_covid_testing_end_date_lt_baseline', f.when(f.col('covid_testing_end_date') < f.col('baseline_date'), 1).otherwise(0))
          .withColumn('_flag_data_end_date_lt_baseline', f.when(f.col('data_end_date') < f.col('baseline_date'), 1).otherwise(0))
          .withColumn('_flag_vacc_1st_date_lt_baseline', f.when(f.col('vacc_1st_date') < f.col('baseline_date'), 1).otherwise(0))
         ); print()

# check
tmpt = tab(tmp1, '_flag_DOD_lt_baseline'); print()
tmpt = tab(tmp1, '_flag_covid_testing_end_date_lt_baseline'); print()
tmpt = tab(tmp1, '_flag_data_end_date_lt_baseline'); print()
tmpt = tab(tmp1, '_flag_vacc_1st_date_lt_baseline'); print()


# # filter out those with terminating events before baseline
# tmp2 = (tmp1
#         .where(~(
#           (f.col('_flag_DOD') == 1) 
#           | (f.col('_flag_covid_testing_end_date') == 1) 
#           | (f.col('_flag_data_end_date') == 1) 
#           | (f.col('_flag_vacc_1st_date') == 1)
#         ))
#         .drop(*[var for var in tmp1.columns if re.match('_flag.*', var)])
#        )

# # check
# count_var(tmp2, 'PERSON_ID'); print()

tmp2 = tmp1.drop(*[var for var in tmp1.columns if re.match('_flag.*', var)])

tmp3 = (tmp2
        .withColumn('baseline_age', f.round(f.datediff(f.col('baseline_date'), f.col('DOB'))/365.25, 2))
        .withColumn('fu_end_date', f.least('data_end_date', 'age_18_date', 'DOD', 'covid_testing_end_date'))
        .withColumn('_fu_end_date_source', 
                      f.when(f.col('fu_end_date') == f.col('DOD'), 'DOD')
                      #.when(f.col('fu_end_date') == f.col('age_18_date'), 'age_18_date')
                      .when(f.col('fu_end_date') == f.col('data_end_date'), 'data_end_date')
                      .when(f.col('fu_end_date') == f.col('covid_testing_end_date'), 'covid_testing_end_date'))    
        .withColumn('fu_end_date', f.greatest('baseline_date', 'fu_end_date'))
        .withColumn('fu_end_age', f.round(f.datediff(f.col('fu_end_date'), f.col('DOB'))/365.25, 2))
        .withColumn('fu_days', f.datediff(f.col('fu_end_date'), f.col('baseline_date')))
        .withColumn('vacc_1st_Pfizer', f.when(f.col('PRODUCT').isin(['Pfizer',      
          'Pfizer child']), 1).otherwise(0))
        .withColumn('vacc_1st_noPfizer', f.when(~f.col('PRODUCT').isin(['Pfizer', 'Pfizer child', 'null']), 1).otherwise(0))
        )

# check

tmpt = tab(tmp3, 'vacc_1st_Pfizer'); print()
tmpt = tab(tmp3, 'vacc_1st_noPfizer'); print()

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

display(tmp3)

# COMMAND ----------

count_var(tmp3, 'PERSON_ID'); print()
tmpt = tab(tmp3, 'SEX'); print()
tmpt = tab(tmp3, 'ETHNIC_CAT'); print()
tmpt = tabstat(tmp3, 'baseline_age'); print()
tmpt = tabstat(tmp3, 'DOB', date=1); print()
tmp = tab(tmp3, '_fu_end_date_source'); print()
tmpt = tabstat(tmp3, 'fu_end_age'); print()
tmpt = tabstat(tmp3, 'fu_days'); print()
tmpt = tab(tmp3, 'PRODUCT'); print()

# COMMAND ----------

# DBTITLE 1,Age at baseline
tmpp = (tmp3
        .withColumn('baseline_age', f.round(f.col('baseline_age')*12)/12)
        .groupBy('baseline_age')
        .agg(f.count(f.lit(1)).alias('n'))
        .toPandas()
       )

plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['baseline_age'],tmpp['n'], width = 1/12, edgecolor = "black")
axes.set(xticks=np.arange(0, 19, step=1))
axes.set_xlim(0,18)
axes.set(xlabel="Age at baseline (years)")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
display(fig)

# COMMAND ----------

# DBTITLE 1,Age at baseline (excluding 0 years)
tmpp = (tmp3
        .where(f.col('baseline_age') > 0)
        .withColumn('baseline_age', f.round(f.col('baseline_age')*12)/12)
        .groupBy('baseline_age')
        .agg(f.count(f.lit(1)).alias('n'))
        .toPandas()
       )

plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['baseline_age'],tmpp['n'], width = 1/12, edgecolor = "black")
axes.set(xticks=np.arange(0, 19, step=1))
axes.set_xlim(0,18)
axes.set(xlabel="Age at baseline (years)")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
display(fig)

# COMMAND ----------

# DBTITLE 1,Date of birth
tmpp = (tmp3
        .groupBy('DOB')
        .agg(f.count(f.lit(1)).alias('n'))
        .toPandas()
       )
tmpp['date_formatted'] = pd.to_datetime(tmpp['DOB'], errors='coerce')

# plot
plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['date_formatted'], tmpp['n'], width = 28, edgecolor = "black")
# axes.set(xticks=np.arange(0, 19, step=1))
axes.set_xlim(datetime.datetime(2002, 1, 1), datetime.datetime(2022, 11, 1))
axes.set(xlabel="Date of birth")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
display(fig)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_inf_cohort'.lower()

# save
tmp3.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
