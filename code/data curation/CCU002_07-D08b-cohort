# Databricks notebook source
# MAGIC %md
# MAGIC # CCU002_07-D08b-cohort
# MAGIC  
# MAGIC **Description** This notebook creates the cohort table for vaccination.
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

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_inc_exc_vacc_cohort""")
cohort = spark.table(f'{dbc}.{proj}_out_inc_exc_vacc_cohort')

# COMMAND ----------

display(cohort)

# COMMAND ----------

# MAGIC %md # 2 Create

# COMMAND ----------

# create dates
# flag those with terminating events before baseline
tmp1 = (cohort
          .withColumn('baseline_date', f.to_date(f.col('baseline_date')))
          .withColumn('data_end_date', f.to_date(f.lit('2022-12-31')))
          .withColumn('age_18_date', f.add_months(f.col('DOB'), 12*18))        
          .withColumn('_flag_DOD_lt_baseline', f.when(f.col('DOD') < f.col('baseline_date'), 1).otherwise(0))
          .withColumn('_flag_data_end_date_lt_baseline', f.when(f.col('data_end_date') < f.col('baseline_date'), 1).otherwise(0))
          .withColumn('_flag_vacc_1st_date_lt_baseline', f.when(f.col('date_1') < f.col('baseline_date'), 1).otherwise(0))
          .withColumn('_flag_vacc_2nd_date_lt_baseline', f.when(f.col('date_2') < f.col('baseline_date'), 1).otherwise(0))
          .withColumn('_flag_vacc_2nd_date_lt_vacc_1st_date', f.when(f.col('date_2') < f.col('date_1'), 1).otherwise(0))
         ); print()

# check
tmpt = tab(tmp1, '_flag_DOD_lt_baseline'); print()
tmpt = tab(tmp1, '_flag_data_end_date_lt_baseline'); print()
tmpt = tab(tmp1, '_flag_vacc_1st_date_lt_baseline'); print()
tmpt = tab(tmp1, '_flag_vacc_2nd_date_lt_baseline'); print()
tmpt = tab(tmp1, '_flag_vacc_2nd_date_lt_vacc_1st_date'); print()

tmp2 = tmp1.drop(*[var for var in tmp1.columns if re.match('_flag.*', var)])

# create fu_end_date etc # 20230223 AS not including first vacc, 2nd vacc. AS will calculate and apply in R for greater flexibility.
# correct fu_end_date before baseline (before fu end age and fu end days are calculated)
tmp3 = (tmp2
        .withColumn('baseline_age', f.round(f.datediff(f.col('baseline_date'), f.col('DOB'))/365.25, 2))
        .withColumn('fu_end_date', f.least('data_end_date', 'DOD'))
        .withColumn('_fu_end_date_source', 
                      f.when(f.col('fu_end_date') == f.col('DOD'), 'DOD')
                      .when(f.col('fu_end_date') == f.col('data_end_date'), 'data_end_date'))    
        .withColumn('fu_end_date', f.greatest('baseline_date', 'fu_end_date'))
        .withColumn('fu_end_age', f.round(f.datediff(f.col('fu_end_date'), f.col('DOB'))/365.25, 2))
        .withColumn('fu_days', f.datediff(f.col('fu_end_date'), f.col('baseline_date'))))

# COMMAND ----------

# MAGIC %md # 3 Check

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

# MAGIC %md # 4 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_vacc_cohort'.lower()

# save
tmp3.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
