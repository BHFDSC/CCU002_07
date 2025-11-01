# Databricks notebook source
# MAGIC %md # CCU002_07-D6b-covariates_LSOA
# MAGIC  
# MAGIC **Description** This notebook creates the covariates for vaccination based on LSOA. LSOA will be used to derive LSOA, region and index of multiple deprivation;
# MAGIC  
# MAGIC **Authors** Alexia Sampri
# MAGIC
# MAGIC **Reviewers** Wen Shi
# MAGIC
# MAGIC **Acknowledgements** Based on previous work for CCU003_05, CCU018_01 (Tom Bolton, John Nolan) and the earlier CCU002 sub-projects.
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

# tmp1 = spark.table(f'gdppr_{db}_archive')
# tmpt = tab(tmp1, 'tmp_archived_on')

# COMMAND ----------

# tmp2 = spark.table(f'{dbc}.gdppr_{db}_archive')\
#   .where(f.col('archived_on') == '2021-08-18')
# tmpt = tab(tmp2, 'REPORTING_PERIOD_END_DATE')

# COMMAND ----------

# tmp2 = spark.table(f'{dbc}.gdppr_{db}_archive')\
#   .where(f.col('archived_on') == '2021-07-29')
# tmpt = tab(tmp2, 'REPORTING_PERIOD_END_DATE')

# COMMAND ----------

lsoa_region  = spark.table(path_cur_lsoa_region)
lsoa_imd     = spark.table(path_cur_lsoa_imd)
gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
gdppr_10th    = extract_batch_from_archive(parameters_df_datasets, 'gdppr_10th')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# tenth batch of GDPPR
tmp1 = (
  gdppr_10th
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'PRACTICE', f.col('REPORTING_PERIOD_END_DATE').alias('RPED'), 'LSOA')
  .where(f.col('PERSON_ID').isNotNull())
  .where(f.col('RPED').isNotNull())
  .where(f.col('LSOA').isNotNull())
  .dropDuplicates(['PERSON_ID', 'RPED', 'LSOA'])
  .withColumn('LSOA_batch', f.lit('First'))
)

# latest batch
tmp2 = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'PRACTICE', f.col('REPORTING_PERIOD_END_DATE').alias('RPED'), 'LSOA')
  .where(f.col('PERSON_ID').isNotNull())
  .where(f.col('RPED').isNotNull())
  .where(f.col('LSOA').isNotNull())
  .dropDuplicates(['PERSON_ID', 'RPED', 'LSOA'])
  .withColumn('LSOA_batch', f.lit('Latest'))
)

# union
tmp3 = (
  tmp1
  .union(tmp2)
  .dropDuplicates(['PERSON_ID', 'RPED', 'LSOA'])
)

# # filter to the cohort of interest
# # merge(tmp3, cohort.select('PERSON_ID'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
# tmp4 = (
#   tmp3
#   .join(cohort.select('PERSON_ID'), on=['PERSON_ID'], how='inner')
# )

# # temp save (checkpoint)
# tmp4 = temp_save(df=tmp4, out_name=f'{proj}_tmp_lsoa_tmp4'); print()

# temp save (checkpoint)
tmp3 = temp_save(df=tmp3, out_name=f'{proj}_tmp_lsoa_tmp3'); print()
# # check
# count_var(cohort, 'PERSON_ID'); print()
# count_var(tmp4, 'PERSON_ID'); print()

count_var(tmp3, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 3 Filter

# COMMAND ----------
win_denserank = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('RPED')      
win_rownum = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('RPED', 'LSOA')
win_rownummax = Window\
  .partitionBy('PERSON_ID')
tmp4 = (
  tmp3
  .withColumn('dense_rank', f.dense_rank().over(win_denserank))
  .where(f.col('dense_rank') == 1)
  .drop('dense_rank')
  .withColumn('rownum', f.row_number().over(win_rownum))
  .withColumn('rownummax', f.count('PERSON_ID').over(win_rownummax))
  .withColumn('LSOA_1', f.substring(f.col('LSOA'), 1, 1))
)

# check
count_var(tmp4, 'PERSON_ID'); print()
tmpt = tab(tmp4.where(f.col('rownum') == 1), 'rownummax'); print()
tmpt = tab(tmp4, 'LSOA_1'); print()

# COMMAND ----------

# MAGIC %md # 5 Add region

# COMMAND ----------

display(lsoa_region)

# COMMAND ----------

# prepare mapping
lsoa_region_1 = lsoa_region\
  .select('LSOA', 'lsoa_name', 'region')

# check
tmpt = tab(lsoa_region_1, 'region'); print()

# map
tmp5 = merge(tmp4, lsoa_region_1, ['LSOA'], validate='m:1', keep_results=['both', 'left_only']); print()
        
# check
count_var(tmp5, 'PERSON_ID'); print()
tmpt = tab(tmp5, 'LSOA_1', '_merge'); print()

# edit
tmp5 = (
  tmp5
  .withColumn('region',
              f.when(f.col('LSOA_1') == 'W', 'Wales')
              .when(f.col('LSOA_1') == 'S', 'Scotland')
              .otherwise(f.col('region'))
             )
  .drop('_merge')
)

# check
tmpt = tab(tmp5, 'region'); print()

# COMMAND ----------

# MAGIC %md # 6 Add IMD

# COMMAND ----------

# check
tmpt = tab(lsoa_imd, 'IMD_2019_DECILES', 'IMD_2019_QUINTILES', var2_unstyled=1); print()

# map
tmp6 = merge(tmp5, lsoa_imd, ['LSOA'], validate='m:1', keep_results=['both', 'left_only']); print()
  
# check
count_var(tmp6, 'PERSON_ID'); print()
tmpt = tab(tmp6, 'LSOA_1', '_merge', var2_unstyled=1); print()  

# tidy
tmp6 = (
  tmp6
  .drop('_merge')    
)

# temp save (checkpoint)
tmp6 = temp_save(df=tmp6, out_name=f'{proj}_tmp_vacc_lsoa_tmp7'); print()

# COMMAND ----------

# MAGIC %md # 7 Collapse

# COMMAND ----------

# collapse to 1 row per individual
win_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
win_lag = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('rownum')

# note - intentionally not using null safe equality for region_lag1_diff
tmp7 = (
  tmp6
  .orderBy('PERSON_ID', 'rownum')
  
  .withColumn('LSOA_conflict', f.when(f.col('rownummax') > 1, 1).otherwise(0))
  
  .withColumn('IMD_min', f.min(f.col('IMD_2019_DECILES')).over(win_egen))
  .withColumn('IMD_max', f.max(f.col('IMD_2019_DECILES')).over(win_egen))
  .withColumn('IMD_conflict', f.lit(1) - udf_null_safe_equality('IMD_min', 'IMD_max').cast(t.IntegerType()))

  .withColumn('region_lag1', f.lag(f.col('region'), 1).over(win_lag))
  .withColumn('region_lag1_diff', f.when(f.col('region') != f.col('region_lag1'), 1).otherwise(0)) 
  .withColumn('region_conflict', f.max(f.col('region_lag1_diff')).over(win_egen))
)

# check
tmpt = tab(tmp7, 'LSOA_conflict', 'IMD_conflict'); print()
tmpt = tab(tmp7, 'LSOA_conflict', 'region_conflict'); print()
tmpt = tab(tmp7, 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp7, 'rownummax', 'region_conflict'); print()
tmpt = tab(tmp7.where(f.col('rownum') == 1), 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp7.where(f.col('rownum') == 1), 'rownummax', 'region_conflict'); print()

# COMMAND ----------

# check
display(tmp7.where(f.col('rownummax') > 1))

# COMMAND ----------

# finalise
tmp8 = (
  tmp7          
  .withColumn('IMD_2019_DECILES', f.when((f.col('rownummax') > 1) & (f.col('IMD_conflict') == 1), f.lit(None)).otherwise(f.col('IMD_2019_DECILES')))       
  .withColumn('region', f.when((f.col('rownummax') > 1) & (f.col('region_conflict') == 1), f.lit(None)).otherwise(f.col('region')))       
  .select('PERSON_ID', 'LSOA_batch', 'LSOA_conflict', 'LSOA', 'lsoa_name', f.col('RPED').alias('LSOA_date'),  'region_conflict', 'region', 'IMD_conflict', 'IMD_2019_DECILES', 'rownum', 'rownummax')
  .where(f.col('rownum') == 1)
)
        
# check
tmpt = tab(tmp8, 'LSOA_batch'); print()
tmpt = tab(tmp8, 'rownummax', 'LSOA_conflict'); print()
tmpt = tab(tmp8, 'rownummax', 'region_conflict'); print()
tmpt = tab(tmp8, 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp8, 'region', 'region_conflict'); print()
tmpt = tab(tmp8, 'IMD_2019_DECILES', 'IMD_conflict'); print()
tmpt = tab(tmp8, 'region_conflict', 'IMD_conflict'); print()
tmpt = tab(tmp8.where(f.col('rownummax') > 1), 'region_conflict', 'IMD_conflict'); print()
tmpt = tabstat(tmp8, 'LSOA_date', date=1); print()

# tidy
tmp9 = (
  tmp8
  .drop('rownum', 'rownummax')
)

# COMMAND ----------

# check
display(tmp9)

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_vacc_covariates_lsoa'.lower()

# # save previous version for comparison purposes
# tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
#   .select('tableName')\
#   .where(f.col('tableName') == outName)\
#   .collect()
# if(len(tmpt)>0):
#   _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#   outName_pre = f'{outName}_pre{_datetimenow}'.lower()
#   print(outName_pre)
#   spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
#   spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
tmp9.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
