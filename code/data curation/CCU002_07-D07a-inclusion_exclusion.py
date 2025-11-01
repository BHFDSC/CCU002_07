# Databricks notebook source
# MAGIC %md # CCU002_07-D07a-inclusion_exclusion
# MAGIC  
# MAGIC **Description** This notebook creates the inclusion exclusion table for the infection.
# MAGIC  
# MAGIC **Authors** Alexia Sampri
# MAGIC  
# MAGIC **Reviewers** Genevieve Cezard
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC Patients will be excluded if they meet ANY the following criteria:
# MAGIC
# MAGIC 1.Exclusion of patients aged >18 at baseline (2020-01-01)
# MAGIC 2.Exclusion of patients not in GDPPR
# MAGIC 3.Exclusion of patients who died before baseline (2020-01-01)
# MAGIC 4.Exclusion of patients with an sgss data before baseline (2020-01-01)
# MAGIC 5.Exclusion of patients with region missing or outside England
# MAGIC 6.Exclusion of patients without LSOA
# MAGIC 7.Exclusion of patients who failed the quality assurance

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

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_skinny""")
spark.sql(f"""REFRESH TABLE {dbc}.{proj}_quality_assurance""")
spark.sql(f"""REFRESH TABLE {path_cur_deaths_sing}""")
spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_inf_covariates_lsoa""")

skinny  = spark.table(f'{dbc}.{proj}_skinny')
deaths  = spark.table(path_cur_deaths_sing)
qa      = spark.table(f'{dbc}.{proj}_quality_assurance')
cov_lsoa   = spark.table(f'{dbc}.{proj}_out_inf_covariates_lsoa')
sgss    = extract_batch_from_archive(parameters_df_datasets, 'sgss')

# COMMAND ----------

display(skinny)

# COMMAND ----------

display(deaths)

# COMMAND ----------

display(qa)

# COMMAND ----------

display(sgss)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('skinny')
print('---------------------------------------------------------------------------------')
# reduce
_skinny = skinny.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'in_gdppr')

# check
count_var(_skinny, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('deaths')
print('---------------------------------------------------------------------------------')
# reduce
_deaths = deaths.select('PERSON_ID', f.col('REG_DATE_OF_DEATH').alias('DOD'))

# check
count_var(_deaths, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('quality assurance')
print('---------------------------------------------------------------------------------')
_qa = qa

# check
count_var(_qa, 'PERSON_ID'); print()
tmpt = tab(_qa, '_rule_total'); print()


print('---------------------------------------------------------------------------------')
print('sgss')
print('---------------------------------------------------------------------------------')
# reduce
_sgss = (
  sgss
  .select(f.col('PERSON_ID_DEID').alias('PERSON_ID'), 'Lab_Report_date')
  .where(f.col('PERSON_ID').isNotNull())
  .where(f.col('Lab_Report_date').isNotNull())
  .groupBy('PERSON_ID')
  .agg(f.min('Lab_Report_date').alias('Lab_Report_date_min')))

# check
count_var(_sgss, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('merged')
print('---------------------------------------------------------------------------------')
# merge skinny and deaths
_merged = (
  merge(_skinny, _deaths, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'])
  .withColumn('in_deaths', f.when(f.col('_merge') == 'both', 1).otherwise(0))
  .drop('_merge')); print()

# merge in qa
_merged = (
  merge(_merged, _qa, ['PERSON_ID'], validate='1:1', assert_results=['both'])
  .withColumn('in_qa', f.when(f.col('_merge') == 'both', 1).otherwise(0))
  .drop('_merge')); print()

# merge in sgss
# add baseline_date
_merged = (
  merge(_merged, _sgss, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'])
  .withColumn('in_sgss', f.when(f.col('_merge') == 'both', 1).otherwise(0))
  .drop('_merge')
  .withColumn('baseline_date', f.when(f.col('DOB') > '2020-01-01', f.col('DOB')).otherwise(f.lit('2020-01-01')))); print()  

# check
count_var(_merged, 'PERSON_ID'); print()

# merge in lsoa

_merged = (
  merge(_merged, cov_lsoa, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'])
  .withColumn('cov_lsoa', f.when(f.col('_merge') == 'both', 1).otherwise(0))
  .drop('_merge')); print()

# temp save
outName = f'{proj}_tmp_inclusion_exclusion_merged_inf'.lower()
_merged.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
_merged = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

# check
tmpt = tab(_merged, 'in_gdppr'); print()
tmpt = tab(_merged, 'in_deaths'); print()
tmpt = tab(_merged, 'in_qa'); print()
tmpt = tab(_merged, '_rule_total'); print()
tmpt = tab(_merged, 'in_sgss'); print()

# COMMAND ----------

# MAGIC %md # 3 Inclusion / exclusion

# COMMAND ----------

tmp0 = _merged
tmpc = count_var(tmp0, 'PERSON_ID', ret=1, df_desc='original', indx=0); print()

# COMMAND ----------

# MAGIC %md ## 3.1 Exclude patients aged > 18 at baseline

# COMMAND ----------


tmp1 = tmp0.where(f.col('DOB') > '2002-01-01')

# check
tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='post exclusion of patients aged > 18 at baseline', indx=1); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.2 Exclude patients not in GDPPR

# COMMAND ----------

# filter out patients not in GDPPR
tmp2 = tmp1.where(f.col('in_gdppr') == 1)

# check
tmpt = count_var(tmp2, 'PERSON_ID', ret=1, df_desc='post exclusion of patients not in GDPPR', indx=2); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.3 Exclude patients who died before baseline

# COMMAND ----------

# filter out patients who died before baseline
tmp2 = (
  tmp2
  .withColumn('flag_DOD_lt_baseline', f.when(f.col('DOD') < f.col('baseline_date'), 1).otherwise(0))
  .withColumn('flag_baseline_gt_20200101', f.when(f.col('baseline_date') > f.to_date(f.lit('2020-01-01')), 1).otherwise(0)))

# check
tmpt = tab(tmp2, 'flag_DOD_lt_baseline', 'flag_baseline_gt_20200101', var2_unstyled=1); print()

# filter out and tidy
tmp3 = (
  tmp2
  .where(f.col('flag_DOD_lt_baseline') == 0)
  .drop('flag_DOD_lt_baseline', 'flag_baseline_gt_20200101'))

# check
tmpt = count_var(tmp3, 'PERSON_ID', ret=1, df_desc='post exlusion of patients who died before baseline', indx=3); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.4 Exclude patients who have a positive COVID-19 test before baseline

# COMMAND ----------

# filter out patients who had a positive COVID-19 test before baseline
tmp3 = (
  tmp3
  .withColumn('flag_sgss_lt_baseline', f.when(f.col('Lab_Report_date_min') < f.col('baseline_date'), 1).otherwise(0))
  .withColumn('flag_baseline_gt_20200101', f.when(f.col('baseline_date') > f.to_date(f.lit('2020-01-01')), 1).otherwise(0)))

# check
tmpt = tab(tmp3, 'flag_sgss_lt_baseline', 'flag_baseline_gt_20200101', var2_unstyled=1); print()

# filter out and tidy
tmp4 = (
  tmp3
  .where(f.col('flag_sgss_lt_baseline') == 0)
  .drop('flag_sgss_lt_baseline', 'flag_baseline_gt_20200101'))

# check
tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='post exclusion of patients with an sgss date before baseline', indx=4); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.5 Exclude patients with region missing or outside England

# COMMAND ----------

tmp3.groupBy("region").count().show()

# COMMAND ----------

# Has a record of region in England

# filter out patients without a region in England
tmp5 = tmp4.where(~f.col('region').isin(['Scotland','Wales', 'None']))

# check
tmpt = count_var(tmp5, 'PERSON_ID', ret=1, df_desc='post exclusion of patients with region outside England', indx=5); print()

tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.6 Exclude patients without deprivation

# COMMAND ----------

# Has a record of deprivation

# filter out patients without deprivation
tmp6 = tmp5.where(~f.col('IMD_2019_DECILES').isNull())

# check
tmpt = count_var(tmp6, 'PERSON_ID', ret=1, df_desc='post exclusion of patients without deprivation', indx=6); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.7 Exclude patients who failed the quality assurance

# COMMAND ----------

# filter out patients who failed the quality assurance
tmp6 = (
  tmp6
  .withColumn('flag_rule_total_gt_0', f.when(f.col('_rule_total') > 0, 1).otherwise(0))
  .withColumn('flag_baseline_gt_20200101', f.when(f.col('baseline_date') > f.to_date(f.lit('2020-01-01')), 1).otherwise(0)))

# check
tmpt = tab(tmp6, '_rule_total', 'flag_rule_total_gt_0', var2_unstyled=1); print()
tmpt = tab(tmp6, 'flag_rule_total_gt_0', 'flag_baseline_gt_20200101', var2_unstyled=1); print()

# filter out and tidy
tmp7 = (
  tmp6
  .where(f.col('flag_rule_total_gt_0') == 0)
  .drop('flag_rule_total_gt_0', 'flag_baseline_gt_20200101'))

# check
tmpt = count_var(tmp7, 'PERSON_ID', ret=1, df_desc='post exclusion of patients who failed the quality assurance', indx=7); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md # 4 Flow diagram

# COMMAND ----------

# check flow table
tmpp = (
  tmpc
  .orderBy('indx')
  .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
  .withColumnRenamed('df_desc', 'stage')
  .toPandas())
tmpp['n'] = tmpp['n'].astype(int)
tmpp['n_id'] = tmpp['n_id'].astype(int)
tmpp['n_id_distinct'] = tmpp['n_id_distinct'].astype(int)
tmpp['n_diff'] = (tmpp['n'] - tmpp['n'].shift(1)).fillna(0).astype(int)
tmpp['n_id_diff'] = (tmpp['n_id'] - tmpp['n_id'].shift(1)).fillna(0).astype(int)
tmpp['n_id_distinct_diff'] = (tmpp['n_id_distinct'] - tmpp['n_id_distinct'].shift(1)).fillna(0).astype(int)
for v in [col for col in tmpp.columns if re.search("^n.*", col)]:
  tmpp.loc[:, v] = tmpp[v].map('{:,.0f}'.format)
for v in [col for col in tmpp.columns if re.search(".*_diff$", col)]:  
  tmpp.loc[tmpp['stage'] == 'original', v] = ''
# tmpp = tmpp.drop('indx', axis=1)
print(tmpp.to_string()); print()

# COMMAND ----------

# suppress cols in tmp1 by creating a new dataframe tmpp2 - to be able to export
tmpp2 = tmpc
cols = ['n', 'n_id', 'n_id_distinct']
for i, var in enumerate(cols):
  tmpp2 = tmpp2.withColumn(var, f.col(var).cast(t.IntegerType()))
  typ = dict(tmpp2.dtypes)[var]
  print(i, var, typ)  
  assert str(typ) in('bigint')
  assert tmpp2.where(f.col(var)<0).count() == 0
  tmpp2 = (tmpp2
         .withColumn(var,
                     f.when(f.col(var) == 0, 0)
                     .when(f.col(var) < 10, 10)
                     .when(f.col(var) >= 10, 5*f.round(f.col(var)/5))
                    )
        )

# COMMAND ----------

display(tmpp2)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# MAGIC %md ## 5.1 Cohort

# COMMAND ----------

tmpf = tmp7.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'DOD', 'LSOA', 'lsoa_name'
                   ,'region', 'IMD_2019_DECILES', 'baseline_date')

# check
count_var(tmpf, 'PERSON_ID')

# COMMAND ----------

# check 
display(tmpf)

# COMMAND ----------

# save name
outName = f'{proj}_out_inc_exc_inf_cohort'.lower()

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
tmpf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 5.2 Flow

# COMMAND ----------

# save name
outName = f'{proj}_out_inf_flow_inc_exc'.lower()

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
tmpc.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
