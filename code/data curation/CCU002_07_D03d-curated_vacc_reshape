# Databricks notebook source
# MAGIC %md # CCU002_07_D03d_curated_vacc_reshape
# MAGIC
# MAGIC **Description** Reshape the cleaned vaccination data for CCU002_07
# MAGIC
# MAGIC **Author(s)** Alexia Sampri, Tom Bolton
# MAGIC
# MAGIC **Reviewers** Wen Shi
# MAGIC

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

spark.sql(f'REFRESH TABLE {dbc}.{proj}_cur_vacc')
vacc = spark.table(f'{dbc}.{proj}_cur_vacc')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_cur_vacc_qa')
vacc_qa = spark.table(f'{dbc}.{proj}_cur_vacc_qa')

# COMMAND ----------

# MAGIC %md # 2 Check

# COMMAND ----------

display(vacc)

# COMMAND ----------

display(vacc_qa)

# COMMAND ----------

count_var(vacc, 'PERSON_ID'); print()
count_varlist(vacc, ['PERSON_ID', 'DATE'])
count_varlist(vacc, ['PERSON_ID', 'DATE', 'DATE_rownum'])

count_var(vacc_qa, 'PERSON_ID'); print()

# COMMAND ----------

tmpt = tab(vacc, 'dose_sequence'); print()

# COMMAND ----------

# MAGIC %md # 3 Prepare

# COMMAND ----------

# MAGIC %md ## 3.1 Set unknown procedure to Booster

# COMMAND ----------

# Unknown PROCEDURE may relate to dose 3
# <20 records with Unknown PROCEDURE
# set to booster

# check 
tmpt = tab(vacc, 'PROCEDURE'); print()
count_unknown = vacc.where(f.col('PROCEDURE') == 'Unknown').count()
assert (count_unknown > 0) & (count_unknown < 50) 

# exclude Unknown PROCEDURE
_vacc = (
  vacc
  .withColumn('PROCEDURE', f.when(f.col('PROCEDURE') == 'Unknown', 'B').otherwise(f.col('PROCEDURE')))
)

# check
tmpt = tab(_vacc, 'PROCEDURE'); print()

# COMMAND ----------

# MAGIC %md # 4 Exclusions

# COMMAND ----------

tmp0 = _vacc
tmpc = count_var(tmp0, 'PERSON_ID', ret=1, df_desc='original', indx=0); print()

# COMMAND ----------

# MAGIC %md ## 4.1 Exclude records before 20201208 (QA4)

# COMMAND ----------

# filter out records before 20201208
tmp0 = (
  tmp0
  .withColumn('qa_4', f.when(f.col('DATE') < f.to_date(f.lit('2020-12-08')), 1).otherwise(0))   
)

# check 
tmpt = tab(tmp0, 'qa_4'); print()
tmpt = tabstat(tmp0, 'DATE', byvar='qa_4', date=1); print()

# filter out and tidy
tmp1 = (
  tmp0
  .where(f.col('qa_4') == 0)
  .drop('qa_4')
  .drop('dose_sequence', 'PERSON_ID_rownum', 'PERSON_ID_rownummax')
)

# check
tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='post exclusion of records before 20201208', indx=1); print()
tmpc = tmpc.unionByName(tmpt)

# check
print(tmp1.orderBy('PERSON_ID', 'DATE', 'DATE_rownum').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 4.2 Exclude duplicate PERSON_ID and DATE (QA1)

# COMMAND ----------

# filter out duplicate PERSON_ID and DATE

_win_PERSON_ID_DATE_ord = Window\
  .partitionBy('PERSON_ID', 'DATE')\
  .orderBy(f.desc('PROCEDURE'), f.desc('TIME'), 'DATE_rownum')
_win_PERSON_ID_DATE = Window\
  .partitionBy('PERSON_ID', 'DATE')
_win_PERSON_ID_ord = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', f.desc('PROCEDURE'), f.desc('TIME'), 'DATE_rownum')
_win_PERSON_ID_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', f.desc('PROCEDURE'), f.desc('TIME'), 'DATE_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmp1 = (
  tmp1
  .withColumn('_PERSON_ID_DATE_rownum', f.row_number().over(_win_PERSON_ID_DATE_ord))
  .withColumn('_PERSON_ID_DATE_rownummax', f.count('PERSON_ID').over(_win_PERSON_ID_DATE))
  .withColumn('_PERSON_ID_rownum', f.row_number().over(_win_PERSON_ID_ord))
  .withColumn('_PERSON_ID_egen_rownummax', f.max(f.col('_PERSON_ID_DATE_rownummax')).over(_win_PERSON_ID_egen))
)

# check
tmpt = tab(tmp1.where(f.col('_PERSON_ID_DATE_rownum') == 1), '_PERSON_ID_DATE_rownummax'); print()
tmpt = tab(tmp1.where(f.col('_PERSON_ID_rownum') == 1), '_PERSON_ID_egen_rownummax'); print()
count_varlist(tmp1, ['PERSON_ID', 'DATE'])

# filter out and tidy
tmp2 = (
  tmp1
  .where(f.col('_PERSON_ID_DATE_rownum') == 1)
  .drop('_PERSON_ID_DATE_rownum', '_PERSON_ID_DATE_rownummax', '_PERSON_ID_rownum', '_PERSON_ID_egen_rownummax')
  .drop('DATE_rownum', 'TIME')
)

# check
count_varlist(tmp2, ['PERSON_ID', 'DATE'])

# check
tmpt = count_var(tmp2, 'PERSON_ID', ret=1, df_desc='post exclusion of duplicate PERSON_ID and DATE', indx=2); print()
tmpc = tmpc.unionByName(tmpt)

# check
print(tmp2.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 4.3 Exclude multiple dose 1 and 2 PROCEDURE (QA2)

# COMMAND ----------


# check
tmpt = tab(tmp2, 'PROCEDURE'); print()

# add row number for PERSON_ID and PROCEDURE
_win_PERSON_ID_PROCEDURE_ord = Window\
  .partitionBy('PERSON_ID', 'PROCEDURE')\
  .orderBy('DATE')
tmp2 = (
  tmp2
  .withColumn('PROCEDURE_rownum', f.row_number().over(_win_PERSON_ID_PROCEDURE_ord))
  .withColumn('PROCEDURE_rownum', f.when(f.col('PROCEDURE').isin('1', '2'), f.col('PROCEDURE_rownum')).otherwise(f.lit(1)))
)

# check
count_varlist(tmp2, ['PERSON_ID', 'PROCEDURE'])
tmpt = tab(tmp2, 'PROCEDURE_rownum', 'PROCEDURE'); print()

# filter
tmp3 = (
  tmp2
  .where(f.col('PROCEDURE_rownum') == 1)
)

# check
count_varlist(tmp3, ['PERSON_ID', 'PROCEDURE'])
tmpt = tab(tmp3, 'PROCEDURE', 'PROCEDURE_rownum'); print()

# tidy
tmp3 = tmp3.drop('PROCEDURE_rownum')

# check
tmpt = count_var(tmp3, 'PERSON_ID', ret=1, df_desc='post exclusion of multiple dose 1 and 2 PROCEDURE ', indx=3); print()
tmpc = tmpc.unionByName(tmpt)

# check
print(tmp3.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 4.4 Exclude dose less than 20 days after previous dose (QA6)

# COMMAND ----------
# filter out dose less than 20 days after previous dose
_win_PERSON_ID_ord = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE')
_win_PERSON_ID_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmp3 = (
  tmp3
  .withColumn('_tmp_DATE_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win_PERSON_ID_ord)))  
  .withColumn('qa_6', f.when(f.col('_tmp_DATE_diff') < 20, 1).otherwise(0))   
  .withColumn('_PERSON_ID_rownum', f.row_number().over(_win_PERSON_ID_ord))
  .withColumn('_PERSON_ID_egen_sum_qa_6', f.sum(f.col('qa_6')).over(_win_PERSON_ID_egen))  
)

# check 
tmpt = tab(tmp3, 'qa_6'); print()
tmpt = tabstat(tmp3, '_tmp_DATE_diff', byvar='qa_6'); print()
tmpt = tab(tmp3.where(f.col('_PERSON_ID_rownum') == 1), '_PERSON_ID_egen_sum_qa_6'); print()

# filter out and tidy
tmp4 = (
  tmp3
  .where(f.col('qa_6') == 0)
  .drop('_tmp_DATE_diff', 'qa_6', '_PERSON_ID_rownum', '_PERSON_ID_egen_sum_qa_6')
)

# check
tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='post exclusion of dose less than 20 days after previous dose', indx=4); print()
tmpc = tmpc.unionByName(tmpt)

# check
print(tmp4.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 4.5 Exclude dose sequence with very small numbers

# COMMAND ----------

# MAGIC %md ### 4.5.1 Recreate dose sequence (QA7)

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('recreate dose sequence')
print('---------------------------------------------------------------------------------')
# currently PROCEDURE takes values '1', '2', and 'B'
# not possible to distinguish between boosters (i.e., booster 1, booster 2, etc.) by SNOMED codes, simply 'B' 
# so we number boosters according to DATE and DATE_rownum, assuming a dose sequence start number of 3 after first and second dose
_win_cumulative = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE')\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

tmp4 = (
  tmp4
  .withColumn('_booster', f.when(f.col('PROCEDURE') == 'B', f.lit(1)).otherwise(0))
  .withColumn('_booster_cumulative', f.sum(f.col('_booster')).over(_win_cumulative))
  .withColumn('_booster_cumulative_plus2', f.col('_booster_cumulative') + f.lit(2))  
  .withColumn('dose_sequence', 
              f.when(f.col('PROCEDURE').isin(['1','2']), f.col('PROCEDURE').cast(t.IntegerType()))
              .when(f.col('PROCEDURE') == 'B', f.col('_booster_cumulative_plus2'))
              .otherwise(999999))
  .drop('_booster', '_booster_cumulative', '_booster_cumulative_plus2')
)

# check
count_varlist(tmp4, ['PERSON_ID', 'dose_sequence'])
tmpt = tab(tmp4, 'dose_sequence'); print()
tmpt = tab(tmp4, 'dose_sequence', 'PROCEDURE'); print()


print('---------------------------------------------------------------------------------')
print('recreate row number')
print('---------------------------------------------------------------------------------')
# add row number that we will compare _dose_seq against
_win_PERSON_ID_ord = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE')
_win_PERSON_ID = Window\
  .partitionBy('PERSON_ID')

tmp4 = (
  tmp4
  .withColumn('PERSON_ID_rownum', f.row_number().over(_win_PERSON_ID_ord))
  .withColumn('PERSON_ID_rownummax', f.count('PERSON_ID').over(_win_PERSON_ID)))

# check
count_varlist(tmp4, ['PERSON_ID', 'dose_sequence'])
tmpt = tab(tmp4, 'PERSON_ID_rownum'); print()
tmpt = tab(tmp4.where(f.col('PERSON_ID_rownum') == 1), 'PERSON_ID_rownummax'); print()


print('---------------------------------------------------------------------------------')
print('compare dose_sequence and PERSON_ID_rownum')
print('---------------------------------------------------------------------------------')
tmpt = tab(tmp4, 'PERSON_ID_rownum', 'dose_sequence'); print()

# check
print(tmp4.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

display(tmp4.where((f.col('PERSON_ID_rownum') == 4) & (f.col('dose_sequence') == 1)))

# COMMAND ----------

display(tmp4.where(f.col('PERSON_ID') == '68LUYM4JFSA2ORJ'))

# COMMAND ----------

# MAGIC %md ### 4.5.2 Exclude dose sequence with very small numbers

# COMMAND ----------

# check
tmpt = tab(tmp4, 'dose_sequence'); print()
assert tmp4.where(f.col('dose_sequence') > 7).count() < 100

# filter out
tmp5 = (
  tmp4
  .where(f.col('dose_sequence') <= 7)
)

# check
tmpt = tab(tmp5, 'dose_sequence'); print()

# check
tmpt = count_var(tmp5, 'PERSON_ID', ret=1, df_desc='post exlusion of dose sequence with very small numbers', indx=5); print()
tmpc = tmpc.unionByName(tmpt)

# check
print(tmp5.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 4.6 Flow diagram

# COMMAND ----------

# check flow table
tmpp = (
  tmpc
  .orderBy('indx')
  .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
  .withColumnRenamed('df_desc', 'stage')
  .toPandas()
)
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

# MAGIC %md # 5 Flag possible exclusions

# COMMAND ----------

tmpf1 = tmp5 # for flag/summary
tmpr1 = tmp5 # for prepare/reshape

# COMMAND ----------

# MAGIC %md ## 5.1 Flag dose_sequence NOT sequential (QA7)

# COMMAND ----------

# check
# display(tmpr2.where((f.col('dose_sequence') == 1) & (f.col('PERSON_ID_rownum') == 2)))
# display(tmpf1.where(f.col('PERSON_ID') == '0JIJQSCRYHBLDJ4')) 
# wrong order! dose 2 on date n then dose 1 on date n + 120

# COMMAND ----------

# flag
_win_PERSON_ID_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmpf1 = (
  tmpf1
  .withColumn('qa_7', f.lit(1) - udf_null_safe_equality('dose_sequence', 'PERSON_ID_rownum').cast(t.IntegerType()))
  .withColumn('diff', f.col('dose_sequence') - f.col('PERSON_ID_rownum'))
  .withColumn('_PERSON_ID_egen_max_qa_7', f.max(f.col('qa_7')).over(_win_PERSON_ID_egen)) 
)
  
# check
tmpt = tab(tmpf1, 'qa_7'); print()
tmpt = tab(tmpf1, 'diff', 'qa_7'); print()
tmpt = tab(tmpf1.where(f.col('_PERSON_ID_rownum') == 1), '_PERSON_ID_egen_max_qa_7'); print()

# tidy
tmpf1 = (
  tmpf1
  .drop('diff', '_PERSON_ID_egen_max_qa_7')
)

# check
print(tmpf1.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# check
# display(tmpf1.where(f.col('diff') == -3))
# display(tmp5.where(f.col('PERSON_ID') == '68LUYM4JFSA2ORJ'))

# COMMAND ----------

# MAGIC %md ## 5.2 Flag Mixed vaccine PRODUCT before 20210507 (QA8)

# COMMAND ----------

# flag
_win_PERSON_ID_ord = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE')
_win_PERSON_ID_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmpf1 = (
  tmpf1
  .withColumn('_tmp_PRODUCT_diff', f.when(f.col('PRODUCT') != f.lag(f.col('PRODUCT'), 1).over(_win_PERSON_ID_ord), 1).otherwise(0))
  .withColumn('qa_8', f.when((f.col('_tmp_PRODUCT_diff') == 1) & (f.col('DATE') < f.to_date(f.lit('2021-05-07'))), 1).otherwise(0))   
  .withColumn('_PERSON_ID_egen_max_qa_8', f.max(f.col('qa_8')).over(_win_PERSON_ID_egen)) 
)
  
# check
tmpt = tab(tmpf1, 'qa_8'); print()
tmpt = tab(tmpf1, 'qa_8', '_tmp_PRODUCT_diff'); print()
tmpt = tabstat(tmpf1, 'DATE', byvar='qa_8', date=1); print()
tmpt = tab(tmpf1.where(f.col('_PERSON_ID_rownum') == 1), '_PERSON_ID_egen_max_qa_8'); print()
tmpt = tab(tmpf1, 'PRODUCT', '_PERSON_ID_egen_max_qa_8'); print()

# tidy
tmpf1 = (
  tmpf1
  .drop('_tmp_PRODUCT_diff') #, '_PERSON_ID_egen_max_qa_8')
)

# check
print(tmpf1.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

tmpp = (
  tmpf1
  .where(f.col('qa_8') == 1)
  .groupBy('DATE')
  .agg(f.count(f.lit(1)).alias('n'))
  .toPandas()
)
tmpp['date_formatted'] = pd.to_datetime(tmpp['DATE'], errors='coerce')

# plot
plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2

axes.bar(tmpp['date_formatted'], tmpp['n'], width = 1, edgecolor = 'black')
# axes.set(xticks=np.arange(0, 19, step=1))
# axes.set_xticks(axes.get_xticks())
# axes.set_xticklabels(rotation=90, ha='right')
# axes.set_xticks(axes.get_xticks(), axes.get_xticklabels(), rotation=90, ha='right')
axes.set_xlim(datetime.datetime(2021, 1, 1), datetime.datetime(2021, 5, 7))
axes.set(xlabel="Date")
axes.set(ylabel="Number of records")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
for label in axes.get_xticklabels():
  print(label)
  label.set_rotation(90)
display(fig)

# COMMAND ----------

display(tmpf1.where(f.col('_PERSON_ID_egen_max_qa_8') == 1).orderBy('PERSON_ID', 'DATE'))

# COMMAND ----------

# MAGIC %md ## 5.3 Summarise

# COMMAND ----------

# summarise by PERSON_ID
tmpf2 = (
  tmpf1
  .select('PERSON_ID', 'qa_7', 'qa_8', 'dose_sequence')    
  .groupBy('PERSON_ID')
  .agg(
    f.count(f.lit(1)).alias('n_rows')
    , f.max(f.col(f'dose_sequence')).alias(f'dose_sequence_max')  
    , f.max(f.col(f'qa_7')).alias(f'qa_7')  
    , f.max(f.col(f'qa_8')).alias(f'qa_8')
  )
)

# check
tmpt = tab(tmpf2, 'n_rows'); print()
tmpt = tab(tmpf2, 'dose_sequence_max'); print()
tmpt = tab(tmpf2, 'n_rows', 'dose_sequence_max'); print()
tmpt = tab(tmpf2, 'qa_7'); print()
tmpt = tab(tmpf2, 'qa_8'); print()
tmpt = tab(tmpf2, 'qa_7', 'qa_8'); print()

# check
count_var(tmpf2, 'PERSON_ID'); print()
print(tmpf2.orderBy('PERSON_ID').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 6 Reshape

# COMMAND ----------

# MAGIC %md ## 6.1 Prepare

# COMMAND ----------

# check
print(tmpr1.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# tidy
tmpr2 = (
  tmpr1
  .select('PERSON_ID', f.col('DATE').alias('date'), 'dose_sequence', 'PRODUCT')
  .orderBy('PERSON_ID', 'dose_sequence')
)  

# check
count_varlist(tmpr2, ['PERSON_ID', 'dose_sequence']); print()
count_var(tmpr2, 'PERSON_ID'); print()
print(tmpr2.orderBy('PERSON_ID', 'dose_sequence').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 6.2 Reshape

# COMMAND ----------

# reshape
tmpr3 = (
  tmpr2
  .groupBy('PERSON_ID')
  .pivot('dose_sequence')
  .agg(
    f.min('date').alias('date')
    , f.first('PRODUCT').alias('PRODUCT')
  )
)

# rename columns
dict_rename = {}
for col in tmpr3.columns:
  col_rematch = re.match(r'(\d)\_(date|PRODUCT)', col)
  if(col_rematch):
    dict_rename[col] = col_rematch.group(2) + '_' + col_rematch.group(1)
tmpr4 = rename_columns(tmpr3, dict_rename); print()

# check
count_var(tmpr4, 'PERSON_ID'); print()
print(len(tmpr4.columns)); print()
print(pd.DataFrame({f'_cols': tmpr4.columns}).to_string()); print()

# COMMAND ----------

# MAGIC %md ## 6.3 Add flags

# COMMAND ----------

# add flags
tmpr5 = merge(tmpr4, tmpf2, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()

# COMMAND ----------

# MAGIC %md # 7 Check

# COMMAND ----------

display(tmpf2)

# COMMAND ----------

# check
display(tmpr5)

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

# save name
outName = f'{proj}_cur_vacc_reshaped'.lower()

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
tmpr5.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmpr5 = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md # 9 Filter 1st vaccination only

# COMMAND ----------

# MAGIC %md ## 9.1 Check

# COMMAND ----------

tmp6 = tmp5
count_var(tmp6, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md ## 9.2 Reduce to include 1st vaccination only

# COMMAND ----------

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
tmp7 = (tmp6.where(f.col('PERSON_ID_rownum') == 1))
        
# check
count_var(tmp7, 'PERSON_ID')

# COMMAND ----------

display(tmp7)

# COMMAND ----------

# MAGIC %md # 10 Save

# COMMAND ----------

outName = f'{proj}_cur_vacc_first'.lower()  
tmp7.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
