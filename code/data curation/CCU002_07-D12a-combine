# Databricks notebook source
# MAGIC %md
# MAGIC # CCU002_07-D12a-combine
# MAGIC
# MAGIC **Description** This notebook creates the infection analysis-ready dataset for use in R Studio.
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

# MAGIC %run "../shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU002_07-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# cohort = spark.table(path_out_cohort)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_inf_cohort')

# covariates = spark.table(path_out_covariates)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_covariates')
covariates = spark.table(f'{dbc}.{proj}_out_inf_covariates')

# exposures = spark.table(path_out_exposures)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_exposures')
exposures = spark.table(f'{dbc}.{proj}_out_inf_exposures')

# # outcomes = spark.table(path_out_outcomes)
# spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_outcomes')
# outcomes = spark.table(f'{dbc}.{proj}_out_inf_outcomes')

# outcomes = spark.table(path_out_inf_ecds_outcomes)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_ecds_outcomes')
outcomes = spark.table(f'{dbc}.{proj}_out_inf_ecds_outcomes')

# COMMAND ----------

# spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_analysis')
# analysis= spark.table(f'{dbc}.{proj}_out_inf_analysis')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_ecds_analysis')
analysis= spark.table(f'{dbc}.{proj}_out_inf_ecds_analysis')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# rename columns

_cohort = cohort\
 .withColumnRenamed('ETHNIC_CAT', 'ethnicity')

_covariates = covariates\
  .withColumnRenamed('CENSOR_DATE_START', 'CENSOR_DATE_START_cov')\
  .withColumnRenamed('CENSOR_DATE_END', 'CENSOR_DATE_END_cov')

_outcomes = outcomes\
  .withColumnRenamed('CENSOR_DATE_START', 'CENSOR_DATE_START_out')\
  .withColumnRenamed('CENSOR_DATE_END', 'CENSOR_DATE_END_out')

# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------

analysis = merge(cohort, _covariates, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()
analysis = merge(analysis, exposures, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()
analysis = merge(analysis, _outcomes, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()

analysis = analysis\
  .withColumn('CHUNK', f.floor(f.rand(seed=1234) * 8) + f.lit(1))

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

# check
count_var(analysis, 'PERSON_ID'); print()
print(len(analysis.columns)); print()
print(pd.DataFrame({f'_cols': analysis.columns}).to_string()); print()
tmpt = tab(analysis, 'CHUNK'); print()

# COMMAND ----------

display(analysis)

# COMMAND ----------

analysis.groupBy("region").count().show()
analysis.groupBy("SEX").count().show()
analysis.groupBy("ETHNIC_CAT").count().show()
analysis.groupBy("IMD_2019_DECILES").count().show()

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save name
# outName = f'{proj}_out_inf_analysis'.lower()

# # save name
outName = f'{proj}_out_inf_ecds_analysis'.lower()

# save
analysis.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
