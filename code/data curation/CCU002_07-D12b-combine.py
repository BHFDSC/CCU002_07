# Databricks notebook source
# MAGIC %md
# MAGIC # CCU002_07-D12b-combine
# MAGIC
# MAGIC **Description** This notebook creates the analysis-ready vaccination dataset for use in R Studio.
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

# cohort = spark.table(path_out_vacc_cohort)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_vacc_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_vacc_cohort')

# covariates = spark.table(path_out_vacc_covariates)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_vacc_covariates')
covariates = spark.table(f'{dbc}.{proj}_out_vacc_covariates')

# # outcomes = spark.table(path_out_vacc_outcomes)
# spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_vacc_outcomes')
# outcomes = spark.table(f'{dbc}.{proj}_out_vacc_outcomes')

# outcomes = spark.table(path_out_vacc_outcomes)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_vacc_ecds_outcomes')
outcomes = spark.table(f'{dbc}.{proj}_out_vacc_ecds_outcomes')

# COMMAND ----------

# spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_vacc_analysis')
# analysis= spark.table(f'{dbc}.{proj}_out_vacc_analysis')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

_covariates = covariates\
  .withColumnRenamed('CENSOR_DATE_START', 'CENSOR_DATE_START_cov')\
  .withColumnRenamed('CENSOR_DATE_END', 'CENSOR_DATE_END_cov')

_outcomes = outcomes\
  .withColumnRenamed('CENSOR_DATE_START', 'CENSOR_DATE_START_out')\
  .withColumnRenamed('CENSOR_DATE_END', 'CENSOR_DATE_END_out')

# COMMAND ----------

display(cohort)

# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------

analysis = merge(cohort, _covariates, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()
analysis = merge(analysis, _outcomes, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

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

spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode","name")

# COMMAND ----------

# # save name
# outName = f'{proj}_out_vacc_analysis'.lower()

# save name
outName = f'{proj}_out_vacc_ecds_analysis'.lower()

# save
analysis.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
