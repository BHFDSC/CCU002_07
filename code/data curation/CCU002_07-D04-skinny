# Databricks notebook source
# MAGIC %md # CCU002_07-D04-skinny
# MAGIC  
# MAGIC **Description** This notebook creates the skinny patient table, which includes key patient characteristics.
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

# MAGIC %run "/Repos/as3293@medschl.cam.ac.uk/shds/common/skinny_20221113"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU002_07-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
hes_ae  = extract_batch_from_archive(parameters_df_datasets, 'hes_ae')
hes_op  = extract_batch_from_archive(parameters_df_datasets, 'hes_op')

# COMMAND ----------

# MAGIC %md # 2 Skinny

# COMMAND ----------

kpc_harmonised = key_patient_characteristics_harmonise(gdppr=gdppr, hes_apc=hes_apc, hes_ae=hes_ae, hes_op=hes_op)
kpc_selected = key_patient_characteristics_select(harmonised=kpc_harmonised)

# temp save (~15 minutes)
outName = f'{proj}_tmp_kpc_harmonised'.lower()
kpc_harmonised.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
kpc_harmonised = spark.table(f'{dbc}.{outName}')

# temp save (~20 minutes)
outName = f'{proj}_tmp_kpc_selected'.lower()
kpc_selected.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
kpc_selected = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md # 3 Check

# COMMAND ----------

count_var(kpc_selected, 'PERSON_ID')

# COMMAND ----------

display(kpc_selected)

# COMMAND ----------

# see .\data_checks\"skinny" - for further checks

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# save name
outName = f'{proj}_skinny'.lower()

# save previous version for comparison purposes
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
kpc_selected.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
