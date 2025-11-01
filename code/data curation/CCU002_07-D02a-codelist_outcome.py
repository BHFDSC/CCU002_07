# Databricks notebook source
# MAGIC %md # CCU002_07-D02a-codelist_outcome
# MAGIC
# MAGIC **Description** This notebook creates the codelist for outcomes.
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

# MAGIC %md # 1 Imports

# COMMAND ----------

# MAGIC %md ## 1.1 BHF_COVID_UK_PHENOTYPES codelist

# COMMAND ----------

bhf_phenotypes  = spark.table(path_ref_bhf_phenotypes)
map_ctv3_snomed = spark.table(path_ref_map_ctv3_snomed)

# COMMAND ----------

# ------------------------------------------------------------------------------
# bhf_phenotypes
# ------------------------------------------------------------------------------
bhf_phenotypes = bhf_phenotypes\
  .select(['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate'])\
  .dropDuplicates()

# cache
bhf_phenotypes.cache()
print(f'{bhf_phenotypes.count():,}'); print()

# check
tmpt = tab(bhf_phenotypes, 'name', 'terminology', var2_unstyled=1); print()



# COMMAND ----------

# MAGIC %md ## 1.2 Reference files

# COMMAND ----------

# pmeds           = get_archive_table('pmeds')
#map_ctv3_snomed = spark.table(path_map_ctv3_snomed)
#icd10           = spark.table(path_icd10) ####################################### # SHOULD THIS BE COMMENTED OUT?
#gdppr_snomed    = spark.table(path_gdppr_snomed)

# COMMAND ----------

# MAGIC %md # 2 Codelists for Diseases - Outcomes

# COMMAND ----------

# MAGIC %md ## 2.1 Arterial thrombotic events

# COMMAND ----------

#Acute myocardial infarction (AMI)

#We keep AMI with the code I21, I22, I23 (available in the BHF phenotype table).
#But we don't use the codes I24.1 and I25.2 (because used for covariates only and we are defining the codelist for AMI outcomes)

codelist_AMI = spark.createDataFrame(
  [
    ("AMI", "ICD10", "I21", "Acute myocardial infarction", "1", "20210127"),
    ("AMI", "ICD10", "I22", "Subsequent myocardial infarction", "1", "20210127"),
    ("AMI", "ICD10", "I23", "Certain current complications following acute myocardial infarction", "1", "20210127") 
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

#tmp_codelist_AMI = bhf_phenotypes\
#  .where((f.col('name') == 'AMI') & (f.col('terminology') == 'SNOMED'))

#codelist_AMI= codelist_AMI.unionByName(tmp_codelist_AMI)

# COMMAND ----------

# Stroke (ischaemic or unclassified stroke, spinal stroke or retinal infraction) 

# stroke

codelist_stroke = spark.createDataFrame(
  [ ("stroke", "ICD10", "I61", "Nontraumatic intracerebral hemorrhage", "1", "20210127"),
    ("stroke", "ICD10", "I64", "Stroke, not specified as haemorrhage or infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.0", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.1", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.2", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.3", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.4", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.5", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.8", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "I63.9", "Cerebral infarction", "1", "20210127"),
    ("stroke", "ICD10", "H34", "Retinal vascular occlusions", "1", "20210127"),
    ("stroke", "ICD10", "G95.1", "Avascular myelopathies (arterial or venous)", "1", "20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)
# other_arterial_embolism
codelist_other_arterial_embolism = spark.createDataFrame(
  [
    ("other_arterial_embolism","ICD10","I74","Arterial embolism and thrombosis","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# # check
# tmpt = tab(codelist_stroke, 'name', 'terminology', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 2.2 Venous thromboembolic events

# COMMAND ----------

# Pulmonary Embolim (PE)
codelist_PE = spark.createDataFrame(
  [
    ("PE", "ICD10", "I26", "Pulmonary embolism", "1", "20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


# COMMAND ----------

# DVT
codelist_DVT = spark.createDataFrame(
  [
    ("DVT","ICD10","I80","Phlebitis and thrombophlebitis","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# DVT_other
codelist_DVT_other = spark.createDataFrame(
  [
    ("DVT_other","ICD10","I82","Other vein thrombosis","1","20210127"),
    ("DVT_other","ICD10","O22.3","Thrombosis during pregnancy and puerperium","1","20210127"),
    ("DVT_other","ICD10","O87.1","Thrombosis during pregnancy and puerperium","1","20210127"),
    ("DVT_other","ICD10","O87.9","Thrombosis during pregnancy and puerperium","1","20210127"),
    ("DVT_other","ICD10","O88.2","Thrombosis during pregnancy and puerperium","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


# COMMAND ----------

# Intracranial Venous Thrombosis (ICVT)
codelist_ICVT = spark.createDataFrame(
  [
    ("ICVT","ICD10","G08","Intracranial venous thrombosis","1","20210127"),
    ("ICVT","ICD10","I67.6","Intracranial venous thrombosis","1","20210127"),
    ("ICVT","ICD10","I63.6","Intracranial venous thrombosis","1","20210127"),
    ("ICVT","ICD10","O22.5","Intracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
    ("ICVT","ICD10","O87.3","Intracranial venous thrombosis in pregnancy and puerperium","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


# COMMAND ----------

# Portal Vein Thrombosis (PVT)
codelist_PVT = spark.createDataFrame(
  [
    ("PVT","ICD10","I81","Portal vein thrombosis","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# MAGIC %md ## 2.4 Pericarditis

# COMMAND ----------

# Pericarditis
codelist_pericarditis = spark.createDataFrame(
  [
    ("pericarditis","ICD10","I30","Acute pericarditis","1","20210127"),
    ("pericarditis","ICD10","I32","Pericarditis in diseases classified elsewhere","1","20210127")
  
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# MAGIC %md ## 2.5 Myocarditis

# COMMAND ----------

# Myocarditis
codelist_myocarditis = spark.createDataFrame(
  [
    ("myocarditis","ICD10","I51.4","Myocarditis, unspecified","1","20210127"),
    ("myocarditis","ICD10","I40","Acute myocarditis","1","20210127"),
    ("myocarditis","ICD10","I41","Myocarditis in diseases classified elsewhere","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)



# COMMAND ----------

# MAGIC %md ## 2.6 Thrombocytopenic haematological events

# COMMAND ----------

# Thrombocytopenia
codelist_thrombocytopenia = spark.createDataFrame(
  [
    ("thrombocytopenia","ICD10","D69.3","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","ICD10","D69.4","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","ICD10","D69.5","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","ICD10","D69.6","Thrombocytopenia","1","20210127"),
    ("thrombocytopenia","ICD10","M31.1","Thrombotic microangiopathy","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)



# COMMAND ----------

# # Thrombotic Thrombocytopenic Purpura (TTP)
# codelist_TTP = spark.createDataFrame(
#   [
#     ("TTP","ICD10","M31.1","Thrombotic microangiopathy","1","20210127")
#   ],
#   ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
# )


# COMMAND ----------

# MAGIC %md ## 2.7 Inflammatory conditions

# COMMAND ----------

# Mucocutaneous lymph node syndrome - Kawasaki (Kawasaki)
codelist_Kawasaki = spark.createDataFrame(
  [
    ("Kawasaki","ICD10","M30.3","Mucocutaneous lymph node syndrome - Kawasaki","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# Paediatric Inflammatory Multisystem Syndrome (PIMS)
codelist_PIMS = spark.createDataFrame(
  [
    ("PIMS","ICD10","U07.5","Systemic inflammatory response syndrome","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# Systemic Inflammatory response Syndrome (SIRS)
codelist_SIRS = spark.createDataFrame(
  [
    ("SIRS","ICD10","R65","Systemic inflammatory response syndrome","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)
# Multi system inflammatory syndrome associated with COVID-19 (MSIS)
codelist_MSIS = spark.createDataFrame(
  [
    ("MSIS","ICD10","U10.9","Emergency use code for Multisystem inflammatory syndrome associated with COVID-19","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# fracture
codelist_fracture = spark.createDataFrame(
  [
    ("fracture","ICD10","S72","Upper leg fracture","1","20210127"),
    ("fracture","ICD10","S82","Lower leg fracture","1","20210127"),
    ("fracture","ICD10","S92","Foot fracture","1","20210127"),
    ("fracture","ICD10","T12","Fracture of lower limb","1","20210127"),
    ("fracture","ICD10","T02.5","Fractures involving multiple regions of both lower limbs","1","20210127"),
    ("fracture","ICD10","T02.3","Fractures involving multiple regions of both lower limbs","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


#display(codelist_fracture)

# COMMAND ----------

codelists = [codelist_AMI, codelist_stroke, codelist_other_arterial_embolism, 
            codelist_PE,codelist_DVT, codelist_DVT_other, codelist_ICVT, codelist_PVT, 
            codelist_thrombocytopenia, 
            codelist_pericarditis, codelist_myocarditis, 
            codelist_Kawasaki, codelist_PIMS, codelist_SIRS, codelist_MSIS,
            codelist_fracture]

clist_outcome = reduce(DataFrame.unionAll, codelists)


clist_outcome = clist_outcome\
  .withColumn('Disease_group',\
    f.when(f.col('name').isin(['AMI', 'stroke', 'other_arterial_embolism']), 'Arterial')\
    .when(f.col('name').isin(['PE', 'DVT', 'DVT_other', 'ICVT', 'PVT']), 'Venous')\
    .when(f.col('name').isin(['thrombocytopenia']), 'Thrombocytopenia')\
    .when(f.col('name').isin(['pericarditis', 'myocarditis']), 'Myocarditis/Pericarditis')\
    .when(f.col('name').isin(['Kawasaki', 'PIMS', 'SIRS', 'MSIS']), 'Inflammatory')\
    .when(f.col('name').isin(['fracture']), 'Lower limb fracture')\
  )

# reorder columns
vlist_ordered = ['Disease_group']
vlist_unordered = [v for v in clist_outcome.columns if v not in vlist_ordered]
vlist_all = vlist_ordered + vlist_unordered
clist_outcome = clist_outcome\
  .select(*vlist_all)

tmpt = tab(clist_outcome, 'name', 'Disease_group', var2_unstyled=1)

# COMMAND ----------

display(clist_outcome)

# COMMAND ----------

clistm = clist_outcome

# COMMAND ----------

# check
tmpt = tab(clistm, 'name', 'terminology')

# COMMAND ----------

#display(clistm.where(f.col('covariate_only') == 1))

# COMMAND ----------

# MAGIC %md # 4 Reformat

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
clistmr = clistm\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', 'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', '[\.\-\s]', '')).otherwise(f.col('code')))
display(clistmr)

# COMMAND ----------

# MAGIC %md # 5 Checks

# COMMAND ----------

for terminology in ['ICD10']:
  print(terminology)
  ctmp = clistmr\
    .where(f.col('terminology') == terminology)
  count_var(ctmp, 'code'); print()

# COMMAND ----------

# MAGIC %md # 6 Save

# COMMAND ----------

# # save name
outName = f'{proj}_out_codelist_outcome'


# # save previous version for comparison purposes
# _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# outName_pre = f'{outName}_pre{_datetimenow}'.lower()
# print(outName_pre)
# spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
# spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
clistmr.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
