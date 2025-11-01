# Databricks notebook source
# MAGIC %md # CCU002_07-D02b-codelist_comorbidity
# MAGIC
# MAGIC **Description** This notebook creates the codelist for comorbidities.
# MAGIC
# MAGIC **Author(s)** Alexia Sampri
# MAGIC
# MAGIC **Reviewer(s)** Genevieve Cezard, Wen Shi
# MAGIC
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

# MAGIC %md # 1 Data

# COMMAND ----------

# bhf_phenotypes  = spark.table(path_bhf_phenotypes)
# pmeds           = get_archive_table('pmeds') 
# map_ctv3_snomed = spark.table(path_map_ctv3_snomed)
# icd10           = spark.table(path_icd10)    ####################################### # SHOULD THIS BE COMMENTED OUT?
# gdppr_snomed    = spark.table(path_gdppr_snomed)

# COMMAND ----------

# MAGIC %md # 2 Codelists - Comorbidities

# COMMAND ----------

# comorbidities


# v2
# Source
# CHANGE -------->
# CVD-COVID-UK Consortium Shared Folder\Projects\CCU002_07\Analysis and results\codelist comorbidities CCU002_07 20220929 AS.csv
tmp_comorbidities_v1 = """
Disease_group,name,terminology,code,term,code_type,RecordDate
mentalhealth,substance_abuse,ICD10,E24.4,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F10,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F11,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F12,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F13,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F14,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F15,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F16,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F17,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F18,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F19,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,F55,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,G24.0,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,G31.2,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,G40.5,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,G62.1,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,G72.0,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,G72.1,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,I42.6,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,K29.2,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,K70,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,K85.2,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,K85.3,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,K86.0,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,O35.4,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,R78.1,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,R78.2,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,R78.3,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,R78.4,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,R78.5,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Y47,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Y49,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Z50.2,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Z50.3,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Z71.4,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Z71.5,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Z72.2,Substance abuse,1,20210127
mentalhealth,substance_abuse,ICD10,Z86.4,Substance abuse,1,20210127
mentalhealth,self_harm,ICD10,X60,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X61,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X62,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X63,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X64,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X65,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X66,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X67,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X68,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X69,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X70,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X71,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X72,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X73,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X74,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X75,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X76,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X77,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X78,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X79,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X80,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X81,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X82,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X83,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,X84,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y10,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y11,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y12,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y13,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y14,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y15,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y16,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y17,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y18,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y19,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y20,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y21,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y22,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y23,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y24,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y25,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y26,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y27,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y28,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y29,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y30,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y31,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y32,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y33,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y34,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y87.0,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Y87.2,Self-harm,1,20210127
mentalhealth,self_harm,ICD10,Z91.5,Self-harm,1,20210127
mentalhealth,mentalhealth_other,ICD10,F00,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F01,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F02.8,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F03,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F04,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F05,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F06,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F07,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F09,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F20,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F21,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F22,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F23,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F24,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F25,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F28,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F29,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F30,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F31,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F32,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F33,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F34,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F38,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F39,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F40,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F41,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F42,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F43,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F44,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F45,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F48,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F50,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F53,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F54,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F59,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F60,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F61,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F62,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F63,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F64,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F65,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F66,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F68,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F69,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,F99,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,Z09.3,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,Z50.4,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,Z86.5,Other mental health problems,1,20210127
mentalhealth,mentalhealth_other,ICD10,Z91.4,Other mental health problems,1,20210127
mentalhealth,behavioural,ICD10,F70,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F71,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F72,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F73,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F78,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F79,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F80.0,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F80.1,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F80.2,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F80.8,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F80.9,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F81,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F82,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F83,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F84,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F88,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F89,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F90,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F91,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F92,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F93,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F94,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F95,Behavioural/developmental disorders,1,20210127
mentalhealth,behavioural,ICD10,F98,Behavioural/developmental disorders,1,20210127
cancer,neoplasms,ICD10,C00,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C01,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C02,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C03,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C04,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C05,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C06,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C07,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C08,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C09,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C10,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C11,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C12,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C13,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C14,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C15,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C16,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C17,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C18,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C19,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C20,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C21,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C22,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C23,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C24,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C25,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C26,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C30,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C31,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C32,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C33,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C34,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C37,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C38,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C39,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C40,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C41,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C43,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C44,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C45,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C46,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C47,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C48,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C49,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C50,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C51,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C52,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C53,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C54,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C55,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C56,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C57,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C58,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C60,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C61,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C62,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C63,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C64,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C65,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C66,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C67,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C68,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C69,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C70,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C71,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C72,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C73,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C74,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C75,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C76,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C77,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C78,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C79,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C80,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C81,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C82,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C83,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C84,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C85,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C86,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C88,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C90,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C91,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C92,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C93,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C94,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C95,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C96,Neoplasms,1,20210127
cancer,neoplasms,ICD10,C97,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D00,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D01,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D02,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D05,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D06,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D07,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D09,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D12,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D13,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D14.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D14.2,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D14.3,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D14.4,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D15,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D20,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D32,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D33,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D34,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D35,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D37,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D38,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D39,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D40,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D41,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D42,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D43,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D44,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D45,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D46,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D47,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D48,Neoplasms,1,20210127
cancer,neoplasms,ICD10,D63.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,E34.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,E88.3,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G13.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G13.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G53.3,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G55.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G63.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G73.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G73.2,Neoplasms,1,20210127
cancer,neoplasms,ICD10,G94.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,M36.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,M36.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,M49.5,Neoplasms,1,20210127
cancer,neoplasms,ICD10,M82.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,M90.6,Neoplasms,1,20210127
cancer,neoplasms,ICD10,M90.7,Neoplasms,1,20210127
cancer,neoplasms,ICD10,N08.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,N16.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Y43.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Y43.2,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Y43.3,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Y84.2,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z08,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z51.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z51.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z51.2,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z54.1,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z54.2,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z85,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z86.0,Neoplasms,1,20210127
cancer,neoplasms,ICD10,Z92.3,Neoplasms,1,20210127
cancer,immunological,ICD10,D80,Immunological disorders,1,20210127
cancer,immunological,ICD10,D81,Immunological disorders,1,20210127
cancer,immunological,ICD10,D82,Immunological disorders,1,20210127
cancer,immunological,ICD10,D83,Immunological disorders,1,20210127
cancer,immunological,ICD10,D84,Immunological disorders,1,20210127
cancer,immunological,ICD10,G53.2,Immunological disorders,1,20210127
cancer,immunological,ICD10,Q98.0,Immunological disorders,1,20210127
cancer,blood,ICD10,D50,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D56.0,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D56.1,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D56.2,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D56.4,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D56.8,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D56.9,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D57.0,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D57.1,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D57.2,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D57.8,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D58,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D61.0,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D61.9,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D64,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D66,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D67,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.0,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.1,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.2,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.4,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.5,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.6,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.7,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.8,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D68.9,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D69,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D70,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D71,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D72,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D73,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D74,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D75,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,D76,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,M36.2,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,M36.3,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,M36.4,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,M90.4,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,N08.2,Anaemia and other blood disorders,1,20210127
cancer,blood,ICD10,Z86.2,Anaemia and other blood disorders,1,20210127
chronic,HIV,ICD10,B20,HIV,1,20210127
chronic,HIV,ICD10,B21,HIV,1,20210127
chronic,HIV,ICD10,B22,HIV,1,20210127
chronic,HIV,ICD10,B23,HIV,1,20210127
chronic,HIV,ICD10,B24,HIV,1,20210127
chronic,HIV,ICD10,F02.4,HIV,1,20210127
chronic,HIV,ICD10,R75,HIV,1,20210127
chronic,HIV,ICD10,Z21,HIV,1,20210127
chronic,tuberculosis,ICD10,A15,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,A16,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,A17,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,A18,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,A19,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,E35.0,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,K23.0,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,K67.3,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,K93.0,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,M01.1,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,M49.0,Tuberculosis,1,20210127
chronic,tuberculosis,ICD10,P37.0,Tuberculosis,1,20210127
chronic,chronic_other,ICD10,A50,Other,1,20210127
chronic,chronic_other,ICD10,A81,Other,1,20210127
chronic,chronic_other,ICD10,B18,Other,1,20210127
chronic,chronic_other,ICD10,B37.1,Other,1,20210127
chronic,chronic_other,ICD10,B37.5,Other,1,20210127
chronic,chronic_other,ICD10,B37.6,Other,1,20210127
chronic,chronic_other,ICD10,B37.7,Other,1,20210127
chronic,chronic_other,ICD10,B38.1,Other,1,20210127
chronic,chronic_other,ICD10,B39.1,Other,1,20210127
chronic,chronic_other,ICD10,B40.1,Other,1,20210127
chronic,chronic_other,ICD10,B44.0,Other,1,20210127
chronic,chronic_other,ICD10,B44.7,Other,1,20210127
chronic,chronic_other,ICD10,B45,Other,1,20210127
chronic,chronic_other,ICD10,B46,Other,1,20210127
chronic,chronic_other,ICD10,B48.7,Other,1,20210127
chronic,chronic_other,ICD10,B50.0,Other,1,20210127
chronic,chronic_other,ICD10,B50.8,Other,1,20210127
chronic,chronic_other,ICD10,B51.0,Other,1,20210127
chronic,chronic_other,ICD10,B51.8,Other,1,20210127
chronic,chronic_other,ICD10,B52.8,Other,1,20210127
chronic,chronic_other,ICD10,B52.0,Other,1,20210127
chronic,chronic_other,ICD10,B55,Other,1,20210127
chronic,chronic_other,ICD10,B57.2,Other,1,20210127
chronic,chronic_other,ICD10,B57.3,Other,1,20210127
chronic,chronic_other,ICD10,B57.4,Other,1,20210127
chronic,chronic_other,ICD10,B57.5,Other,1,20210127
chronic,chronic_other,ICD10,B58.0,Other,1,20210127
chronic,chronic_other,ICD10,B59,Other,1,20210127
chronic,chronic_other,ICD10,B67,Other,1,20210127
chronic,chronic_other,ICD10,B69,Other,1,20210127
chronic,chronic_other,ICD10,B73,Other,1,20210127
chronic,chronic_other,ICD10,B74,Other,1,20210127
chronic,chronic_other,ICD10,B78.7,Other,1,20210127
chronic,chronic_other,ICD10,B90,Other,1,20210127
chronic,chronic_other,ICD10,B91,Other,1,20210127
chronic,chronic_other,ICD10,B92,Other,1,20210127
chronic,chronic_other,ICD10,B94,Other,1,20210127
chronic,chronic_other,ICD10,F02.1,Other,1,20210127
chronic,chronic_other,ICD10,K23.1,Other,1,20210127
chronic,chronic_other,ICD10,K93.1,Other,1,20210127
chronic,chronic_other,ICD10,M00,Other,1,20210127
chronic,chronic_other,ICD10,N33.0,Other,1,20210127
chronic,chronic_other,ICD10,P35.0,Other,1,20210127
chronic,chronic_other,ICD10,P35.1,Other,1,20210127
chronic,chronic_other,ICD10,P35.2,Other,1,20210127
chronic,chronic_other,ICD10,P35.8,Other,1,20210127
chronic,chronic_other,ICD10,P35.9,Other,1,20210127
chronic,chronic_other,ICD10,P37.1,Other,1,20210127
respiratory,asthma,ICD10,J41,Asthma and chronic lower respiratory disease,1,20210127
respiratory,asthma,ICD10,J42,Asthma and chronic lower respiratory disease,1,20210127
respiratory,asthma,ICD10,J43,Asthma and chronic lower respiratory disease,1,20210127
respiratory,asthma,ICD10,J44,Asthma and chronic lower respiratory disease,1,20210127
respiratory,asthma,ICD10,J45,Asthma and chronic lower respiratory disease,1,20210127
respiratory,asthma,ICD10,J46,Asthma and chronic lower respiratory disease,1,20210127
respiratory,asthma,ICD10,J47,Asthma and chronic lower respiratory disease,1,20210127
respiratory,cystic_fibrosis,ICD10,E84,Cystic fibrosis,1,20210127
respiratory,cystic_fibrosis,ICD10,P75,Cystic fibrosis,1,20210127
respiratory,respiratory_injuries,ICD10,S17,Injuries,1,20210127
respiratory,respiratory_injuries,ICD10,S27,Injuries,1,20210127
respiratory,respiratory_injuries,ICD10,S28,Injuries,1,20210127
respiratory,respiratory_injuries,ICD10,T27,Injuries,1,20210127
respiratory,respiratory_injuries,ICD10,T91.4,Injuries,1,20210127
respiratory,respiratory_congenital,ICD10,Q30,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q31,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q32,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q33,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q34,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q35,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q36,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q37,Congenital anomalies,1,20210127
respiratory,respiratory_congenital,ICD10,Q79.0,Congenital anomalies,1,20210127
respiratory,respiratory_other,ICD10,G47.3,Other,1,20210127
respiratory,respiratory_other,ICD10,J60,Other,1,20210127
respiratory,respiratory_other,ICD10,J61,Other,1,20210127
respiratory,respiratory_other,ICD10,J62,Other,1,20210127
respiratory,respiratory_other,ICD10,J63,Other,1,20210127
respiratory,respiratory_other,ICD10,J64,Other,1,20210127
respiratory,respiratory_other,ICD10,J65,Other,1,20210127
respiratory,respiratory_other,ICD10,J66,Other,1,20210127
respiratory,respiratory_other,ICD10,J67,Other,1,20210127
respiratory,respiratory_other,ICD10,J68,Other,1,20210127
respiratory,respiratory_other,ICD10,J69,Other,1,20210127
respiratory,respiratory_other,ICD10,J70,Other,1,20210127
respiratory,respiratory_other,ICD10,J80,Other,1,20210127
respiratory,respiratory_other,ICD10,J81,Other,1,20210127
respiratory,respiratory_other,ICD10,J82,Other,1,20210127
respiratory,respiratory_other,ICD10,J84,Other,1,20210127
respiratory,respiratory_other,ICD10,J85,Other,1,20210127
respiratory,respiratory_other,ICD10,J86,Other,1,20210127
respiratory,respiratory_other,ICD10,J96.1,Other,1,20210127
respiratory,respiratory_other,ICD10,J98,Other,1,20210127
respiratory,respiratory_other,ICD10,P27,Other,1,20210127
respiratory,respiratory_other,ICD10,Y55.6,Other,1,20210127
respiratory,respiratory_other,ICD10,Z43.0,Other,1,20210127
respiratory,respiratory_other,ICD10,Z93.0,Other,1,20210127
respiratory,respiratory_other,ICD10,Z94.2,Other,1,20210127
metabolic,diabetes,ICD10,E10,Diabetes,1,20210127
metabolic,diabetes,ICD10,E11,Diabetes,1,20210127
metabolic,diabetes,ICD10,E12,Diabetes,1,20210127
metabolic,diabetes,ICD10,E13,Diabetes,1,20210127
metabolic,diabetes,ICD10,E14,Diabetes,1,20210127
metabolic,diabetes,ICD10,G59.0,Diabetes,1,20210127
metabolic,diabetes,ICD10,G63.2,Diabetes,1,20210127
metabolic,diabetes,ICD10,I79.2,Diabetes,1,20210127
metabolic,diabetes,ICD10,M14.2,Diabetes,1,20210127
metabolic,diabetes,ICD10,N08.3,Diabetes,1,20210127
metabolic,diabetes,ICD10,O24,Diabetes,1,20210127
metabolic,diabetes,ICD10,Y42.3,Diabetes,1,20210127
metabolic,endocrine_other,ICD10,E00,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E03.0,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E03.1,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E07.1,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E22.0,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E23.0,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E25,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E26.8,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E29.1,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E31,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E34.1,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E34.2,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E34.5,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,E34.8,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,G13.2,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,G73.5,Other endocrine,1,20210127
metabolic,endocrine_other,ICD10,Y42.1,Other endocrine,1,20210127
metabolic,metabolic,ICD10,D55,Metabolic,1,20210127
metabolic,metabolic,ICD10,E70,Metabolic,1,20210127
metabolic,metabolic,ICD10,E71,Metabolic,1,20210127
metabolic,metabolic,ICD10,E72,Metabolic,1,20210127
metabolic,metabolic,ICD10,E74,Metabolic,1,20210127
metabolic,metabolic,ICD10,E75,Metabolic,1,20210127
metabolic,metabolic,ICD10,E76,Metabolic,1,20210127
metabolic,metabolic,ICD10,E77,Metabolic,1,20210127
metabolic,metabolic,ICD10,E78,Metabolic,1,20210127
metabolic,metabolic,ICD10,E79.1,Metabolic,1,20210127
metabolic,metabolic,ICD10,E79.8,Metabolic,1,20210127
metabolic,metabolic,ICD10,E79.9,Metabolic,1,20210127
metabolic,metabolic,ICD10,E80.0,Metabolic,1,20210127
metabolic,metabolic,ICD10,E80.1,Metabolic,1,20210127
metabolic,metabolic,ICD10,E80.2,Metabolic,1,20210127
metabolic,metabolic,ICD10,E80.3,Metabolic,1,20210127
metabolic,metabolic,ICD10,E80.5,Metabolic,1,20210127
metabolic,metabolic,ICD10,E80.7,Metabolic,1,20210127
metabolic,metabolic,ICD10,E83,Metabolic,1,20210127
metabolic,metabolic,ICD10,E85,Metabolic,1,20210127
metabolic,metabolic,ICD10,E88.0,Metabolic,1,20210127
metabolic,metabolic,ICD10,E88.1,Metabolic,1,20210127
metabolic,metabolic,ICD10,E88.2,Metabolic,1,20210127
metabolic,metabolic,ICD10,E88.8,Metabolic,1,20210127
metabolic,metabolic,ICD10,E88.9,Metabolic,1,20210127
metabolic,metabolic,ICD10,G73.6,Metabolic,1,20210127
metabolic,metabolic,ICD10,L99.0,Metabolic,1,20210127
metabolic,metabolic,ICD10,M14.4,Metabolic,1,20210127
metabolic,metabolic,ICD10,M14.3,Metabolic,1,20210127
metabolic,metabolic,ICD10,N16.3,Metabolic,1,20210127
metabolic,digestive,ICD10,K20,Digestive,1,20210127
metabolic,digestive,ICD10,K21.0,Digestive,1,20210127
metabolic,digestive,ICD10,K22,Digestive,1,20210127
metabolic,digestive,ICD10,K23.8,Digestive,1,20210127
metabolic,digestive,ICD10,K25,Digestive,1,20210127
metabolic,digestive,ICD10,K26,Digestive,1,20210127
metabolic,digestive,ICD10,K27,Digestive,1,20210127
metabolic,digestive,ICD10,K28,Digestive,1,20210127
metabolic,digestive,ICD10,K29.0,Digestive,1,20210127
metabolic,digestive,ICD10,K29.1,Digestive,1,20210127
metabolic,digestive,ICD10,K29.3,Digestive,1,20210127
metabolic,digestive,ICD10,K29.4,Digestive,1,20210127
metabolic,digestive,ICD10,K29.5,Digestive,1,20210127
metabolic,digestive,ICD10,K29.6,Digestive,1,20210127
metabolic,digestive,ICD10,K29.7,Digestive,1,20210127
metabolic,digestive,ICD10,K29.8,Digestive,1,20210127
metabolic,digestive,ICD10,K29.9,Digestive,1,20210127
metabolic,digestive,ICD10,K31,Digestive,1,20210127
metabolic,digestive,ICD10,K50,Digestive,1,20210127
metabolic,digestive,ICD10,K51,Digestive,1,20210127
metabolic,digestive,ICD10,K52,Digestive,1,20210127
metabolic,digestive,ICD10,K55,Digestive,1,20210127
metabolic,digestive,ICD10,K57,Digestive,1,20210127
metabolic,digestive,ICD10,K59.2,Digestive,1,20210127
metabolic,digestive,ICD10,K63.0,Digestive,1,20210127
metabolic,digestive,ICD10,K63.1,Digestive,1,20210127
metabolic,digestive,ICD10,K63.2,Digestive,1,20210127
metabolic,digestive,ICD10,K63.3,Digestive,1,20210127
metabolic,digestive,ICD10,K66,Digestive,1,20210127
metabolic,digestive,ICD10,K72,Digestive,1,20210127
metabolic,digestive,ICD10,K73,Digestive,1,20210127
metabolic,digestive,ICD10,K74,Digestive,1,20210127
metabolic,digestive,ICD10,K75,Digestive,1,20210127
metabolic,digestive,ICD10,K76,Digestive,1,20210127
metabolic,digestive,ICD10,K80,Digestive,1,20210127
metabolic,digestive,ICD10,K81,Digestive,1,20210127
metabolic,digestive,ICD10,K82,Digestive,1,20210127
metabolic,digestive,ICD10,K83,Digestive,1,20210127
metabolic,digestive,ICD10,K85.0,Digestive,1,20210127
metabolic,digestive,ICD10,K85.1,Digestive,1,20210127
metabolic,digestive,ICD10,K85.8,Digestive,1,20210127
metabolic,digestive,ICD10,K85.9,Digestive,1,20210127
metabolic,digestive,ICD10,K86.1,Digestive,1,20210127
metabolic,digestive,ICD10,K86.2,Digestive,1,20210127
metabolic,digestive,ICD10,K86.3,Digestive,1,20210127
metabolic,digestive,ICD10,K86.8,Digestive,1,20210127
metabolic,digestive,ICD10,K86.9,Digestive,1,20210127
metabolic,digestive,ICD10,K87.0,Digestive,1,20210127
metabolic,digestive,ICD10,K90,Digestive,1,20210127
metabolic,digestive,ICD10,M07.4,Digestive,1,20210127
metabolic,digestive,ICD10,M07.5,Digestive,1,20210127
metabolic,digestive,ICD10,M09.1,Digestive,1,20210127
metabolic,digestive,ICD10,M09.2,Digestive,1,20210127
metabolic,digestive,ICD10,T86.4,Digestive,1,20210127
metabolic,digestive,ICD10,Z43.2,Digestive,1,20210127
metabolic,digestive,ICD10,Z43.3,Digestive,1,20210127
metabolic,digestive,ICD10,Z43.4,Digestive,1,20210127
metabolic,digestive,ICD10,Z46.5,Digestive,1,20210127
metabolic,digestive,ICD10,Z90.3,Digestive,1,20210127
metabolic,digestive,ICD10,Z90.4,Digestive,1,20210127
metabolic,digestive,ICD10,Z93.2,Digestive,1,20210127
metabolic,digestive,ICD10,Z93.3,Digestive,1,20210127
metabolic,digestive,ICD10,Z93.4,Digestive,1,20210127
metabolic,digestive,ICD10,Z93.5,Digestive,1,20210127
metabolic,renal,ICD10,D63.8,Renal/GU,1,20210127
metabolic,renal,ICD10,G63.8,Renal/GU,1,20210127
metabolic,renal,ICD10,G99.8,Renal/GU,1,20210127
metabolic,renal,ICD10,I68.8,Renal/GU,1,20210127
metabolic,renal,ICD10,M90.8,Renal/GU,1,20210127
metabolic,renal,ICD10,N08.4,Renal/GU,1,20210127
metabolic,renal,ICD10,N00,Renal/GU,1,20210127
metabolic,renal,ICD10,N01,Renal/GU,1,20210127
metabolic,renal,ICD10,N02,Renal/GU,1,20210127
metabolic,renal,ICD10,N03,Renal/GU,1,20210127
metabolic,renal,ICD10,N04,Renal/GU,1,20210127
metabolic,renal,ICD10,N05,Renal/GU,1,20210127
metabolic,renal,ICD10,N07,Renal/GU,1,20210127
metabolic,renal,ICD10,N11,Renal/GU,1,20210127
metabolic,renal,ICD10,N12,Renal/GU,1,20210127
metabolic,renal,ICD10,N13,Renal/GU,1,20210127
metabolic,renal,ICD10,N14,Renal/GU,1,20210127
metabolic,renal,ICD10,N15,Renal/GU,1,20210127
metabolic,renal,ICD10,N16.0,Renal/GU,1,20210127
metabolic,renal,ICD10,N16.2,Renal/GU,1,20210127
metabolic,renal,ICD10,N16.4,Renal/GU,1,20210127
metabolic,renal,ICD10,N16.5,Renal/GU,1,20210127
metabolic,renal,ICD10,N16.8,Renal/GU,1,20210127
metabolic,renal,ICD10,N18,Renal/GU,1,20210127
metabolic,renal,ICD10,N19,Renal/GU,1,20210127
metabolic,renal,ICD10,N20,Renal/GU,1,20210127
metabolic,renal,ICD10,N21,Renal/GU,1,20210127
metabolic,renal,ICD10,N22,Renal/GU,1,20210127
metabolic,renal,ICD10,N23,Renal/GU,1,20210127
metabolic,renal,ICD10,N25,Renal/GU,1,20210127
metabolic,renal,ICD10,N26,Renal/GU,1,20210127
metabolic,renal,ICD10,N28,Renal/GU,1,20210127
metabolic,renal,ICD10,N29,Renal/GU,1,20210127
metabolic,renal,ICD10,N31,Renal/GU,1,20210127
metabolic,renal,ICD10,N32,Renal/GU,1,20210127
metabolic,renal,ICD10,N33.8,Renal/GU,1,20210127
metabolic,renal,ICD10,N35,Renal/GU,1,20210127
metabolic,renal,ICD10,N36,Renal/GU,1,20210127
metabolic,renal,ICD10,N39.1,Renal/GU,1,20210127
metabolic,renal,ICD10,N39.3,Renal/GU,1,20210127
metabolic,renal,ICD10,N39.4,Renal/GU,1,20210127
metabolic,renal,ICD10,N40,Renal/GU,1,20210127
metabolic,renal,ICD10,N41,Renal/GU,1,20210127
metabolic,renal,ICD10,N42,Renal/GU,1,20210127
metabolic,renal,ICD10,N70,Renal/GU,1,20210127
metabolic,renal,ICD10,N71,Renal/GU,1,20210127
metabolic,renal,ICD10,N72,Renal/GU,1,20210127
metabolic,renal,ICD10,N73,Renal/GU,1,20210127
metabolic,renal,ICD10,N74,Renal/GU,1,20210127
metabolic,renal,ICD10,N80,Renal/GU,1,20210127
metabolic,renal,ICD10,N81,Renal/GU,1,20210127
metabolic,renal,ICD10,N82,Renal/GU,1,20210127
metabolic,renal,ICD10,N85,Renal/GU,1,20210127
metabolic,renal,ICD10,N86,Renal/GU,1,20210127
metabolic,renal,ICD10,N87,Renal/GU,1,20210127
metabolic,renal,ICD10,N88,Renal/GU,1,20210127
metabolic,renal,ICD10,P96.0,Renal/GU,1,20210127
metabolic,renal,ICD10,T82.4,Renal/GU,1,20210127
metabolic,renal,ICD10,T83.1,Renal/GU,1,20210127
metabolic,renal,ICD10,T83.2,Renal/GU,1,20210127
metabolic,renal,ICD10,T83.4,Renal/GU,1,20210127
metabolic,renal,ICD10,T83.5,Renal/GU,1,20210127
metabolic,renal,ICD10,T83.6,Renal/GU,1,20210127
metabolic,renal,ICD10,T83.8,Renal/GU,1,20210127
metabolic,renal,ICD10,T83.9,Renal/GU,1,20210127
metabolic,renal,ICD10,T85.5,Renal/GU,1,20210127
metabolic,renal,ICD10,T86.1,Renal/GU,1,20210127
metabolic,renal,ICD10,Y60.2,Renal/GU,1,20210127
metabolic,renal,ICD10,Y61.2,Renal/GU,1,20210127
metabolic,renal,ICD10,Y62.2,Renal/GU,1,20210127
metabolic,renal,ICD10,Y84.1,Renal/GU,1,20210127
metabolic,renal,ICD10,Z49,Renal/GU,1,20210127
metabolic,renal,ICD10,Z93.6,Renal/GU,1,20210127
metabolic,renal,ICD10,Z94.0,Renal/GU,1,20210127
metabolic,renal,ICD10,Z99.2,Renal/GU,1,20210127
metabolic,metabolic_congenital,ICD10,Q38.0,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q38.3,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q38.4,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q38.6,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q38.7,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q38.8,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q39,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q40.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q40.3,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q40.8,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q40.9,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q41,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q42,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q43.1,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q43.3,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q43.4,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q43.5,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q43.6,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q43.7,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q43.9,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q44,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q45,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q50.0,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q51,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q52.0,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q52.1,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q52.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q52.4,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q54.0,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q54.1,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q54.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q54.3,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q54.8,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q54.9,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q55.0,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q55.5,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q56,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q60.1,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q60.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q60.4,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q60.5,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q60.6,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q61,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.0,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.1,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.3,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.4,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.5,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.6,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q62.8,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q63.0,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q63.1,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q63.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q63.8,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q63.9,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q64,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q79.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q79.3,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q79.4,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q79.5,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q87.8,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q89.1,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_congenital,ICD10,Q89.2,Congenital anomalies of the digestive/renal/GU system,1,20210127
metabolic,metabolic_injuries,ICD10,S36,Injuries,1,20210127
metabolic,metabolic_injuries,ICD10,S37,Injuries,1,20210127
metabolic,metabolic_injuries,ICD10,S38,Injuries,1,20210127
metabolic,metabolic_injuries,ICD10,S39.6,Injuries,1,20210127
metabolic,metabolic_injuries,ICD10,S39.7,Injuries,1,20210127
metabolic,metabolic_injuries,ICD10,T06.5,Injuries,1,20210127
metabolic,metabolic_injuries,ICD10,T28,Injuries,1,20210127
metabolic,metabolic_injuries,ICD10,T91.5,Injuries,1,20210127
metabolic,metabolic_other,ICD10,E66,Other/unspecific,1,20210127
metabolic,metabolic_other,ICD10,G63.3,Other/unspecific,1,20210127
metabolic,metabolic_other,ICD10,G99.0,Other/unspecific,1,20210127
metabolic,metabolic_other,ICD10,M14.5,Other/unspecific,1,20210127
metabolic,metabolic_other,ICD10,N92,Other/unspecific,1,20210127
metabolic,metabolic_other,ICD10,Z86.3,Other/unspecific,1,20210127
metabolic,metabolic_other,ICD10,Z93.8,Other/unspecific,1,20210127
musculoskeletal,musculoskeletal,ICD10,G55.1,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,G55.2,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,G55.3,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,G63.5,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,G63.6,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,G73.7,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,J99.0,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,J99.1,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,L62.0,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M05,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M06,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M07.0,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M07.1,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M07.2,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M07.3,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M07.6,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M08,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M09.8,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M10,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M11,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M12,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M13,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M14.0,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M14.6,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M14.8,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M30,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M31,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M32,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M33,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M34,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M35,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M40,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M41,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M42,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M43,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M45,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M46,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M47,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M48,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M50,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M51,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M53,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M54,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M60,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M61,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M62,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M63.8,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M80.1,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M80.2,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M80.3,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M80.4,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M80.5,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M80.8,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M80.9,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.1,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.2,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.3,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.4,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.5,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.6,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.8,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M81.9,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M82.1,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M82.8,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M84.0,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M84.1,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M84.2,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M84.8,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M84.9,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M85,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M86.3,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M86.4,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M86.5,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M86.6,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M89,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M90.0,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M91,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M92,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M93,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,M94,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,N08.5,musculoskeletal/connective tissue,1,20210127
musculoskeletal,musculoskeletal,ICD10,Y45.4,musculoskeletal/connective tissue,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S13,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S22.0,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S22.1,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S22.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S22.5,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S23,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S32,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S33,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S68.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S68.4,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S68.8,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S77,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S78,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S87,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S88,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S97,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S98.0,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S98.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S98.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,S98.4,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T02,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T04,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T05,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T20.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T20.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T21.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T21.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T22.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T22.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T23.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T23.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T23.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T23.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T24.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T24.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T25.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T25.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T25.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T25.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T29.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T29.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T30.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T30.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.4,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.5,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.8,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T31.9,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.4,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.5,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.8,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T32.9,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T87.3,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T87.4,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T87.5,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T87.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T91.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T91.8,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T92.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T93.1,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T93.4,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T93.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T94.0,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T94.1,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T95.0,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T95.1,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T95.4,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T95.8,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,T95.9,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Y83.5,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Z89.1,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Z89.2,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Z89.5,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Z89.6,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Z89.7,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Z89.8,Skeletal injuries/amputations,1,20210127
musculoskeletal,skeletal_injuries,ICD10,Z97.1,Skeletal injuries/amputations,1,20210127
musculoskeletal,skin_chronic,ICD10,L10,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L11.0,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L11.8,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L11.9,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L12,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L13,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L14,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L28,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L40,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L41,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L42,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L43,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L44,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L45,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L57,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L58.1,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L59,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L87,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L88,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L90,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L92,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L95,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L93,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,L98.5,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,M09.0,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q80,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q81,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q87.0,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q87.1,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q87.2,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q87.3,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q87.4,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q87.5,Chronic skin disorders,1,20210127
musculoskeletal,skin_chronic,ICD10,Q89.4,Chronic skin disorders,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q18.8,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q65.0,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q65.1,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q65.2,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q65.8,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q65.9,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q67.5,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q68.2,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q68.3,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q68.4,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q68.5,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q71,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q72,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q73,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q74,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q75.3,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q75.4,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q75.5,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q75.8,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q75.9,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q76.1,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q76.2,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q76.3,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q76.4,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q77,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q78,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q79.6,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q79.8,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q82.0,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q82.1,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q82.2,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q82.3,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q82.4,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q82.9,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q86.2,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q89.7,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q89.8,Congenital anomalies,1,20210127
musculoskeletal,musculoskeletal_congenital,ICD10,Q89.9,Congenital anomalies,1,20210127
neurological,epilepsy,ICD10,F80.3,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.0,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.1,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.2,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.3,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.4,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.6,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.7,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.8,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G40.9,Epilepsy,1,20210127
neurological,epilepsy,ICD10,G41,Epilepsy,1,20210127
neurological,epilepsy,ICD10,R56.8,Epilepsy,1,20210127
neurological,epilepsy,ICD10,Y46.0,Epilepsy,1,20210127
neurological,epilepsy,ICD10,Y46.1,Epilepsy,1,20210127
neurological,epilepsy,ICD10,Y46.2,Epilepsy,1,20210127
neurological,epilepsy,ICD10,Y46.3,Epilepsy,1,20210127
neurological,epilepsy,ICD10,Y46.4,Epilepsy,1,20210127
neurological,epilepsy,ICD10,Y46.5,Epilepsy,1,20210127
neurological,epilepsy,ICD10,Y46.6,Epilepsy,1,20210127
neurological,celebral_palsy,ICD10,G80,Cerebral palsy,1,20210127
neurological,celebral_palsy,ICD10,G81,Cerebral palsy,1,20210127
neurological,celebral_palsy,ICD10,G82,Cerebral palsy,1,20210127
neurological,celebral_palsy,ICD10,G83,Cerebral palsy,1,20210127
neurological,brain_injuries,ICD10,S05,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S06,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S07,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S08,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S12,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S14,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S24,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S34,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S44,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S54,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S64,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S74,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S84,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,S94,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T06.0,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T06.1,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T06.2,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T26,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T90.4,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T90.5,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T91.1,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T91.3,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,brain_injuries,ICD10,T92.4,"Injuries of brain, nerves, eyes or ears",1,20210127
neurological,chronic_eye,ICD10,H05.1,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H05.2,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H05.3,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H05.4,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H05.5,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H05.8,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H05.9,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H13.3,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H17,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H18,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H19.3,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H19.8,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H21,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H26,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H27,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H28.0,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H28.1,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H28.2,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H31,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H32.8,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H33,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H34,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H35,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H40,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H42.0,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H43,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H44,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H47,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H54.0,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H54.1,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H54.2,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,H54.4,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,T85.2,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,T85.3,Chronic eye conditions,1,20210127
neurological,chronic_eye,ICD10,Z44.2,Chronic eye conditions,1,20210127
neurological,chronic_ear,ICD10,H60.2,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H65.2,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H65.3,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H65.4,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H66.1,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H66.2,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H66.3,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H69.0,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H70.1,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H73.1,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H74.0,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H74.1,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H74.2,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H74.3,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H75.0,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H80,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H81.0,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H81.4,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H83.0,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H83.2,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H90.0,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H90.3,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H90.5,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H90.6,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,H91,Chronic ear conditions,1,20210127
neurological,chronic_ear,ICD10,Z45.3,Chronic ear conditions,1,20210127
neurological,perinatal,ICD10,P10,Perinatal conditions,1,20210127
neurological,perinatal,ICD10,P21.0,Perinatal conditions,1,20210127
neurological,perinatal,ICD10,P52,Perinatal conditions,1,20210127
neurological,perinatal,ICD10,P57,Perinatal conditions,1,20210127
neurological,perinatal,ICD10,P90,Perinatal conditions,1,20210127
neurological,perinatal,ICD10,P91.1,Perinatal conditions,1,20210127
neurological,perinatal,ICD10,P91.2,Perinatal conditions,1,20210127
neurological,perinatal,ICD10,P91.6,Perinatal conditions,1,20210127
neurological,neurological_congenital,ICD10,Q00,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q01,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q02,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q03,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q04,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q05,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q06,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q07,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q10.4,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q10.7,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q11,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q12,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q13.0,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q13.1,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q13.2,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q13.3,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q13.4,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q13.8,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q13.9,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q14,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q15,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q16,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q75.0,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q75.1,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q85,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q86.0,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q86.1,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q86.8,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q90,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q91,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q92,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q93,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q95.2,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q95.3,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q97,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_congenital,ICD10,Q99,Congenital anomalies of neurological or sensory systems,1,20210127
neurological,neurological_other,ICD10,F02.2,Other,1,20210127
neurological,neurological_other,ICD10,F02.3,Other,1,20210127
neurological,neurological_other,ICD10,G00,Other,1,20210127
neurological,neurological_other,ICD10,G01,Other,1,20210127
neurological,neurological_other,ICD10,G02,Other,1,20210127
neurological,neurological_other,ICD10,G03,Other,1,20210127
neurological,neurological_other,ICD10,G04,Other,1,20210127
neurological,neurological_other,ICD10,G05,Other,1,20210127
neurological,neurological_other,ICD10,G06,Other,1,20210127
neurological,neurological_other,ICD10,G07,Other,1,20210127
neurological,neurological_other,ICD10,G08,Other,1,20210127
neurological,neurological_other,ICD10,G09,Other,1,20210127
neurological,neurological_other,ICD10,G10,Other,1,20210127
neurological,neurological_other,ICD10,G11,Other,1,20210127
neurological,neurological_other,ICD10,G12,Other,1,20210127
neurological,neurological_other,ICD10,G13.8,Other,1,20210127
neurological,neurological_other,ICD10,G14,Other,1,20210127
neurological,neurological_other,ICD10,G20,Other,1,20210127
neurological,neurological_other,ICD10,G21,Other,1,20210127
neurological,neurological_other,ICD10,G22,Other,1,20210127
neurological,neurological_other,ICD10,G23,Other,1,20210127
neurological,neurological_other,ICD10,G24.1,Other,1,20210127
neurological,neurological_other,ICD10,G24.2,Other,1,20210127
neurological,neurological_other,ICD10,G24.3,Other,1,20210127
neurological,neurological_other,ICD10,G24.4,Other,1,20210127
neurological,neurological_other,ICD10,G24.5,Other,1,20210127
neurological,neurological_other,ICD10,G24.8,Other,1,20210127
neurological,neurological_other,ICD10,G24.9,Other,1,20210127
neurological,neurological_other,ICD10,G25,Other,1,20210127
neurological,neurological_other,ICD10,G26,Other,1,20210127
neurological,neurological_other,ICD10,G30,Other,1,20210127
neurological,neurological_other,ICD10,G31.0,Other,1,20210127
neurological,neurological_other,ICD10,G31.1,Other,1,20210127
neurological,neurological_other,ICD10,G31.8,Other,1,20210127
neurological,neurological_other,ICD10,G31.9,Other,1,20210127
neurological,neurological_other,ICD10,G32,Other,1,20210127
neurological,neurological_other,ICD10,G35,Other,1,20210127
neurological,neurological_other,ICD10,G36,Other,1,20210127
neurological,neurological_other,ICD10,G37,Other,1,20210127
neurological,neurological_other,ICD10,G43,Other,1,20210127
neurological,neurological_other,ICD10,G44,Other,1,20210127
neurological,neurological_other,ICD10,G45,Other,1,20210127
neurological,neurological_other,ICD10,G46,Other,1,20210127
neurological,neurological_other,ICD10,G47.0,Other,1,20210127
neurological,neurological_other,ICD10,G47.1,Other,1,20210127
neurological,neurological_other,ICD10,G47.2,Other,1,20210127
neurological,neurological_other,ICD10,G47.4,Other,1,20210127
neurological,neurological_other,ICD10,G47.8,Other,1,20210127
neurological,neurological_other,ICD10,G47.9,Other,1,20210127
neurological,neurological_other,ICD10,G50,Other,1,20210127
neurological,neurological_other,ICD10,G51,Other,1,20210127
neurological,neurological_other,ICD10,G52,Other,1,20210127
neurological,neurological_other,ICD10,G53.0,Other,1,20210127
neurological,neurological_other,ICD10,G53.1,Other,1,20210127
neurological,neurological_other,ICD10,G53.8,Other,1,20210127
neurological,neurological_other,ICD10,G54,Other,1,20210127
neurological,neurological_other,ICD10,G55.8,Other,1,20210127
neurological,neurological_other,ICD10,G56,Other,1,20210127
neurological,neurological_other,ICD10,G57,Other,1,20210127
neurological,neurological_other,ICD10,G58,Other,1,20210127
neurological,neurological_other,ICD10,G59.8,Other,1,20210127
neurological,neurological_other,ICD10,G60,Other,1,20210127
neurological,neurological_other,ICD10,G61,Other,1,20210127
neurological,neurological_other,ICD10,G62.0,Other,1,20210127
neurological,neurological_other,ICD10,G62.2,Other,1,20210127
neurological,neurological_other,ICD10,G62.8,Other,1,20210127
neurological,neurological_other,ICD10,G62.9,Other,1,20210127
neurological,neurological_other,ICD10,G64,Other,1,20210127
neurological,neurological_other,ICD10,G70,Other,1,20210127
neurological,neurological_other,ICD10,G71,Other,1,20210127
neurological,neurological_other,ICD10,G72.2,Other,1,20210127
neurological,neurological_other,ICD10,G72.3,Other,1,20210127
neurological,neurological_other,ICD10,G72.4,Other,1,20210127
neurological,neurological_other,ICD10,G72.8,Other,1,20210127
neurological,neurological_other,ICD10,G72.9,Other,1,20210127
neurological,neurological_other,ICD10,G73.0,Other,1,20210127
neurological,neurological_other,ICD10,G73.3,Other,1,20210127
neurological,neurological_other,ICD10,G90,Other,1,20210127
neurological,neurological_other,ICD10,G91,Other,1,20210127
neurological,neurological_other,ICD10,G92,Other,1,20210127
neurological,neurological_other,ICD10,G93,Other,1,20210127
neurological,neurological_other,ICD10,G94.2,Other,1,20210127
neurological,neurological_other,ICD10,G94.8,Other,1,20210127
neurological,neurological_other,ICD10,G95,Other,1,20210127
neurological,neurological_other,ICD10,G96,Other,1,20210127
neurological,neurological_other,ICD10,G98,Other,1,20210127
neurological,neurological_other,ICD10,G99.1,Other,1,20210127
neurological,neurological_other,ICD10,G99.2,Other,1,20210127
neurological,neurological_other,ICD10,I60,Other,1,20210127
neurological,neurological_other,ICD10,I61,Other,1,20210127
neurological,neurological_other,ICD10,I62,Other,1,20210127
neurological,neurological_other,ICD10,I63,Other,1,20210127
neurological,neurological_other,ICD10,I64,Other,1,20210127
neurological,neurological_other,ICD10,I65,Other,1,20210127
neurological,neurological_other,ICD10,I66,Other,1,20210127
neurological,neurological_other,ICD10,I67,Other,1,20210127
neurological,neurological_other,ICD10,I68.0,Other,1,20210127
neurological,neurological_other,ICD10,I68.2,Other,1,20210127
neurological,neurological_other,ICD10,I69,Other,1,20210127
neurological,neurological_other,ICD10,I72.0,Other,1,20210127
neurological,neurological_other,ICD10,I72.5,Other,1,20210127
neurological,neurological_other,ICD10,T85.0,Other,1,20210127
neurological,neurological_other,ICD10,T85.1,Other,1,20210127
neurological,neurological_other,ICD10,Y46.7,Other,1,20210127
neurological,neurological_other,ICD10,Y46.8,Other,1,20210127
neurological,neurological_other,ICD10,Z98.2,Other,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q20,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q21,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q22,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q23,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q24,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q25,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q26,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_congenital,ICD10,Q89.3,Congenital heart disease,1,20210127
cardiovascular,cardiovascular_other,ICD10,I00,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I01,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I02,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I05,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I06,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I07,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I08,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I09,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I10,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I11,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I12,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I13,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I15,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I20,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I21,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I22,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I23,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I24,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I25,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I26,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I27,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I28,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I31,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I32,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I33,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I34,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I35,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I36,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I37,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I38,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I39,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I41,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.0,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.2,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.3,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.4,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.5,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.7,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I42.9,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I43.0,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I43.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I43.2,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I43.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I44.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I44.2,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I44.3,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I44.4,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I44.5,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I44.6,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I44.7,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.2,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.3,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.4,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.5,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.6,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I45.9,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I46,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I47,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I48,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I49,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I50,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I51,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I52.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I70,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I71,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I72.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I72.2,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I72.3,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I72.4,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I72.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I72.9,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I73,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I74,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I77,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I79.0,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I79.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I79.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I81,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I82,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I98,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,I99,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,M03.6,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,N08.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Q27,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Q28,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,S26,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.0,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.2,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.3,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.5,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.6,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.7,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.8,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T82.9,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,T86.2,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Y60.5,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Y61.5,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Y62.5,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Y84.0,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Z45.0,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Z50.0,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Z94.1,Other,1,20210127
cardiovascular,cardiovascular_other,ICD10,Z95,Other,1,20210127
nonspecific,nonspecific_chronic,ICD10,R62,Codes indicating non-specific chronic condition,1,20210127
nonspecific,nonspecific_chronic,ICD10,R63.3,Codes indicating non-specific chronic condition,1,20210127
nonspecific,nonspecific_chronic,ICD10,Z43.1,Codes indicating non-specific chronic condition,1,20210127
nonspecific,nonspecific_chronic,ICD10,Z51.5,Codes indicating non-specific chronic condition,1,20210127
nonspecific,nonspecific_chronic,ICD10,Z75.5,Codes indicating non-specific chronic condition,1,20210127
nonspecific,nonspecific_chronic,ICD10,Z93.1,Codes indicating non-specific chronic condition,1,20210127
nonspecific,nonspecific_chronic,ICD10,Z99.3,Codes indicating non-specific chronic condition,1,20210127



"""
tmp_comorbidities_v1 = spark.createDataFrame(pd.DataFrame(pd.read_csv(io.StringIO(tmp_comorbidities_v1))))


# check
count_var(tmp_comorbidities_v1, 'code')

# save
clist_comorbidity = tmp_comorbidities_v1

# COMMAND ----------

tmpt = tab(clist_comorbidity, 'name')

# COMMAND ----------

# MAGIC %md # 3 Reformat

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
clistmr = clist_comorbidity\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

# check
tmpt = tab(clistmr, 'name', var2_unstyled=1)

# COMMAND ----------

# check
display(clistmr)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_comorbidity'


# # save previous version for comparison purposes
# _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# outName_pre = f'{outName}_pre{_datetimenow}'.lower()
# print(outName_pre)
# spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
# spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
clistmr.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')


# COMMAND ----------

print(outName)
