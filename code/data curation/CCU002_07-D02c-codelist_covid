# Databricks notebook source
# MAGIC %md # CCU002_07-D02c-codelist_covid
# MAGIC
# MAGIC **Description** This notebook creates the codelist for COVID-19.
# MAGIC
# MAGIC **Authors** Alexia Sampri, Tom Bolton
# MAGIC
# MAGIC **Reviewers**  Genevieve Cezard, Wen Shi
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

# MAGIC %md # 1 Codelists for Covid-19

# COMMAND ----------

# covid19 (SNOMED codes only)
# covid_status = ['Lab confirmed incidence', 'Lab confirmed historic', 'Clinically confirmed']
                   
tmp_covid19 = spark.createDataFrame(
  [
    ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
    ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
    ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
    ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
    ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
    ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
    ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
    ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
    ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
    ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
    ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
    ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
    ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
    ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
    ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
    ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed historic"),
    ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
    ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
    ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
    ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
    ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
    ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
    ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
    ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
    ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
    ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
    ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
    ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
    ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
    ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
    ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
    ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
    ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
    ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
    ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
    ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")
  ],
  ['code', 'term', 'sensitive_status', 'include_binary', 'covid_status']
)

codelist_covid19 = tmp_covid19\
  .distinct()\
  .withColumn('name', f.lit('covid19'))\
  .withColumn('terminology', f.lit('SNOMED'))\
  .withColumn('code_type', f.lit(''))\
  .withColumn('RecordDate', f.lit(''))\
  .select(['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate'])

# check
count_var(codelist_covid19, 'code')

# COMMAND ----------

# MAGIC %md # 2 Checks

# COMMAND ----------

display(codelist_covid19)

# COMMAND ----------

# MAGIC %md # 3 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_covid'

# save previous version for comparison purposes
# _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# outName_pre = f'{outName}_pre{_datetimenow}'.lower()
# print(outName_pre)
# spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
# spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
codelist_covid19.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
