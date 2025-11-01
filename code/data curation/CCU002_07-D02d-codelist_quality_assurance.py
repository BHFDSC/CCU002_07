# Databricks notebook source
# MAGIC %md # CCU002_07-D02d-codelist_quality_assurance
# MAGIC
# MAGIC **Description** This notebook creates the codelist for the quality assurance, which includes codelists for pregnancy and prostate cancer.
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

# MAGIC %md # 1 Codelists

# COMMAND ----------

# MAGIC %md ## Prostate cancer codelist

# COMMAND ----------

# prostate_cancer (SNOMED codes only)
codelist_prostate_cancer = spark.createDataFrame(
  [
    ("prostate_cancer","SNOMED","126906006","Neoplasm of prostate","",""),
    ("prostate_cancer","SNOMED","81232004","Radical cystoprostatectomy","",""),
    ("prostate_cancer","SNOMED","176106009","Radical cystoprostatourethrectomy","",""),
    ("prostate_cancer","SNOMED","176261008","Radical prostatectomy without pelvic node excision","",""),
    ("prostate_cancer","SNOMED","176262001","Radical prostatectomy with pelvic node sampling","",""),
    ("prostate_cancer","SNOMED","176263006","Radical prostatectomy with pelvic lymphadenectomy","",""),
    ("prostate_cancer","SNOMED","369775001","Gleason Score 2-4: Well differentiated","",""),
    ("prostate_cancer","SNOMED","369777009","Gleason Score 8-10: Poorly differentiated","",""),
    ("prostate_cancer","SNOMED","385377005","Gleason grade finding for prostatic cancer (finding)","",""),
    ("prostate_cancer","SNOMED","394932008","Gleason prostate grade 5-7 (medium) (finding)","",""),
    ("prostate_cancer","SNOMED","399068003","Malignant tumor of prostate (disorder)","",""),
    ("prostate_cancer","SNOMED","428262008","History of malignant neoplasm of prostate (situation)","","")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# MAGIC %md ## Pregnancy & birth codelist

# COMMAND ----------

# pregnancy

# v1
tmp_pregnancy_v1 = spark.createDataFrame(
  [
    ("171057006","PregnancyÂ alcoholÂ educationÂ (procedure)"),
    ("72301000119103","AsthmaÂ inÂ pregnancyÂ (disorder)"),
    ("10742121000119104","AsthmaÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("10745291000119103","MalignantÂ neoplasticÂ diseaseÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("10749871000119100","MalignantÂ neoplasticÂ diseaseÂ inÂ pregnancyÂ (disorder)"),
    ("20753005","HypertensiveÂ heartÂ diseaseÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("237227006","CongenitalÂ heartÂ diseaseÂ inÂ pregnancyÂ (disorder)"),
    ("169501005","Pregnant,Â diaphragmÂ failureÂ (finding)"),
    ("169560008","PregnantÂ -Â urineÂ testÂ confirmsÂ (finding)"),
    ("169561007","PregnantÂ -Â bloodÂ testÂ confirmsÂ (finding)"),
    ("169562000","PregnantÂ -Â vaginalÂ examinationÂ confirmsÂ (finding)"),
    ("169565003","PregnantÂ -Â plannedÂ (finding)"),
    ("169566002","PregnantÂ -Â unplannedÂ -Â wantedÂ (finding)"),
    ("413567003","AplasticÂ anemiaÂ associatedÂ withÂ pregnancyÂ (disorder)"),
    ("91948008","AsymptomaticÂ humanÂ immunodeficiencyÂ virusÂ infectionÂ inÂ pregnancyÂ (disorder)"),
    ("169488004","ContraceptiveÂ intrauterineÂ deviceÂ failureÂ -Â pregnantÂ (finding)"),
    ("169508004","Pregnant,Â sheathÂ failureÂ (finding)"),
    ("169564004","PregnantÂ -Â onÂ abdominalÂ palpationÂ (finding)"),
    ("77386006","PregnantÂ (finding)"),
    ("10746341000119109","AcquiredÂ immuneÂ deficiencyÂ syndromeÂ complicatingÂ childbirthÂ (disorder)"),
    ("10759351000119103","SickleÂ cellÂ anemiaÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("10757401000119104","Pre-existingÂ hypertensiveÂ heartÂ andÂ chronicÂ kidneyÂ diseaseÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("10757481000119107","Pre-existingÂ hypertensiveÂ heartÂ andÂ chronicÂ kidneyÂ diseaseÂ inÂ motherÂ complicatingÂ pregnancyÂ (disorder)"),
    ("10757441000119102","Pre-existingÂ hypertensiveÂ heartÂ diseaseÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("10759031000119106","Pre-existingÂ hypertensiveÂ heartÂ diseaseÂ inÂ motherÂ complicatingÂ pregnancyÂ (disorder)"),
    ("1474004","HypertensiveÂ heartÂ ANDÂ renalÂ diseaseÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("199006004","Pre-existingÂ hypertensiveÂ heartÂ diseaseÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ (disorder)"),
    ("199007008","Pre-existingÂ hypertensiveÂ heartÂ andÂ renalÂ diseaseÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ (disorder)"),
    ("22966008","HypertensiveÂ heartÂ ANDÂ renalÂ diseaseÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("59733002","HypertensiveÂ heartÂ diseaseÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("171054004","PregnancyÂ dietÂ educationÂ (procedure)"),
    ("106281000119103","Pre-existingÂ diabetesÂ mellitusÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("10754881000119104","DiabetesÂ mellitusÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("199225007","DiabetesÂ mellitusÂ duringÂ pregnancyÂ -Â babyÂ deliveredÂ (disorder)"),
    ("237627000","PregnancyÂ andÂ typeÂ 2Â diabetesÂ mellitusÂ (disorder)"),
    ("609563008","Pre-existingÂ diabetesÂ mellitusÂ inÂ pregnancyÂ (disorder)"),
    ("609566000","PregnancyÂ andÂ typeÂ 1Â diabetesÂ mellitusÂ (disorder)"),
    ("609567009","Pre-existingÂ typeÂ 2Â diabetesÂ mellitusÂ inÂ pregnancyÂ (disorder)"),
    ("199223000","DiabetesÂ mellitusÂ duringÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ (disorder)"),
    ("199227004","DiabetesÂ mellitusÂ duringÂ pregnancyÂ -Â babyÂ notÂ yetÂ deliveredÂ (disorder)"),
    ("609564002","Pre-existingÂ typeÂ 1Â diabetesÂ mellitusÂ inÂ pregnancyÂ (disorder)"),
    ("76751001","DiabetesÂ mellitusÂ inÂ motherÂ complicatingÂ pregnancy,Â childbirthÂ AND/ORÂ puerperiumÂ (disorder)"),
    ("526961000000105","PregnancyÂ adviceÂ forÂ patientsÂ withÂ epilepsyÂ (procedure)"),
    ("527041000000108","PregnancyÂ adviceÂ forÂ patientsÂ withÂ epilepsyÂ notÂ indicatedÂ (situation)"),
    ("527131000000100","PregnancyÂ adviceÂ forÂ patientsÂ withÂ epilepsyÂ declinedÂ (situation)"),
    ("10753491000119101","GestationalÂ diabetesÂ mellitusÂ inÂ childbirthÂ (disorder)"),
    ("40801000119106","GestationalÂ diabetesÂ mellitusÂ complicatingÂ pregnancyÂ (disorder)"),
    ("10562009","MalignantÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("198944004","BenignÂ essentialÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ -Â deliveredÂ (disorder)"),
    ("198945003","BenignÂ essentialÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ -Â deliveredÂ withÂ postnatalÂ complicationÂ (disorder)"),
    ("198946002","BenignÂ essentialÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ -Â notÂ deliveredÂ (disorder)"),
    ("198949009","RenalÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ (disorder)"),
    ("198951008","RenalÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ -Â deliveredÂ (disorder)"),
    ("198954000","RenalÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ withÂ postnatalÂ complicationÂ (disorder)"),
    ("199005000","Pre-existingÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ puerperiumÂ (disorder)"),
    ("23717007","BenignÂ essentialÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("26078007","HypertensionÂ secondaryÂ toÂ renalÂ diseaseÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("29259002","MalignantÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("65402008","Pre-existingÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("8218002","ChronicÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("10752641000119102","EclampsiaÂ withÂ pre-existingÂ hypertensionÂ inÂ childbirthÂ (disorder)"),
    ("118781000119108","Pre-existingÂ hypertensiveÂ chronicÂ kidneyÂ diseaseÂ inÂ motherÂ complicatingÂ pregnancyÂ (disorder)"),
    ("18416000","EssentialÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("198942000","BenignÂ essentialÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ (disorder)"),
    ("198947006","BenignÂ essentialÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ withÂ postnatalÂ complicationÂ (disorder)"),
    ("198952001","RenalÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ -Â deliveredÂ withÂ postnatalÂ complicationÂ (disorder)"),
    ("198953006","RenalÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ theÂ puerperiumÂ -Â notÂ deliveredÂ (disorder)"),
    ("199008003","Pre-existingÂ secondaryÂ hypertensionÂ complicatingÂ pregnancy,Â childbirthÂ andÂ puerperiumÂ (disorder)"),
    ("34694006","Pre-existingÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("37618003","ChronicÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("48552006","HypertensionÂ secondaryÂ toÂ renalÂ diseaseÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("71874008","BenignÂ essentialÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ childbirthÂ (disorder)"),
    ("78808002","EssentialÂ hypertensionÂ complicatingÂ AND/ORÂ reasonÂ forÂ careÂ duringÂ pregnancyÂ (disorder)"),
    ("91923005","AcquiredÂ immunodeficiencyÂ syndromeÂ virusÂ infectionÂ associatedÂ withÂ pregnancyÂ (disorder)"),
    ("10755671000119100","HumanÂ immunodeficiencyÂ virusÂ inÂ motherÂ complicatingÂ childbirthÂ (disorder)"),
    ("721166000","HumanÂ immunodeficiencyÂ virusÂ complicatingÂ pregnancyÂ childbirthÂ andÂ theÂ puerperiumÂ (disorder)"),
    ("449369001","StoppedÂ smokingÂ beforeÂ pregnancyÂ (finding)"),
    ("449345000","SmokedÂ beforeÂ confirmationÂ ofÂ pregnancyÂ (finding)"),
    ("449368009","StoppedÂ smokingÂ duringÂ pregnancyÂ (finding)"),
    ("88144003","RemovalÂ ofÂ ectopicÂ interstitialÂ uterineÂ pregnancyÂ requiringÂ totalÂ hysterectomyÂ (procedure)"),
    ("240154002","IdiopathicÂ osteoporosisÂ inÂ pregnancyÂ (disorder)"),
    ("956951000000104","PertussisÂ vaccinationÂ inÂ pregnancyÂ (procedure)"),
    ("866641000000105","PertussisÂ vaccinationÂ inÂ pregnancyÂ declinedÂ (situation)"),
    ("956971000000108","PertussisÂ vaccinationÂ inÂ pregnancyÂ givenÂ byÂ otherÂ healthcareÂ providerÂ (finding)"),
    ("169563005","PregnantÂ -Â onÂ historyÂ (finding)"),
    ("10231000132102","In-vitroÂ fertilizationÂ pregnancyÂ (finding)"),
    ("134781000119106","HighÂ riskÂ pregnancyÂ dueÂ toÂ recurrentÂ miscarriageÂ (finding)"),
    ("16356006","MultipleÂ pregnancyÂ (disorder)"),
    ("237239003","LowÂ riskÂ pregnancyÂ (finding)"),
    ("276367008","WantedÂ pregnancyÂ (finding)"),
    ("314204000","EarlyÂ stageÂ ofÂ pregnancyÂ (finding)"),
    ("439311009","IntendsÂ toÂ continueÂ pregnancyÂ (finding)"),
    ("713575004","DizygoticÂ twinÂ pregnancyÂ (disorder)"),
    ("80997009","QuintupletÂ pregnancyÂ (disorder)"),
    ("1109951000000101","PregnancyÂ insufficientlyÂ advancedÂ forÂ reliableÂ antenatalÂ screeningÂ (finding)"),
    ("1109971000000105","PregnancyÂ tooÂ advancedÂ forÂ reliableÂ antenatalÂ screeningÂ (finding)"),
    ("237238006","PregnancyÂ withÂ uncertainÂ datesÂ (finding)"),
    ("444661007","HighÂ riskÂ pregnancyÂ dueÂ toÂ historyÂ ofÂ pretermÂ laborÂ (finding)"),
    ("459166009","DichorionicÂ diamnioticÂ twinÂ pregnancyÂ (disorder)"),
    ("459167000","MonochorionicÂ twinÂ pregnancyÂ (disorder)"),
    ("459168005","MonochorionicÂ diamnioticÂ twinÂ pregnancyÂ (disorder)"),
    ("459171002","MonochorionicÂ monoamnioticÂ twinÂ pregnancyÂ (disorder)"),
    ("47200007","HighÂ riskÂ pregnancyÂ (finding)"),
    ("60810003","QuadrupletÂ pregnancyÂ (disorder)"),
    ("64254006","TripletÂ pregnancyÂ (disorder)"),
    ("65147003","TwinÂ pregnancyÂ (disorder)"),
    ("713576003","MonozygoticÂ twinÂ pregnancyÂ (disorder)"),
    ("171055003","PregnancyÂ smokingÂ educationÂ (procedure)"),
    ("10809101000119109","HypothyroidismÂ inÂ childbirthÂ (disorder)"),
    ("428165003","HypothyroidismÂ inÂ pregnancyÂ (disorder)")
  ],
  ['code', 'term']  
)
tmp_pregnancy = tmp_pregnancy_v1\
  .withColumn('name', f.lit('pregnancy'))\
  .withColumn('terminology', f.lit('SNOMED'))\
  .select(['name', 'terminology', 'code', 'term'])

# check
count_var(tmp_pregnancy, 'code')

# save
codelist_pregnancy = tmp_pregnancy

# COMMAND ----------

# append (union) codelists defined above
codelist = codelist_prostate_cancer\
  .select('name', 'terminology', 'code', 'term')\
  .unionByName(codelist_pregnancy)\
  .orderBy('name', 'terminology', 'code')

# COMMAND ----------

# MAGIC %md # 2 Check

# COMMAND ----------

# check 
tmpt = tab(codelist, 'name', 'terminology', var2_unstyled=1)

# COMMAND ----------

# check
display(codelist)

# COMMAND ----------

# MAGIC %md # 3 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_quality_assurance'

# save previous version for comparison purposes
# _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# outName_pre = f'{outName}_pre{_datetimenow}'.lower()
# print(outName_pre)
# spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
# spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
codelist.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
