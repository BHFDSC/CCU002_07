# Databricks notebook source
# MAGIC %md # CCU002_07-D02a-codelist_outcome_all
# MAGIC
# MAGIC **Description** This notebook creates the codelist for outcomes.
# MAGIC
# MAGIC **Authors** Alexia Sampri
# MAGIC
# MAGIC **Reviewers**
# MAGIC
# MAGIC **Last Reviewed**
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


codelist_AMI = spark.createDataFrame(
  [
    ("AMI","SNOMED","401303003","Acute ST segment elevation myocardial infarction (disorder)","1",""),
    ("AMI","SNOMED","401314000","Acute non-ST segment elevation myocardial infarction (disorder)","1",""),
    ("AMI", "ICD10", "I21", "Acute myocardial infarction", "1", "20210127"),
    ("AMI", "ICD10", "I22", "Subsequent myocardial infarction", "1", "20210127"),
    ("AMI", "ICD10", "I23", "Certain current complications following acute myocardial infarction", "1", "20210127") 
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


# COMMAND ----------

# Stroke (ischaemic or unclassified stroke, spinal stroke or retinal infraction) 


# #stroke_any
# codelist_stroke_any = spark.createDataFrame(
#   [
#     ("Any stroke","SNOMED","230690007","Transient ischemic attack (disorder)","1",""),
#     ("Mesenteric ischaemia", "SNOMED","91489000", "Acute vascular insufficiency of intestine (disorder)","1",""),
#     ("Subarachnoid haemorrhage (with ICH)", "SNOMED","21454007", "Subarachnoid intracranial hemorrhage (disorder)","1",""),
#     ("Intracerebral haemorrhage (ICH)", "SNOMED","274100004", "Cerebral hemorrhage (disorder)","1",""),
#     ("Intracranial venous thrombosis", "SNOMED","297157005", "Thrombus of intracranial vein (disorder)","1","")
# ],
#   ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
# )

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
    ("stroke", "ICD10", "G95.1", "Avascular myelopathies (arterial or venous)", "1", "20210127"),
    ("stroke", "SNOMED","91489000", "Acute vascular insufficiency of intestine (disorder)","1",""),
    ("stroke", "SNOMED","21454007", "Subarachnoid intracranial hemorrhage (disorder)","1",""),
    ("stroke", "SNOMED","274100004", "Cerebral hemorrhage (disorder)","1",""),
    ("stroke", "SNOMED","297157005", "Thrombus of intracranial vein (disorder)","1","")
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

# COMMAND ----------

# MAGIC %md ## 2.2 Venous thromboembolic events

# COMMAND ----------

# Pulmonary Embolim (PE)
codelist_PE = spark.createDataFrame(
  [
    ("PE","SNOMED","59282003","Pulmonary embolism (disorder)","1",""),
    ("PE", "ICD10", "I26", "Pulmonary embolism", "1", "20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# DVT
codelist_DVT = spark.createDataFrame(
  [
    ("DVT","SNOMED","128053003","Deep venous thrombosis (disorder)","1",""),
    ("DVT","ICD10","I80","Phlebitis and thrombophlebitis","1","20210127"),
    ("DVT","SNOMED","978441000000108","Provoked deep vein thrombosis (disorder)","1",""),
    ("DVT","SNOMED","978421000000101","Unprovoked deep vein thrombosis (disorder)","1","")
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


# Intracranial Venous Thrombosis (ICVT)
codelist_ICVT = spark.createDataFrame(
  [
    ("ICVT","ICD10","G08","Intracranial venous thrombosis","1","20210127"),
    ("ICVT","ICD10","I67.6","Intracranial venous thrombosis","1","20210127"),
    ("ICVT","ICD10","I63.6","Intracranial venous thrombosis","1","20210127"),
    ("ICVT","ICD10","O22.5","Intracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
    ("ICVT","ICD10","O87.3","Intracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
    ("ICVT","SNOMED","297157005","Intracranial venous thrombosis","1","")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

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
    ("pericarditis","SNOMED","3238004","Pericarditis (disorder)","1",""),
    ("pericarditis","SNOMED","373945007","Pericardial effusion (disorder)","1",""),
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
    ("myocarditis","SNOMED","50920009","Myocarditis (disorder)","1",""),
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
    ("thrombocytopenia","ICD10","M31.1","Thrombotic microangiopathy","1","20210127"),
    ("thrombocytopenia","SNOMED","74576004","Acquired thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","439007008","Acquired thrombotic thrombocytopenic purpura","1",""),
    ("thrombocytopenia","SNOMED","28505005","Acute idiopathic thrombocytopenic purpura","1",""),
    ("thrombocytopenia","SNOMED","128091003","Autoimmune thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","13172003","Autoimmune thrombocytopenic purpura","1",""),
    ("thrombocytopenia","SNOMED","438476003","Autoimmune thrombotic thrombocytopenic purpura","1",""),
    ("thrombocytopenia","SNOMED","111588002","Heparin associated thrombotic thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","73397007","Heparin induced thrombocytopaenia","1",""),
    ("thrombocytopenia","SNOMED","438492008","Hereditary thrombocytopenic disorder","1",""),
    ("thrombocytopenia","SNOMED","441511006","History of immune thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","49341000119108","History of thrombocytopaenia",	"1",""),
    ("thrombocytopenia","SNOMED","726769004","HIT (Heparin induced thrombocytopenia) antibody","1",""),
    ("thrombocytopenia","SNOMED","371106008","Idiopathic maternal thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","32273002","Idiopathic thrombocytopenic purpura","1",""),
    ("thrombocytopenia","SNOMED","2897005","Immune thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","36070007","Immunodeficiency with thrombocytopenia AND eczema","1",""),
    ("thrombocytopenia","SNOMED","33183004","Post infectious thrombocytopenic purpura","1",""),
    ("thrombocytopenia","SNOMED","267534000","Primary thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","154826009","Secondary thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","866152006","Thrombocytopenia due to 2019 novel coronavirus","1",""),
    ("thrombocytopenia","SNOMED","82190001","Thrombocytopenia due to defective platelet production","1",""),
    ("thrombocytopenia","SNOMED","78345002","Thrombocytopenia due to diminished platelet production","1",""),
    ("thrombocytopenia","SNOMED","191323001","Thrombocytopenia due to extracorporeal circulation of blood","1",""),
    ("thrombocytopenia","SNOMED","87902006","Thrombocytopenia due to non-immune destruction","1",""),
    ("thrombocytopenia","SNOMED","302873008","Thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","417626001","Thrombocytopenic purpura associated with metabolic disorder","1",""),
    ("thrombocytopenia","SNOMED","402653004","Thrombocytopenic purpura due to defective platelet production","1",""),
    ("thrombocytopenia","SNOMED","402654005","Thrombocytopenic purpura due to platelet consumption","1",""),
    ("thrombocytopenia","SNOMED","78129009","Thrombotic thrombocytopenic purpura","1","20210127"),
    ("thrombocytopenia","SNOMED","441322009","Drug induced thrombotic thrombocytopenic purpura","1",""),
    ("thrombocytopenia","SNOMED","19307009","Drug-induced immune thrombocytopenia","1",""),
    ("thrombocytopenia","SNOMED","783251006","Hereditary thrombocytopenia with normal platelets","1",""),
    ("thrombocytopenia","SNOMED","191322006","Thrombocytopenia caused by drugs","1","")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

# MAGIC %md ## 2.7 Inflammatory conditions

# COMMAND ----------

# Mucocutaneous lymph node syndrome - Kawasaki (Kawasaki)
codelist_Kawasaki = spark.createDataFrame(
  [
    ("Kawasaki","SNOMED","75053002","Acute febrile mucocutaneous lymph node syndrome (disorder)","1",""),
    ("Kawasaki","ICD10","M30.3","Mucocutaneous lymph node syndrome - Kawasaki","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# COMMAND ----------

#Systemic inflammatory response syndrome associated with disease caused by SARS-CoV-2
codelist_SIRS = spark.createDataFrame(
  [
    ("SIRS","SNOMED","238149007","Systemic inflammatory response syndrome","1",""),
    ("SIRS","ICD10","R65","Systemic inflammatory response syndrome","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)
# Multisystem inflammatory syndrome in children  (MSIS)
codelist_MSIS = spark.createDataFrame(
  [
    ("MSIS","SNOMED","895448002","Multisystem inflammatory syndrome in children","1",""),
    ("MSIS","ICD10","U10.9","Emergency use code for Multisystem inflammatory syndrome associated with COVID-19","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

# Paediatric Inflammatory Multisystem Syndrome (PIMS)
codelist_PIMS = spark.createDataFrame(
  [
    ("PIMS","ICD10","U07.5","Systemic inflammatory response syndrome","1","20210127")
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)


# COMMAND ----------

 # Fracture of lower limb
codelist_fracture = spark.createDataFrame(
  [
('fracture','SNOMED','371162008','Closed fracture of skull (disorder)','1',''),
('fracture','SNOMED','81639003','Closed fracture of nasal bones (disorder)','1',''),
('fracture','SNOMED','430984009','Closed fracture of facial bone (disorder)','1',''),
('fracture','SNOMED','66112004','Closed fracture of sternum (disorder)','1',''),
('fracture','SNOMED','60667009','Closed fracture of rib (disorder)','1',''),
('fracture','SNOMED','269062008','Closed fracture of cervical spine (disorder)','1',''),
('fracture','SNOMED','207938004','Closed fracture thoracic vertebra (disorder)','1',''),
('fracture','SNOMED','207957008','Closed fracture lumbar vertebra (disorder)','1',''),
('fracture','SNOMED','13695006','Fracture of pubic rami (disorder)','1',''),
('fracture','SNOMED','207974008','Closed fracture sacrum (disorder)','1',''),
('fracture','SNOMED','766775007','Closed fracture of coccyx (disorder)','1',''),
('fracture','SNOMED','91037003','Closed fracture of pelvis (disorder)','1',''),
('fracture','SNOMED','33173003','Closed fracture of clavicle (disorder)','1',''),
('fracture','SNOMED','29749002','Closed fracture of scapula (disorder)','1',''),
('fracture','SNOMED','43295006','Closed fracture of humerus (disorder)','1',''),
('fracture','SNOMED','58580000','Closed supracondylar fracture of humerus (disorder)','1',''),
('fracture','SNOMED','302222008','Elbow fracture - closed (disorder)','1',''),
('fracture','SNOMED','111640008','Closed fracture of radius (disorder)','1',''),
('fracture','SNOMED','71555008','Closed fracture of ulna (disorder)','1',''),
('fracture','SNOMED','53627009','Closed fracture of radius AND ulna (disorder)','1',''),
('fracture','SNOMED','29045004','Closed Monteggias fracture (disorder)','1',''),
('fracture','SNOMED','208322000','Closed Galeazzi fracture (disorder)','1',''),
('fracture','SNOMED','208371005','Closed fracture scaphoid, waist, transverse (disorder)','1',''),
('fracture','SNOMED','9468002','Closed fracture of carpal bone (disorder)','1',''),
('fracture','SNOMED','208394006','Closed fracture of metacarpal bone (disorder)','1',''),
('fracture','SNOMED','208403005','Closed fracture of thumb metacarpal (disorder)','1',''),
('fracture','SNOMED','704213001','Closed fracture of phalanx of thumb (disorder)','1',''),
('fracture','SNOMED','24424003','Closed fracture of phalanx of finger (disorder)','1',''),
('fracture','SNOMED','359817006','Closed fracture of hip (disorder)','1',''),
('fracture','SNOMED','25415003','Closed fracture of femur (disorder)','1',''),
('fracture','SNOMED','428151000','Closed fracture of bone of knee joint (disorder)','1',''),
('fracture','SNOMED','80756009','Closed fracture of patella (disorder)','1',''),
('fracture','SNOMED','447139008','Closed fracture of tibia (disorder)','1',''),
('fracture','SNOMED','447395005','Closed fracture of fibula (disorder)','1',''),
('fracture','SNOMED','413877007','Closed fracture of tibia AND fibula (disorder)','1',''),
('fracture','SNOMED','42188001','Closed fracture of ankle (disorder)','1',''),
('fracture','SNOMED','64665009','Closed fracture of calcaneus (disorder)','1',''),
('fracture','SNOMED','342070009','Closed fracture of foot (disorder)','1',''),
('fracture','SNOMED','81576005','Closed fracture of phalanx of foot (disorder)','1',''),
('fracture','SNOMED','371161001','Open fracture of skull (disorder)','1',''),
('fracture','SNOMED','111609001','Open fracture of facial bones (disorder)','1',''),
('fracture','SNOMED','40613008','Open fracture of nasal bones (disorder)','1',''),
('fracture','SNOMED','87225004','Open fracture of sternum (disorder)','1',''),
('fracture','SNOMED','45910007','Open fracture of rib (disorder)','1',''),
('fracture','SNOMED','269070003','Open fracture of cervical spine (disorder)','1',''),
('fracture','SNOMED','207949005','Open fracture thoracic vertebra (disorder)','1',''),
('fracture','SNOMED','207965006','Open fracture lumbar vertebra (disorder)','1',''),
('fracture','SNOMED','207977001','Open fracture sacrum (disorder)','1',''),
('fracture','SNOMED','767262002','Open fracture of coccyx (disorder)','1',''),
('fracture','SNOMED','15474008','Open fracture of pelvis (disorder)','1',''),
('fracture','SNOMED','111637008','Open fracture of clavicle (disorder)','1',''),
('fracture','SNOMED','47864008','Open fracture of scapula (disorder)','1',''),
('fracture','SNOMED','89294002','Open fracture of humerus (disorder)','1',''),
('fracture','SNOMED','62356006','Open supracondylar fracture of humerus (disorder)','1',''),
('fracture','SNOMED','302232001','Elbow fracture - open (disorder)','1',''),
('fracture','SNOMED','42945005','Open fracture of radius (disorder)','1',''),
('fracture','SNOMED','37449000','Open fracture of ulna (disorder)','1',''),
('fracture','SNOMED','81966000','Open fracture of radius AND ulna (disorder)','1',''),
('fracture','SNOMED','34578006','Open Monteggias fracture (disorder)','1',''),
('fracture','SNOMED','208341002','Open Galeazzi fracture (disorder)','1',''),
('fracture','SNOMED','29014003','Open fracture of carpal bone (disorder)','1',''),
('fracture','SNOMED','1370007','Open fracture of metacarpal bone (disorder)','1',''),
('fracture','SNOMED','208420009','Open fracture of thumb metacarpal (disorder)','1',''),
('fracture','SNOMED','704236005','Open fracture of phalanx of thumb (disorder)','1',''),
('fracture','SNOMED','21698002','Open fracture of phalanx of finger (disorder)','1',''),
('fracture','SNOMED','361118003','Open fracture of hip (disorder)','1',''),
('fracture','SNOMED','28576007','Open fracture of femur (disorder)','1',''),
('fracture','SNOMED','428019004','Open fracture of bone of knee joint (disorder)','1',''),
('fracture','SNOMED','111643005','Open fracture of patella (disorder)','1',''),
('fracture','SNOMED','446979005','Open fracture of tibia (disorder)','1',''),
('fracture','SNOMED','447017008','Open fracture of fibula (disorder)','1',''),
('fracture','SNOMED','414943006','Open fracture of tibia AND fibula (disorder)','1',''),
('fracture','SNOMED','48187004','Open fracture of ankle (disorder)','1',''),
('fracture','SNOMED','24948002','Open fracture of calcaneus (disorder)','1',''),
('fracture','SNOMED','367527001','Open fracture of foot (disorder)','1',''),
('fracture','SNOMED','74395007','Open fracture of phalanx of foot (disorder)','1',''),
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

display(codelist_fracture)

# COMMAND ----------

codelists = [codelist_AMI, codelist_stroke, codelist_other_arterial_embolism, 
            codelist_PE,codelist_DVT, codelist_DVT_other, codelist_ICVT, codelist_PVT, 
            codelist_thrombocytopenia, 
            codelist_pericarditis, codelist_myocarditis, 
            codelist_Kawasaki, codelist_PIMS, codelist_SIRS, codelist_MSIS,
            codelist_fracture]

# COMMAND ----------

# append (union) codelists defined above
# harmonise columns before appending
clistm = []
for indx, clist in enumerate([clist for clist in globals().keys() if (bool(re.match('^codelist_.*', clist))) & (bool(re.match('^codelist_match.*', clist)) == False)]):
  print(f'{0 if indx<10 else ""}' + str(indx) + ' ' + clist)
  tmp = globals()[clist]
  if(indx == 0):
    clistm = tmp
  else:
    # pre unionByName
    for col in [col for col in tmp.columns if col not in clistm.columns]:
      print('  M - adding column: ' + col)
      clistm = clistm.withColumn(col, f.lit(None))
    for col in [col for col in clistm.columns if col not in tmp.columns]:
      print('  C - adding column: ' + col)
      tmp = tmp.withColumn(col, f.lit(None))
    clistm = clistm.unionByName(tmp)
  
clistm = clistm\
  .orderBy('name', 'terminology', 'code')

# COMMAND ----------

# check
display(clistm)

# COMMAND ----------

# codelists = [codelist_AMI, codelist_stroke, codelist_other_arterial_embolism, 
#             codelist_PE,codelist_DVT, codelist_DVT_other, codelist_ICVT, codelist_PVT, 
#             codelist_thrombocytopenia, 
#             codelist_pericarditis, codelist_myocarditis, 
#             codelist_Kawasaki, codelist_PIMS, codelist_SIRS, codelist_MSIS,
#             codelist_fracture]

#clist_outcome = reduce(DataFrame.unionAll, codelists)

clistm = clistm\
  .withColumn('Disease_group',\
    f.when(f.col('name').isin(['AMI', 'stroke', 'other_arterial_embolism']), 'Arterial')\
    .when(f.col('name').isin(['PE', 'DVT', 'DVT_other', 'ICVT', 'PVT']), 'Venous')\
    .when(f.col('name').isin(['thrombocytopenia']), 'Thrombocytopenia')\
    .when(f.col('name').isin(['pericarditis', 'myocarditis']), 'Myocarditis/Pericarditis')\
    .when(f.col('name').isin(['Kawasaki', 'PIMS', 'SIRS', 'MSIS']), 'Inflammatory')\
    .when(f.col('name').isin(['fracture']), 'Lower limb fracture')\
  )

# # reorder columns
# vlist_ordered = ['Disease_group']
# vlist_unordered = [v for v in clist_outcome.columns if v not in vlist_ordered]
# vlist_all = vlist_ordered + vlist_unordered
# clist_outcome = clist_outcome\
#   .select(*vlist_all)

# tmpt = tab(clist_outcome, 'name', 'Disease_group', var2_unstyled=1)

# COMMAND ----------

# check
display(clistm.where((f.col('terminology') == 'ICD10') & (f.col('code').rlike('X$'))))

# COMMAND ----------

display(clistm.where((f.col('terminology') == 'ICD10') & (f.col('code').rlike('[\.\-\s]'))))

# COMMAND ----------

# MAGIC %md # 4 Reformat

# COMMAND ----------

# check
tmpt = tab(clistm, 'name', 'terminology', var2_unstyled=1)

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
clistmr = clistm\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))

# COMMAND ----------

# MAGIC %md # 5 Checks

# COMMAND ----------

for terminology in ['ICD10', 'SNOMED']:
  print(terminology)
  ctmp = clistmr\
    .where(f.col('terminology') == terminology)
  count_var(ctmp, 'code'); print()

# COMMAND ----------

for terminology in ['ICD10']:
  print(terminology)
  ctmp = clistmr\
    .where(f.col('terminology') == terminology)
  count_var(ctmp, 'code'); print()

# COMMAND ----------

# MAGIC %md # 6 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_ecds_outcome_all'

# save
clistmr.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

display(clistmr)
