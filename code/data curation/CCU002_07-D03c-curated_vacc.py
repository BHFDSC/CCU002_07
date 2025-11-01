# Databricks notebook source
# MAGIC %md # CCU002_07-D03c-curated_vacc
# MAGIC
# MAGIC **Description** This notebook provides a part-curated and streamlined version of the vaccination_status dataset with an accompanying quality assurance table. Minimal exclusions have been applied as the criteria may differ according to the given project. For example, duplicates on PERSON_ID and DATE and duplicates on PERSON_ID and PROCEDURE have been flagged, but not excluded. One may opt for a brute force approach for exluding these records, say keeping the earliest record for duplicates on PERSON_ID and PROCEDURE (recognising and accepting that this may not be optimal for all instances), or one may choose to apply a more complex algorithmic approach to identify the most appropriate record with reference to other records for the individual. However, the latter approach is more time consuming and may still not provide a satisfactory result for the relatively small number of duplicate records identified. The dose sequence and quality assurance table will need to be updated following any additional exclusions. Exclusions, flagged possible exclusions, and quality checks are inline with the data cleaning performed in the SAIL Databank (see below for further details). 
# MAGIC
# MAGIC
# MAGIC **Author(s)** Tom Bolton, Alexia Sampri
# MAGIC
# MAGIC **Reviewers** Wen Shi
# MAGIC

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

vacc = extract_batch_from_archive(parameters_df_datasets, 'vacc')

# COMMAND ----------

# MAGIC %md # 2 Look-up tables

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('VACCINE_PRODUCT_CODE')
print('---------------------------------------------------------------------------------')
# created using https://termbrowser.nhs.uk/?
lookup_vaccine_product_code = """
VACCINE_PRODUCT_CODE,PRODUCT,PRODUCT_ADD
39114911000001105,AstraZeneca,
39115011000001105,AstraZeneca,8 dose
39115111000001106,AstraZeneca,10 dose
40402411000001109,AstraZeneca,

39115611000001103,Pfizer,
39115711000001107,Pfizer,6 dose
40851611000001102,Pfizer,
40556911000001102,Pfizer,
40384611000001108,Pfizer child,Children 5-11

39326911000001101,Moderna,
40801911000001102,Moderna,
40520411000001107,Moderna,
39375411000001104,Moderna,10 dose

39230211000001104,Janssen-Cilag,

39373511000001104,Valneva,
40306411000001101,Sinovac Life Sciences,CoronaVac
39473011000001103,Novavax CZ,Nuvaxovid
40483111000001109,Serum Institute of India,
40348011000001102,Serum Institute of India,
40331611000001100,Beijing Institute of Biological Products,
40332311000001101,Bharat Biotech International Ltd,
39826711000001101,Medicago
36509011000001106,Seqirus UK Ltd,Flucelvax Tetra
38973211000001108,Seqirus UK Ltd,Fluad Tetra
40387411000001100,Gamaleya NRCEM,Sputnik V
40387711000001106,Gamaleya NRCEM,Sputnik V
40712911000001109,CanSino Biologics Inc,Convidecia
40506311000001105,Sanofi,

1362591000000103,Unknown,PROCEDURE_CODE(B)
"""
lookup_vaccine_product_code = (spark.createDataFrame(
  pd.DataFrame(pd.read_csv(io.StringIO(lookup_vaccine_product_code)))
  .fillna('')
  .astype(str)
))

# concat desc
lookup_vaccine_product_code = lookup_vaccine_product_code\
  .withColumn('PRODUCT_ADD',\
    f.when(f.col('PRODUCT_ADD') != '', f.concat(f.lit(' - '), f.col('PRODUCT_ADD')))\
    .otherwise(f.col('PRODUCT_ADD'))\
  )\
  .withColumn('PRODUCT_CONCAT', f.concat_ws('', f.col('PRODUCT'), f.col('PRODUCT_ADD')))\
  .select('VACCINE_PRODUCT_CODE', 'PRODUCT')

# cache
lookup_vaccine_product_code.cache().count()

# check
assert lookup_vaccine_product_code.count() == lookup_vaccine_product_code.select('VACCINE_PRODUCT_CODE').distinct().count()
print(lookup_vaccine_product_code.toPandas().to_string()); print()


print('---------------------------------------------------------------------------------')
print('VACCINATION_PROCEDURE_CODE')
print('---------------------------------------------------------------------------------')
# 1324681000000101 'first dose'  - 'Administration of first dose of severe acute respiratory syndrome coronavirus 2 vaccine (procedure)'
# 1324691000000104 'second dose' - 'Administration of second dose of severe acute respiratory syndrome coronavirus 2 vaccine (procedure)'
# 1362591000000103 'booster'     - 'Immunisation course to maintain protection against severe acute respiratory syndrome coronavirus 2 (regime/therapy)'
# 1324671000000103 ?             - 'Immunisation course to achieve immunity against severe acute respiratory syndrome coronavirus 2 (regime/therapy)' N=negligible

# note: CCU002_06 (CCU002_03-D05 notebook) mentions that 1324671000000103 is 'third dose', but as there are <20 out of 143m records, we ignore these...

lookup_vaccine_procedure_code = """
VACCINATION_PROCEDURE_CODE,PROCEDURE
1324681000000101,1
1324691000000104,2
1362591000000103,B
1324671000000103,Unknown
"""
lookup_vaccine_procedure_code = (spark.createDataFrame(
  pd.DataFrame(pd.read_csv(io.StringIO(lookup_vaccine_procedure_code)))
  .fillna('')
  .astype(str)
))

# cache
lookup_vaccine_procedure_code.cache().count()

# check
assert lookup_vaccine_procedure_code.count() == lookup_vaccine_procedure_code.select('VACCINATION_PROCEDURE_CODE').distinct().count()
print(lookup_vaccine_procedure_code.toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 3 Check

# COMMAND ----------

display(vacc.orderBy('PERSON_ID_DEID', 'DATE_AND_TIME', 'DOSE_SEQUENCE'))

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('Shape')
print('---------------------------------------------------------------------------------')
count_var(vacc, 'PERSON_ID_DEID'); print()
print(len(vacc.columns)); print()
print(pd.DataFrame({f'_cols': vacc.columns}).to_string()); print()


print('---------------------------------------------------------------------------------')
print('PERSON_ID_DEID row number')
print('---------------------------------------------------------------------------------')
_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy('DATE_AND_TIME')
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID')
_vacc = (
  vacc
  .withColumn('_rownum', f.row_number().over(_win_rownum))
  .withColumn('_rownummax', f.count(f.lit(1)).over(_win_rownummax))
)      

# check
tmpt = tab(_vacc.where(f.col('_rownum') == 1), '_rownummax'); print()


print('---------------------------------------------------------------------------------')
print('PERSON_ID_DEID, DATE row number')
print('---------------------------------------------------------------------------------')
_vacc = (
  vacc
  .withColumn('DATE', f.to_date(f.substring(f.col('DATE_AND_TIME'), 1, 8), 'yyyyMMdd'))
)

# check
count_varlist(_vacc, ['PERSON_ID_DEID', 'DATE']); print()

_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID', 'DATE')\
  .orderBy('DATE_AND_TIME')
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID', 'DATE')
_vacc = (
  _vacc
  .withColumn('_rownum', f.row_number().over(_win_rownum))
  .withColumn('_rownummax', f.count(f.lit(1)).over(_win_rownummax))
)

# check
tmpt = tab(_vacc.where(f.col('_rownum') == 1), '_rownummax'); print()
         
     
print('---------------------------------------------------------------------------------')
print('UNIQUE_ID row number')
print('---------------------------------------------------------------------------------')
# check 
count_var(vacc, 'UNIQUE_ID'); print()       

_win_rownum = Window\
  .partitionBy('UNIQUE_ID')\
  .orderBy('PERSON_ID_DEID')
_win_rownummax = Window\
  .partitionBy('UNIQUE_ID')

_vacc = (
  vacc
  .withColumn('_rownum', f.row_number().over(_win_rownum))
  .withColumn('_rownummax', f.count(f.lit(1)).over(_win_rownummax))
)

# check
tmpt = tab(_vacc.where(f.col('_rownum') == 1), '_rownummax'); print()
# => UNIQUE_ID is not unique


print('--------------------------------------------------------------------------------')
print('check single value data fields')
print('--------------------------------------------------------------------------------')
tmpt = tab(vacc, 'PRIMARY_SOURCE'); print()
tmpt = tab(vacc, 'NOT_GIVEN'); print()
tmpt = tab(vacc, 'TRACE_VERIFIED'); print()
assert vacc.select('PRIMARY_SOURCE').distinct().count() == 1
assert vacc.select('NOT_GIVEN').distinct().count() == 1
assert vacc.select('TRACE_VERIFIED').distinct().count() == 1
# => no need to keep these columns with a single value


print('--------------------------------------------------------------------------------')
print('check format of DATE_AND_TIME before below substring and reformat')
print('--------------------------------------------------------------------------------')
tmpf = (vacc
  .withColumn('_chk', 
              f.when(f.col('DATE_AND_TIME').rlike(r'^\d{8}T\d{6}00$'), f.lit(1))
              .when(f.col('DATE_AND_TIME').isNull(), f.lit(2))
              .otherwise(0)))
tmpt = tab(tmpf, '_chk'); print()
#assert tmpf.select('_chk').where(f.col('_chk') == 0).count() == 0


print('--------------------------------------------------------------------------------')
print('check DOSE_SEQUENCE')
print('--------------------------------------------------------------------------------')
tmpt = tab(vacc, 'DOSE_SEQUENCE'); print()
tmpf = (
  vacc
  .withColumn('_chk', 
              f.when(f.col('DOSE_SEQUENCE').isin([1,2]), f.lit(1))
              .when(f.col('DOSE_SEQUENCE').isNull(), f.lit(2))
              .otherwise(0)
             )
) 
tmpt = tab(tmpf, 'DOSE_SEQUENCE', '_chk'); print()
#assert tmpf.select('_chk').where(f.col('_chk') == 0).count() == 0


print('--------------------------------------------------------------------------------')
print('check VACCINATION_PROCEDURE_CODE')
print('--------------------------------------------------------------------------------')
# see lookup table above
tmpt = tab(vacc, 'VACCINATION_PROCEDURE_CODE'); print()

# check 'dose 3' codes are insignificant
assert vacc.where(f.col('VACCINATION_PROCEDURE_CODE') == '1324671000000103').count() < 50

# check that VACCINATION_PROCEDURE_CODE only takes values defined above
tmpg = lookup_vaccine_procedure_code\
  .select('VACCINATION_PROCEDURE_CODE')\
  .toPandas()
tmpl = list(tmpg['VACCINATION_PROCEDURE_CODE'])
print(tmpl); print()
tmpf = (
  vacc
  .withColumn('_chk', 
              f.when(f.col('VACCINATION_PROCEDURE_CODE').isin(tmpl), f.lit(1))
              .when(f.col('VACCINATION_PROCEDURE_CODE').isNull(), f.lit(2))
              .otherwise(0)
             )
) 
tmpt = tab(tmpf, 'VACCINATION_PROCEDURE_CODE', '_chk'); print()
#assert tmpf.select('_chk').where(f.col('_chk') == 0).count() == 0

# check cross-tabulation with DOSE_SEQUENCE
tmpt = tab(vacc, 'VACCINATION_PROCEDURE_CODE', 'DOSE_SEQUENCE'); print()


print('--------------------------------------------------------------------------------')
print('check VACCINE_PRODUCT_CODE')
print('--------------------------------------------------------------------------------')
# see lookup table above
tmpt = tab(vacc, 'VACCINE_PRODUCT_CODE'); print()

# check that VACCINE_PRODUCT_CODE only takes values defined above
tmpg = lookup_vaccine_product_code\
  .select('VACCINE_PRODUCT_CODE')\
  .toPandas()
tmpl = list(tmpg['VACCINE_PRODUCT_CODE'])
print(tmpl); print()
tmpf = (
  vacc
  .withColumn('_chk', 
              f.when(f.col('VACCINE_PRODUCT_CODE').isin(tmpl), f.lit(1))
              .when(f.col('VACCINE_PRODUCT_CODE').isNull(), f.lit(2))
              .otherwise(0)
             )
) 
tmpt = tab(tmpf, 'VACCINE_PRODUCT_CODE', '_chk'); print()
#assert tmpf.select('_chk').where(f.col('_chk') == 0).count() == 0

# 20230126 check that the number of records with an invalid vaccine product code (= vaccine procedure code [booster]) are minimal
assert vacc.where(f.col('VACCINE_PRODUCT_CODE') == '1362591000000103').count() < 10


print('--------------------------------------------------------------------------------')
print('check VACCINATION_SITUATION_CODE')
print('--------------------------------------------------------------------------------')
# 1324741000000101 'first dose declined' 
# 1324751000000103 'second dose declined' - 'Severe acute respiratory syndrome coronavirus 2 vaccination second dose declined (situation)'
tmpt = tab(vacc, 'VACCINATION_SITUATION_CODE'); print()

# check that VACCINATION_SITUATION_CODE only takes known values 
tmpf = (
  vacc
  .withColumn('_chk', 
              f.when(f.col('VACCINATION_SITUATION_CODE').isin(['1324751000000103']), f.lit(1))
              .when(f.col('VACCINATION_SITUATION_CODE').isNull(), f.lit(2))
              .otherwise(0)
             )  
)
tmpt = tab(tmpf, 'VACCINATION_SITUATION_CODE', '_chk'); print()
#assert tmpf.where(f.col('_chk') == 0).count() == 0


print('--------------------------------------------------------------------------------')
print('check REASON_NOT_GIVEN_CODE')
print('--------------------------------------------------------------------------------')
# 1324721000000108 'Severe acute respiratory syndrome coronavirus 2 vaccination dose declined (situation)'
# 1324731000000105 'Severe acute respiratory syndrome coronavirus 2 immunisation course not indicated (situation)'
# 1324761000000100 'Severe acute respiratory syndrome coronavirus 2 immunisation course contraindicated (situation)'
# 1324861000000109 'Severe acute respiratory syndrome coronavirus 2 immunisation course abandoned (situation)'
# 213257006 'Generally unwell (finding)'
# 310376006 'Immunization consent not given (finding)'
tmpt = tab(vacc, 'REASON_NOT_GIVEN_CODE'); print()

# check that VACCINATION_SITUATION_CODE only takes known values 
tmpf = (
  vacc
  .withColumn('_chk', 
              f.when(f.col('REASON_NOT_GIVEN_CODE').isin([
                '1324721000000108'
              , '1324731000000105'
              , '1324761000000100'
              , '1324861000000109'
              , '213257006'
              , '310376006']), f.lit(1))
            .when(f.col('REASON_NOT_GIVEN_CODE').isNull(), f.lit(2))
            .otherwise(0)
             )
)
tmpt = tab(tmpf, 'REASON_NOT_GIVEN_CODE', '_chk'); print()
#assert tmpf.where(f.col('_chk') == 0).count() == 0


print('--------------------------------------------------------------------------------')
print('check cross-tabulations')
print('--------------------------------------------------------------------------------')
tmpt = tab(vacc, 'REASON_NOT_GIVEN_CODE', 'VACCINATION_SITUATION_CODE'); print()
tmpt = tab(vacc, 'DOSE_SEQUENCE', 'VACCINATION_SITUATION_CODE'); print()
tmpt = tab(vacc, 'VACCINATION_PROCEDURE_CODE', 'VACCINATION_SITUATION_CODE'); print()

# checked VACCINATION_SITUATION_CODE with Data Wranglers, since NOT_GIVEN==FALSE for all (see "Exclude not given records" subsection below)...
# checking REASON_NOT_GIVEN_CODE

print('--------------------------------------------------------------------------------')
print('check NHS_NUMBER_STATUS_INDICATOR_CODE')
print('--------------------------------------------------------------------------------')
tmpt = tab(vacc, 'NHS_NUMBER_STATUS_INDICATOR_CODE'); print()

# COMMAND ----------

tmpj1 = (
  vacc
  .where(f.col('VACCINE_PRODUCT_CODE') == '1362591000000103')
  .toPandas()
  .T
  .reset_index()
)
print(tmpj1.to_string())

# COMMAND ----------

display(vacc.where(f.col('PERSON_ID_DEID') == '29BWAP6O7HNQBPL'))

# COMMAND ----------

# MAGIC %md ## 3.1 Check date

# COMMAND ----------

# reformat
_vacc = (
  vacc
  .withColumn('DATE', f.to_date(f.substring(f.col('DATE_AND_TIME'), 1, 8), 'yyyyMMdd'))
  .withColumn('RECORDED_DATE', f.to_date(f.col('RECORDED_DATE'), 'yyyyMMdd'))  
)

print('---------------------------------------------------------------------------------')
print('check DATE (derived from DATE_AND_TIME) and RECORDED_DATE')
print('---------------------------------------------------------------------------------')
tmpt = tabstat(_vacc, 'DATE', date=1); print()
tmpt = tabstat(_vacc, 'RECORDED_DATE', date=1); print()


print('---------------------------------------------------------------------------------')
print('compare DATE (derived from DATE_AND_TIME) with RECORDED_DATE')
print('---------------------------------------------------------------------------------')
# DATE_AND_TIME	- The date and time on which the vaccination intervention was carried out or was meant to be administered
# RECORDED_DATE	- The date that the vaccination administered (procedure) or not administered (situation) was recorded in the source system
_vacc = (
  _vacc
  .withColumn('_DATE_notNull', f.when(f.col('DATE').isNotNull(), 1).otherwise(0))
  .withColumn('_RECORDED_DATE_notNull', f.when(f.col('RECORDED_DATE').isNotNull(), 1).otherwise(0))
  .withColumn('_DATE_lt_20201208', f.when(f.col('DATE') < f.to_date(f.lit('2020-12-08')), 1).otherwise(0))
  .withColumn('_RECORDED_DATE_lt_20201208', f.when(f.col('RECORDED_DATE') < f.to_date(f.lit('2020-12-08')), 1).otherwise(0))
  .withColumn('_diff', f.datediff(f.col('DATE'), f.col('RECORDED_DATE'))))

# check
tmpt = tab(_vacc, '_DATE_notNull', '_RECORDED_DATE_notNull'); print()
tmpt = tab(_vacc, '_DATE_lt_20201208', '_RECORDED_DATE_lt_20201208'); print()
tmpt = tab(_vacc.where(f.col('_DATE_lt_20201208') != f.col('_RECORDED_DATE_lt_20201208')), '_DATE_notNull', '_RECORDED_DATE_notNull'); print()
tmpt = tabstat(_vacc, '_diff'); print()
tmpt = tab(_vacc, '_diff'); print()

# 20221215 summary
# DATE provides 20k values when RECORDED_DATE is missing
# DATE contains 11k erroneous dates when both DATE and RECORDED_DATE exist
# DATE and RECORDED_DATE have 92.7% same day agreement, 97.7% within 1 week
# DATE is almost always before RECORDED_DATE (suggesting that RECORDED_DATE was the date the vaccination was later recorded in the system?)

# => pragmatic approach whilst waiting for the Data Wranglers to confirm the appropriate use of date fields with the asset owner
#    use DATE as primary source 
#    fill in with RECORDED_DATE where DATE is missing
#    replace DATE with RECORDED_DATE for 11k DATE that are less than 20201208

# COMMAND ----------

# add histogram when time

# display(vacc.where(f.col('PERSON_ID_DEID') == 'TFTCOPBENBN6FDD').orderBy('DATE_AND_TIME'))

# COMMAND ----------

# MAGIC %md # 4 Prepare

# COMMAND ----------

# MAGIC %md ## 4.1 Select and reformat required columns

# COMMAND ----------

# select columns needed below 
# reformat date columns
tmpp1 = (
  vacc
  .withColumn('DATE', f.to_date(f.substring(f.col('DATE_AND_TIME'), 1, 8), 'yyyyMMdd'))
  .withColumn('TIME', f.substring(f.col('DATE_AND_TIME'), 10, 15))
  .withColumn('RECORDED_DATE', f.to_date(f.col('RECORDED_DATE'), 'yyyyMMdd'))  
  .select(f.col('PERSON_ID_DEID').alias('PERSON_ID'), 'DATE_AND_TIME', 'DATE', 'TIME', 'RECORDED_DATE', 'DOSE_SEQUENCE', 'VACCINATION_PROCEDURE_CODE', 'VACCINE_PRODUCT_CODE', 'REASON_NOT_GIVEN_CODE', 'VACCINATION_SITUATION_CODE', 'archived_on')
  .orderBy('PERSON_ID', 'DATE', 'TIME', 'DOSE_SEQUENCE'))

# temp save (with partitionBy for later join)
outName = f'{proj}_tmp_vacc_tmpp1'.lower()
tmpp1.write.partitionBy('VACCINE_PRODUCT_CODE').mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmpp1 = spark.table(f'{dbc}.{outName}')

# check
count_var(tmpp1, 'PERSON_ID'); print()

# COMMAND ----------

# check
display(tmpp1.orderBy('PERSON_ID', 'DATE', 'TIME'))

# COMMAND ----------

# MAGIC %md ## 4.2 Label code columns

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('VACCINE_PRODUCT_CODE')
print('---------------------------------------------------------------------------------')
# add simplified vaccination product names

# skewed join - increase number of partitions by also joining on integer 0-9
# https://stackoverflow.com/questions/68899346/slow-join-in-pyspark-tried-repartition
n = 10

# prepare lookup
_lookup_vaccine_product_code = (
  lookup_vaccine_product_code
  .crossJoin(spark.range(0, n).withColumnRenamed('id', '_tmp'))
)

# check
tmpt = tab(_lookup_vaccine_product_code, 'VACCINE_PRODUCT_CODE', '_tmp'); print()

# prepare vacc
_tmpp1 = (
  tmpp1
  .withColumn('_tmp', (f.rand() * n).cast("int"))
)

# check
tmpt = tab(_tmpp1, 'VACCINE_PRODUCT_CODE', '_tmp'); print()

# merge vacc and lookup
# tidy
tmpp2 = (
  merge(_tmpp1, _lookup_vaccine_product_code, ['VACCINE_PRODUCT_CODE', '_tmp'], broadcast_right=1, validate='m:1', keep_results=['both', 'left_only'], indicator=0)
  .drop('_tmp')
); print()

# temp save
tmpp2 = temp_save(df=tmpp2, out_name=f'{proj}_tmp_vacc_tmpp2'); print()

# check
tmpt = tab(tmpp2, 'PRODUCT'); print()
if(tmpp2.where(f.col('PRODUCT').isNull()).count() > 0):
  tmpt = tab(tmpp2.where(f.col('PRODUCT').isNull()), 'VACCINE_PRODUCT_CODE'); print()
  
  
print('---------------------------------------------------------------------------------')
print('VACCINATION_PROCEDURE_CODE')
print('---------------------------------------------------------------------------------')
# skewed join - see above
n = 50

# prepare lookup
_lookup_vaccine_procedure_code = (
  lookup_vaccine_procedure_code
  .crossJoin(spark.range(0, n).withColumnRenamed('id', '_tmp'))
)

# check
tmpt = tab(_lookup_vaccine_procedure_code, '_tmp', 'VACCINATION_PROCEDURE_CODE'); print()

# prepare vacc
_tmpp2 = (
  tmpp2
  .withColumn('_tmp', (f.rand() * n).cast("int"))
)

# check
tmpt = tab(_tmpp2, '_tmp', 'VACCINATION_PROCEDURE_CODE'); print()

# merge vacc and lookup
# tidy
tmpp3 = (
  merge(_tmpp2, _lookup_vaccine_procedure_code, ['VACCINATION_PROCEDURE_CODE', '_tmp'], broadcast_right=1, validate='m:1', keep_results=['both', 'left_only'], indicator=0)
  .drop('_tmp')
); print()

# temp save
tmpp3 = temp_save(df=tmpp3, out_name=f'{proj}_tmp_vacc_tmpp3'); print()

# check 
tmpt = tab(tmpp3, 'PROCEDURE'); print()
tmpt = tab(tmpp3, 'VACCINATION_PROCEDURE_CODE', 'PROCEDURE'); print()
tmpt = tab(tmpp3, 'DOSE_SEQUENCE'); print()
tmpt = tab(tmpp3, 'PROCEDURE', 'DOSE_SEQUENCE'); print()
  
# tidy  
tmpp4 = (
  tmpp3
  .select('PERSON_ID', 'DATE_AND_TIME', 'DATE', 'TIME', 'RECORDED_DATE', 'PROCEDURE', 'PRODUCT', 'VACCINATION_SITUATION_CODE', 'REASON_NOT_GIVEN_CODE', 'archived_on')
)

# temp save
tmpp4 = temp_save(df=tmpp4, out_name=f'{proj}_tmp_vacc_tmpp4'); print()

# check
count_var(tmpp4, 'PERSON_ID'); print()

# COMMAND ----------

# check
display(tmpp4.orderBy('PERSON_ID', 'DATE', 'TIME', 'PROCEDURE', 'PRODUCT'))

# COMMAND ----------

# MAGIC %md ## 4.3 Create date

# COMMAND ----------

# create date as per the check above
tmpp5 = (tmpp4
  .withColumn('_tmp', 
             f.when(f.col('DATE').isNull(), f.lit(1))
              .when((f.col('DATE') < f.to_date(f.lit('2020-12-08'))) & (f.col('RECORDED_DATE') >= f.to_date(f.lit('2020-12-08'))), f.lit(2))
              .otherwise(f.lit(3))
             )  
  .withColumn('DATE_cleaned', 
             f.when(f.col('DATE').isNull(), f.col('RECORDED_DATE'))
              .when((f.col('DATE') < f.to_date(f.lit('2020-12-08'))) & (f.col('RECORDED_DATE') >= f.to_date(f.lit('2020-12-08'))), f.col('RECORDED_DATE'))
              .otherwise(f.col('DATE'))
             ))
  
# check
tmpt = tab(tmpp5, '_tmp'); print()
tmpt = tabstat(tmpp5, 'DATE_cleaned', date=1); print()
count_var(tmpp5, 'PERSON_ID'); print()
count_varlist(tmpp5, ['PERSON_ID', 'DATE_cleaned']); print()
  
# tidy
tmpp5 = (
  tmpp5
  .select('PERSON_ID', f.col('DATE_cleaned').alias('DATE'), 'TIME', 'RECORDED_DATE', 'PROCEDURE', 'PRODUCT', 'VACCINATION_SITUATION_CODE', 'REASON_NOT_GIVEN_CODE', 'archived_on'))

# temp save
tmpp5 = temp_save(df=tmpp5, out_name=f'{proj}_tmp_vacc_tmpp5'); print()

# COMMAND ----------

# check
display(tmpp5.orderBy('PERSON_ID', 'DATE', 'TIME', 'PROCEDURE', 'PRODUCT'))

# COMMAND ----------

# MAGIC %md # 5 Exclusions

# COMMAND ----------

"""
1. Exclude any records with missing PERSON_ID or DATE
2. Exclude any records with an indication that the vaccination was not given 
3. Exclude any duplicate records on PERSON_ID, DATE, PROCEDURE, PRODUCT
"""

# COMMAND ----------

# checkpoint
tmpc1 = tmpp5 # spark.table(f'{dbc}.{proj}_tmp_vacc_tmpp5')

# --------------------------------------------------------------------------------
# limit the number of rows
# --------------------------------------------------------------------------------
# temporary step whilst developing
# _win_rownum = Window\
#   .partitionBy()\
#   .orderBy('PERSON_ID', 'DATE')
# tmpc1 = (
#   tmpc1
#   .withColumn('_rownum', f.row_number().over(_win_rownum))
#   .where(f.col('_rownum') <= 10000000)
#   .drop('_rownum'))

# COMMAND ----------

# MAGIC %md ## 5.1 Exclude null IDs and dates

# COMMAND ----------

# check
tmpc = count_var(tmpc1, 'PERSON_ID', ret=1, df_desc='original', indx=0); print()

# filter 
tmpc2 = (
  tmpc1
  .where(f.col('PERSON_ID').isNotNull())
  .where(f.col('DATE').isNotNull()))

# check 
tmpt = count_var(tmpc2, 'PERSON_ID', ret=1, df_desc='post exclusion of records with missing PERSON_ID or missing DATE', indx=1); print()
tmpc = tmpc.unionByName(tmpt)
print(tmpc2.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 5.2 Exclude not given records

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('VACCINATION_SITUATION_CODE')
print('---------------------------------------------------------------------------------')
# 20221215 Advised by trehelp@nhs.net to exclude records where VACCINATION_SITUATION_CODE == '1324751000000103' (not null)
# checked that VACCINATION_SITUATION_CODE only takes known values in checks above before filtering to keep null below
# filter
tmpc3 = tmpc2\
  .where(f.col('VACCINATION_SITUATION_CODE').isNull())


print('---------------------------------------------------------------------------------')
print('REASON_NOT_GIVEN_CODE')
print('---------------------------------------------------------------------------------')
# 20221215 TO CONFIRM - exclude records where REASON_NOT_GIVEN is not null
# checked that REASON_NOT_GIVEN_CODE only takes known values in checks above before filtering to keep null below
# check that this unconfirmed filtering is minimal
assert tmpc3.where(f.col('REASON_NOT_GIVEN_CODE').isNotNull()).count() < 200
# filter
tmpc3 = tmpc3\
  .where(f.col('REASON_NOT_GIVEN_CODE').isNull())


print('---------------------------------------------------------------------------------')
print('check')
print('---------------------------------------------------------------------------------')
# check
tmpt = tab(tmpc3, 'VACCINATION_SITUATION_CODE'); print()
tmpt = tab(tmpc3, 'REASON_NOT_GIVEN_CODE'); print()

# tidy
tmpc3 = tmpc3\
  .drop('VACCINATION_SITUATION_CODE', 'REASON_NOT_GIVEN_CODE')

# check
tmpt = count_var(tmpc3, 'PERSON_ID', ret=1, df_desc='post exclusion of not given [VACCINATION_SITUATION_CODE, REASON_NOT_GIVEN_CODE]', indx=2); print()
tmpc = tmpc.unionByName(tmpt)
print(tmpc3.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 5.3 Exclude duplicates on PERSON_ID, DATE, PROCEDURE, PRODUCT

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('duplicates on PERSON_ID, DATE, PRODUCT, PROCEDURE')
print('---------------------------------------------------------------------------------')
# i.e., the same information has been entered into the system more than once

# define windows
_win_rownum = Window\
  .partitionBy('PERSON_ID', 'DATE', 'PROCEDURE', 'PRODUCT')\
  .orderBy('TIME')
_win_rownummax = Window\
  .partitionBy('PERSON_ID', 'DATE', 'PROCEDURE', 'PRODUCT')
_win_rownum_PERSON_ID = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', 'PROCEDURE', 'PRODUCT', 'TIME')
_win_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', 'PROCEDURE', 'PRODUCT', 'TIME')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# add row number and maximum row number
tmpc3 = (tmpc3
        .withColumn('_rownum', f.row_number().over(_win_rownum))
        .withColumn('_rownummax', f.count('PERSON_ID').over(_win_rownummax))
        .withColumn('_PERSON_ID_rownum', f.row_number().over(_win_rownum_PERSON_ID))
        .withColumn('_PERSON_ID_egen', f.max(f.col('_rownummax')).over(_win_egen))
        .orderBy('PERSON_ID', 'DATE', 'PRODUCT', 'PROCEDURE', '_rownum')
       )

# check
tmpt = tab(tmpc3.where(f.col('_rownum') == 1), '_rownummax'); print()
tmpt = tab(tmpc3.where(f.col('_rownum') == 1), '_rownummax', 'PROCEDURE'); print()
tmpt = tab(tmpc3.where(f.col('_PERSON_ID_rownum') == 1), '_PERSON_ID_egen'); print()
# display(tmp2.where(f.col('_PERSON_ID_egen') > 1))
# display(tmpc3.where(f.col('_rownum') == 19)) # TFTCOPBENBN6FDD

# filter, tidy, add mono id
tmpc4 = (tmpc3
        .where(f.col('_rownum') == 1)
        .drop('_rownum', '_rownummax', '_PERSON_ID_rownum', '_PERSON_ID_egen')
        .withColumn('_mono_id', f.monotonically_increasing_id())
       )

# temp save
tmpc4 = temp_save(df=tmpc4, out_name=f'{proj}_tmp_vacc_tmpc4'); print()

# check
tmpt = count_var(tmpc4, 'PERSON_ID', ret=1, df_desc='post exclusion of duplicates on [PERSON_ID, DATE, PRODUCT, PROCEDURE]', indx=3); print()
tmpc = tmpc.unionByName(tmpt)
print(tmpc4.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 5.4 Flow diagram 

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

# MAGIC %md # 6 Flag possible exclusions

# COMMAND ----------

# MAGIC %md ## 6.1 Flag duplicates on PERSON_ID and DATE

# COMMAND ----------

# checkpoint
# tmpc4 = spark.table(f'{dbc}.{proj}_tmp_vacc_tmpc4')

# COMMAND ----------

# ordered to prioritise the most recent record by TIME, the latest vaccination (B, 2, 1), mono ID for stability of ordering in the event of ties

print('---------------------------------------------------------------------------------')
print('identify duplicates on PERSON_ID, DATE')
print('---------------------------------------------------------------------------------')
_win_PERSON_ID_DATE_ord = Window\
  .partitionBy('PERSON_ID', 'DATE')\
  .orderBy(f.desc('TIME'), f.desc('PROCEDURE'), '_mono_id')
_win_PERSON_ID_DATE = Window\
  .partitionBy('PERSON_ID', 'DATE')
_win_PERSON_ID_ord = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', f.desc('TIME'), f.desc('PROCEDURE'), '_mono_id')
_win_PERSON_ID_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', f.desc('TIME'), f.desc('PROCEDURE'), '_mono_id')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

tmpc4 = (
  tmpc4
  .withColumn('_PERSON_ID_DATE_rownum', f.row_number().over(_win_PERSON_ID_DATE_ord))
  .withColumn('_PERSON_ID_DATE_rownummax', f.count('PERSON_ID').over(_win_PERSON_ID_DATE))
  .withColumn('_PERSON_ID_rownum', f.row_number().over(_win_PERSON_ID_ord))
  .withColumn('_PERSON_ID_egen_rownummax', f.max(f.col('_PERSON_ID_DATE_rownummax')).over(_win_PERSON_ID_egen))
)

# check
tmpt = tab(tmpc4.where(f.col('_PERSON_ID_DATE_rownum') == 1), '_PERSON_ID_DATE_rownummax'); print()
tmpt = tab(tmpc4.where(f.col('_PERSON_ID_rownum') == 1), '_PERSON_ID_egen_rownummax'); print()

# tidy
tmpc5 = (
  tmpc4
  .select('PERSON_ID', 'DATE', f.col('_PERSON_ID_DATE_rownum').alias('DATE_rownum'), 'TIME', 'PROCEDURE', 'PRODUCT', 'archived_on')
) 

# temp save
tmpc5 = temp_save(df=tmpc5, out_name=f'{proj}_tmp_vacc_tmpc5'); print()

# check
count_varlist(tmpc5, ['PERSON_ID', 'DATE'])
count_varlist(tmpc5, ['PERSON_ID', 'DATE', 'DATE_rownum'])
tmpt = tab(tmpc5, 'DATE_rownum'); print()
print(tmpc5.orderBy('PERSON_ID', 'DATE', 'DATE_rownum').limit(10).toPandas().to_string()); print()


print('---------------------------------------------------------------------------------')
print('save dataframe of individuals with duplicates to investigate separately')
print('---------------------------------------------------------------------------------')
# check
tmpf = (
  tmpc4
  .withColumn('_chk', f.when(f.col('_PERSON_ID_egen_rownummax') >= 1, 1).otherwise(0))
)
assert tmpf.where(f.col('_chk') == 0).count() == 0

# with duplicates
tmpd1 = (
  tmpc4
  .where(f.col('_PERSON_ID_egen_rownummax') > 1)
  .select('PERSON_ID', 'DATE', f.col('_PERSON_ID_DATE_rownum').alias('DATE_rownum'), 'TIME', 'PROCEDURE', 'PRODUCT', 'archived_on')
) 

# temp save
tmpd1 = temp_save(df=tmpd1, out_name=f'{proj}_tmp_vacc_tmpd1'); print()

# check
count_varlist(tmpd1, ['PERSON_ID', 'DATE'])
print(tmpd1.orderBy('PERSON_ID', 'DATE', 'DATE_rownum').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 6.2 Flag duplicates on PERSON_ID and PROCEDURE (dose 1 and 2 only)

# COMMAND ----------

# tmpc5 = spark.table(f'{dbc}.{proj}_tmp_vacc_tmpc5')

# COMMAND ----------

# check
tmpt = tab(tmpc5, 'PROCEDURE'); print()

# add row number for PERSON_ID and PROCEDURE
_win_PERSON_ID_PROCEDURE_ord = Window\
  .partitionBy('PERSON_ID', 'PROCEDURE')\
  .orderBy('DATE', 'DATE_rownum')
tmpc6 = (
  tmpc5
  .withColumn('PROCEDURE_rownum', f.row_number().over(_win_PERSON_ID_PROCEDURE_ord))
  .withColumn('PROCEDURE_rownum', f.when(f.col('PROCEDURE').isin('1', '2'), f.col('PROCEDURE_rownum')).otherwise(f.lit(1)))
)

# check
tmpt = tab(tmpc6, 'PROCEDURE_rownum', 'PROCEDURE'); print()

# tidy
tmpc6 = (
  tmpc6
  .select('PERSON_ID', 'DATE', 'DATE_rownum', 'TIME', 'PROCEDURE', 'PROCEDURE_rownum', 'PRODUCT', 'archived_on')
)

# check
print(tmpc6.orderBy('PERSON_ID', 'DATE', 'DATE_rownum').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 7 Finalise

# COMMAND ----------

# MAGIC %md ## 7.1 Create dose sequence

# COMMAND ----------

# NOTE: dose sequence and row number will need to be updated following any further exlusions 
#       (e.g., records before 20201208, duplicate PROCEDURE dose 1, etc.)

print('---------------------------------------------------------------------------------')
print('create dose sequence')
print('---------------------------------------------------------------------------------')
# currently PROCEDURE takes values '1', '2', and 'B'
# not possible to distinguish between boosters (i.e., booster 1, booster 2, etc.) by SNOMED codes, simply 'B' 
# so we number boosters according to DATE and DATE_rownum, assuming a dose sequence start number of 3 after first and second dose
_win_cumulative = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', 'DATE_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

tmpc7 = (
  tmpc6
  .withColumn('_booster', f.when(f.col('PROCEDURE') == 'B', f.lit(1)).otherwise(0))
  .withColumn('_booster_cumulative', f.sum(f.col('_booster')).over(_win_cumulative))
  .withColumn('_booster_cumulative_plus2', f.col('_booster_cumulative') + f.lit(2))  
  .withColumn('dose_sequence', 
              f.when(f.col('PROCEDURE').isin(['1','2']), f.col('PROCEDURE').cast(t.IntegerType()))
              .when(f.col('PROCEDURE') == 'B', f.col('_booster_cumulative_plus2'))
              .otherwise(999999))
  .drop('_booster', '_booster_cumulative', '_booster_cumulative_plus2'))

# check
tmpt = tab(tmpc7, 'dose_sequence'); print()
tmpt = tab(tmpc7, 'dose_sequence', 'PROCEDURE'); print()


print('---------------------------------------------------------------------------------')
print('create row number')
print('---------------------------------------------------------------------------------')
# add row number that we will compare _dose_seq against
_win_rownum = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', 'DATE_rownum')
_win_rownummax = Window\
  .partitionBy('PERSON_ID')

tmpc7 = (
  tmpc7
  .withColumn('PERSON_ID_rownum', f.row_number().over(_win_rownum))
  .withColumn('PERSON_ID_rownummax', f.count('PERSON_ID').over(_win_rownummax)))

# check
tmpt = tab(tmpc7, 'PERSON_ID_rownum'); print()
tmpt = tab(tmpc7.where(f.col('PERSON_ID_rownum') == 1), 'PERSON_ID_rownummax'); print()


print('---------------------------------------------------------------------------------')
print('compare dose_sequence and PERSON_ID_rownum')
print('---------------------------------------------------------------------------------')
tmpt = tab(tmpc7, 'PERSON_ID_rownum', 'dose_sequence'); print()

# COMMAND ----------

# tmpg = tmpc6.where((f.col('dose_sequence') == 3) & (f.col('PERSON_ID_rownum') == 1))
# display(tmpg)
# display(tmpc6.where(f.col('PERSON_ID') == '0AKLWW0I3VXTGH3'))

# COMMAND ----------

# MAGIC %md ## 7.2 Save

# COMMAND ----------

# save name
outName = f'{proj}_cur_vacc'.lower()

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
tmpc7.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmpc7 = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

display(tmpc7.orderBy('PERSON_ID', 'DATE', 'DATE_rownum'))

# COMMAND ----------

# MAGIC %md # 8 Quality assurance

# COMMAND ----------

"""
Similar to SAIL RRDA_CVVD (Research Ready Data Asset for COVID-19 Vaccination Data) we perform quality assurance (validation rules) for PERSON_ID.
The following quality assurance rules are used

# qa_1: Mutliple vaccinations (with different PROCEDURE and/or PRODUCT) on the same DATE
# qa_2: Mutliple dose 1 and/or dose 2 vaccinations 
# qa_3: DATE is after the Production Date (i.e., archived_on)
# qa_4: DATE is before 20201208 (i.e., the start of the vaccination program) 
# qa_5: DATE is before the specific vaccine PRODUCT start date
# qa_6: Interval between vaccinations is < 20 days
# qa_7: DOSE_SEQUENCE is NOT sequential (e.g., second dose before first dose, multiple second doses) 
# qa_8: Mixed vaccine PRODUCT before 20210507

# # # # # qa_9: Age at DATE is before specific age range start date ** to be added **
# # # # # qa_10: PRODUCT is not appropriate for age range (e.g., Pfizer only for children) ** to be added **

The following additional variables are provided
# qa_max:    maximum of qa_1-qa_8
# qa_concat: concatenate qa_1-qa_8
# n_rows:    number of rows per PERSON_ID
# dose_1_2:  '1' if the PERSON_ID has records with both PROCEDURE=='1' and PROCEDURE=='2', '0' otherwise
"""

# COMMAND ----------

# checkpoint
spark.sql(f'REFRESH TABLE {dbc}.{proj}_cur_vacc')
tmpq1 = spark.table(f'{dbc}.{proj}_cur_vacc')

# COMMAND ----------

# MAGIC %md ## 8.1 Create

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('quality assurance')
print('---------------------------------------------------------------------------------')
# define windows for rownum and rownummax
_win_PERSON_ID = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', 'DATE_rownum')

# create date difference between date and previous date within individuals
# create indicator for when the date difference is <20 days (assumed to be the minimum legitimate interval between vaccine administration [SAIL])
# intentionnaly not using nse for lag difference for products
tmpq1 = (
  tmpq1
  .withColumn('qa_1', f.when(f.col('DATE_rownum') > 1, 1).otherwise(0))    
  .withColumn('qa_2', f.when(f.col('PROCEDURE_rownum') > 1, 1).otherwise(0))    
  .withColumn('qa_3', f.when(f.col('DATE') > f.col('archived_on'), 1).otherwise(0))    
  .withColumn('qa_4', f.when(f.col('DATE') < f.to_date(f.lit('2020-12-08')), 1).otherwise(0))    
  .withColumn('_tmp_qa_5_pf', f.when((f.trim(f.col('PRODUCT')) == 'Pfizer')        & (f.col('DATE') < f.to_date(f.lit('2020-12-08'))), 1).otherwise(0))
  .withColumn('_tmp_qa_5_az', f.when((f.trim(f.col('PRODUCT')) == 'AstraZeneca')   & (f.col('DATE') < f.to_date(f.lit('2021-01-04'))), 1).otherwise(0))
  .withColumn('_tmp_qa_5_mo', f.when((f.trim(f.col('PRODUCT')) == 'Moderna')       & (f.col('DATE') < f.to_date(f.lit('2021-01-04'))), 1).otherwise(0))
  .withColumn('_tmp_qa_5_ja', f.when((f.trim(f.col('PRODUCT')) == 'Janssen-Cilag') & (f.col('DATE') < f.to_date(f.lit('2021-03-01'))), 1).otherwise(0))
  .withColumn('_tmp_qa_5_pc', f.when((f.trim(f.col('PRODUCT')) == 'Pfizer child')  & (f.col('DATE') < f.to_date(f.lit('2022-01-20'))), 1).otherwise(0))
  .withColumn('qa_5', f.greatest(*[f.col('_tmp_qa_5_pf'), f.col('_tmp_qa_5_az'), f.col('_tmp_qa_5_mo'), f.col('_tmp_qa_5_ja'), f.col('_tmp_qa_5_pc')]))  
  .withColumn('_tmp_DATE_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win_PERSON_ID)))  
  .withColumn('qa_6', f.when(f.col('_tmp_DATE_diff') < 20, 1).otherwise(0))  
  .withColumn('qa_7', f.lit(1) - udf_null_safe_equality('dose_sequence', 'PERSON_ID_rownum').cast(t.IntegerType()))
  .withColumn('_tmp_PRODUCT_diff', f.when(f.col('PRODUCT') != f.lag(f.col('PRODUCT'), 1).over(_win_PERSON_ID), 1).otherwise(0))
  .withColumn('qa_8', f.when((f.col('_tmp_PRODUCT_diff') == 1) & (f.col('DATE') < f.to_date(f.lit('2021-05-07'))), 1).otherwise(0))  
  .withColumn('dose_1', f.when(f.col('PROCEDURE') == '1', 1).otherwise(0))
  .withColumn('dose_2', f.when(f.col('PROCEDURE') == '2', 1).otherwise(0))
)

# COMMAND ----------

# MAGIC %md ## 8.2 Check

# COMMAND ----------

# check
display(tmpq1)

# COMMAND ----------

# check

print('---------------------------------------------------------------------------------')
print('qa_1')
print('---------------------------------------------------------------------------------')  
tmpt = tab(tmpq1, 'qa_1'); print()
tmpt = tabstat(tmpq1, 'DATE', byvar='qa_1', date=1); print()

print('---------------------------------------------------------------------------------')
print('qa_2')
print('---------------------------------------------------------------------------------')  
tmpt = tab(tmpq1, 'qa_2'); print()
tmpt = tabstat(tmpq1, 'DATE', byvar='qa_2', date=1); print()

print('---------------------------------------------------------------------------------')
print('qa_3')
print('---------------------------------------------------------------------------------')  
tmpt = tab(tmpq1, 'qa_3'); print()
tmpt = tabstat(tmpq1, 'DATE', byvar='qa_3', date=1); print()

print('---------------------------------------------------------------------------------')
print('qa_4')
print('---------------------------------------------------------------------------------')  
tmpt = tab(tmpq1, 'qa_4'); print()
tmpt = tabstat(tmpq1, 'DATE', byvar='qa_4', date=1); print()

print('---------------------------------------------------------------------------------')
print('qa_5')
print('---------------------------------------------------------------------------------')  
tmpt = tab(tmpq1, 'qa_5'); print()
tmpt = tab(tmpq1, '_tmp_qa_5_pf'); print()
tmpt = tab(tmpq1, '_tmp_qa_5_az'); print()
tmpt = tab(tmpq1, '_tmp_qa_5_mo'); print()
tmpt = tab(tmpq1, '_tmp_qa_5_ja'); print()
tmpt = tab(tmpq1, '_tmp_qa_5_pc'); print()
tmpt = tabstat(tmpq1.where(f.trim(f.col('PRODUCT')) == 'Pfizer'),        'DATE', byvar='_tmp_qa_5_pf', date=1); print()
tmpt = tabstat(tmpq1.where(f.trim(f.col('PRODUCT')) == 'AstraZeneca'),   'DATE', byvar='_tmp_qa_5_az', date=1); print()
tmpt = tabstat(tmpq1.where(f.trim(f.col('PRODUCT')) == 'Moderna'),       'DATE', byvar='_tmp_qa_5_mo', date=1); print()
tmpt = tabstat(tmpq1.where(f.trim(f.col('PRODUCT')) == 'Janssen-Cilag'), 'DATE', byvar='_tmp_qa_5_ja', date=1); print()
tmpt = tabstat(tmpq1.where(f.trim(f.col('PRODUCT')) == 'Pfizer child'),  'DATE', byvar='_tmp_qa_5_pc', date=1); print()
tmpt = tmpq1\
  .withColumn('_concat', f.concat(*[f.col(f'_tmp_qa_5_{pp}') for pp in ['pf', 'az', 'mo', 'ja', 'pc']]))
tmpt = tab(tmpt, '_concat', 'qa_5'); print()

print('---------------------------------------------------------------------------------')
print('qa_6')
print('---------------------------------------------------------------------------------')  
tmpt = tab(tmpq1, 'qa_6'); print()
tmpt = tabstat(tmpq1, '_tmp_DATE_diff', byvar='qa_6'); print()
tmpt = tab(tmpq1, 'qa_6', 'qa_1'); print()

print('---------------------------------------------------------------------------------')
print('qa_7')
print('---------------------------------------------------------------------------------')  
tmpt = tab(tmpq1, 'qa_7'); print()
tmpt = tab(tmpq1, 'PERSON_ID_rownum', 'dose_sequence', var2_unstyled=1); print()
tmpt = tab(tmpq1.where(f.col('qa_7') == 1), 'PERSON_ID_rownum', 'dose_sequence', var2_unstyled=1); print()
tmpt = tab(tmpq1.where(f.col('qa_7') == 0), 'PERSON_ID_rownum', 'dose_sequence', var2_unstyled=1); print()
tmpt = tab(tmpq1.where(f.col('qa_7') == 1), 'PERSON_ID_rownummax'); print()
tmpt = tab(tmpq1.where((f.col('qa_7') == 1) & (f.col('PERSON_ID_rownummax') == 1)), 'PROCEDURE'); print()

print('---------------------------------------------------------------------------------')
print('qa_8')
print('---------------------------------------------------------------------------------') 
tmpt = tab(tmpq1, 'qa_8'); print()
tmpt = tabstat(tmpq1, 'DATE', byvar='qa_8', date=1); print()

# COMMAND ----------

# MAGIC %md ## 8.3 Summarise

# COMMAND ----------

# summarise by PERSON_ID
qa_rules_min = 1
qa_rules_max = 8 + 1
tmpq2 = (
  tmpq1
  .select(['PERSON_ID'] + [f'qa_{n}' for n in range(qa_rules_min, qa_rules_max)] + ['dose_1', 'dose_2'])
  .groupBy('PERSON_ID')
  .agg(
    f.count(f.lit(1)).alias('n_rows')
    , *[f.max(f.col(f'qa_{n}')).alias(f'qa_{n}') for n in range(qa_rules_min, qa_rules_max)]   
    , f.max(f.col(f'dose_1')).alias(f'dose_1')
    , f.max(f.col(f'dose_2')).alias(f'dose_2')
  )
  .withColumn('qa_max', f.greatest(*[f.col(f'qa_{n}') for n in range(qa_rules_min, qa_rules_max)]))  
  .withColumn('qa_concat', f.concat(*[f.col(f'qa_{n}') for n in range(qa_rules_min, qa_rules_max)]))
  .withColumn('dose_1_2', f.when((f.col('dose_1') == 1) & (f.col('dose_2') == 1), 1).otherwise(0))
  .drop('dose_1', 'dose_2')  
)

# COMMAND ----------

# MAGIC %md ## 8.4 Check

# COMMAND ----------

tmpt = tab(tmpq2, 'qa_max'); print()
tmpt = tab(tmpq2, 'qa_concat'); print()
tmpt = tab(tmpq2, 'qa_concat', 'qa_max'); print()
tmpt = tab(tmpq2.where(f.col('qa_max') == 1), 'qa_concat'); print()
tmpt = tab(tmpq2, 'n_rows', 'qa_7', var2_unstyled=1); print()

tmpt = tab(tmpq2, 'qa_2', 'qa_7', var2_unstyled=1); print()

tmpt = tab(tmpq2, 'dose_1_2'); print()
tmpt = tab(tmpq2, 'n_rows', 'dose_1_2', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 8.5 Save

# COMMAND ----------

tmpq3 = (
  tmpq2
  .select([col for col in tmpq2.columns if col != 'n_rows'] + ['n_rows'])
)
display(tmpq3)

# COMMAND ----------

# save name
outName = f'{proj}_cur_vacc_qa'.lower()

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
tmpq3.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
tmpq3 = spark.table(f'{dbc}.{outName}')
