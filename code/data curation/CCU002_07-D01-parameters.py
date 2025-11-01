# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CCU002_07-D01-parameters
# MAGIC
# MAGIC **Description** This notebook defines a set of parameters, which is loaded in each notebook in the data curation pipeline, so that helper functions and parameters are consistently available.
# MAGIC
# MAGIC **Authors** Alexia Sampri
# MAGIC
# MAGIC **Reviewers** Genevieve Cezard
# MAGIC
# MAGIC **Last Reviewed** 2023.05.16
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

# MAGIC %run "../shds/common/functions"

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.functions as F
import pandas as pd
import re

# COMMAND ----------

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu002_07'


# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = 'dars_nic_391419_j3w9t'
dbc = f'dsa_391419_j3w9t_collab'
dbc_old = f'{db}_collab'


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# data frame of datasets

tmp_archived_on = '2023-03-31 00:00:00.000000'
data = [
    ['deaths',  dbc_old, f'deaths_{db}_archive',            tmp_archived_on, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',   dbc_old, f'gdppr_{db}_archive',             tmp_archived_on, 'NHS_NUMBER_DEID',                'DATE']
  , ['gdppr_1st',   dbc_old, f'gdppr_{db}_archive',         '2020-11-23', 'NHS_NUMBER_DEID',                'DATE']
  , ['gdppr_10th',   dbc_old, f'gdppr_{db}_archive',        '2021-07-29', 'NHS_NUMBER_DEID',                'DATE']
  , ['hes_apc', dbc_old, f'hes_apc_all_years_archive',      tmp_archived_on, 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_op',  dbc_old, f'hes_op_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'APPTDATE'] 
  , ['hes_ae',  dbc_old, f'hes_ae_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'ARRIVALDATE'] 
  , ['vacc',    dbc_old, f'vaccine_status_{db}_archive',    tmp_archived_on, 'PERSON_ID_DEID',                 'DATE_AND_TIME']
  , ['sgss',    dbc_old, f'sgss_{db}_archive',              tmp_archived_on, 'PERSON_ID_DEID',                 'Specimen_Date']
#  , ['hes_cc',  dbc, f'hes_cc_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'CCSTARTDATE']   
  , ['sus',     dbc_old, f'sus_{db}_archive',               '2022-09-30',    'NHS_NUMBER_DEID',                'EPISODE_START_DATE']     
  , ['chess',   dbc_old, f'chess_{db}_archive',             tmp_archived_on, 'PERSON_ID_DEID',                 'InfectionSwabDate']       
  , ['pmeds',   dbc_old, f'primary_care_meds_{db}_archive', tmp_archived_on, 'Person_ID_DEID',                 'ProcessingPeriodDate']
  , ['ecds',    dbc_old, f'lowlat_ecds_all_years_archive',  tmp_archived_on, 'PERSON_ID_DEID',                 'ARRIVAL_DATE'] 
]
parameters_df_datasets = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'archived_on', 'idVar', 'dateVar'])
print('parameters_df_datasets:\n', parameters_df_datasets.to_string())
  
# note: the below is largely listed in order of appearance within the pipeline:  

# reference tables
path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
path_ref_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
# path_ref_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
# path_ref_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'
path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'

# curated tables
path_cur_hes_apc_long      = f'{dbc}.{proj}_cur_hes_apc_all_years_long'
path_cur_hes_apc_oper_long = f'{dbc}.{proj}_cur_hes_apc_all_years_archive_oper_long'
path_cur_deaths_long       = f'{dbc}.{proj}_cur_deaths_{db}_archive_long'
path_cur_deaths_sing       = f'{dbc}.{proj}_cur_deaths_{db}_archive_sing'
path_cur_lsoa_region       = f'{dbc}.{proj}_cur_lsoa_region_lookup'
path_cur_lsoa_imd          = f'{dbc}.{proj}_cur_lsoa_imd_lookup'
path_cur_vacc_first        = f'{dbc}.{proj}_cur_vacc_first'
path_cur_covid             = f'{dbc}.{proj}_cur_covid'
path_cur_vacc_qa           = f'{dbc}.{proj}_cur_vacc_qa'
path_cur_ecds              = f'{dbc}.{proj}_cur_ecds_all_years_archive_long'

# # temporary tables
path_tmp_skinny_unassembled             = f'{dbc}.{proj}_tmp_kpc_harmonised'
path_tmp_skinny_assembled               = f'{dbc}.{proj}_tmp_kpc_selected'
path_tmp_skinny                         = f'{dbc}.{proj}_tmp_skinny'
path_tmp_quality_assurance_hx_1st_wide  = f'{dbc}.{proj}_tmp_quality_assurance_hx_1st_wide'
path_tmp_quality_assurance_hx_1st       = f'{dbc}.{proj}_tmp_quality_assurance_hx_1st'
path_tmp_quality_assurance_qax          = f'{dbc}.{proj}_tmp_quality_assurance_qax'
path_tmp_cohort                         = f'{dbc}.{proj}_tmp_cohort'
path_tmp_flow_inc_exc                   = f'{dbc}.{proj}_tmp_flow_inc_exc'
path_tmp_covariates_hes_apc             = f'{dbc}.{proj}_tmp_covariates_hes_apc'
path_tmp_covariates_pmeds               = f'{dbc}.{proj}_tmp_covariates_pmeds'
path_tmp_covariates_lsoa                = f'{dbc}.{proj}_tmp_covariates_lsoa'
path_tmp_covariates_lsoa_2              = f'{dbc}.{proj}_tmp_covariates_lsoa_2'
path_tmp_covariates_lsoa_3              = f'{dbc}.{proj}_tmp_covariates_lsoa_3'
path_tmp_covariates_n_consultations     = f'{dbc}.{proj}_tmp_covariates_n_consultations'
path_tmp_covariates_unique_bnf_chapters = f'{dbc}.{proj}_tmp_covariates_unique_bnf_chapters'
path_tmp_covariates_hx_out_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_out_1st_wide'
path_tmp_covariates_hx_com_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_com_1st_wide'
path_tmp_inf_covariates_lsoa            = f'{dbc}.{proj}_tmp_inf_covariates_lsoa'
path_tmp_cur_vacc_first                 = f'{dbc}.{proj}_tmp_cur_vacc_first'

# out tables
path_out_codelist_ecds_outcome      = f'{dbc}.{proj}_out_codelist_ecds_outcome_as'
path_out_codelist_ecds_outcome_all      = f'{dbc}.{proj}_out_codelist_ecds_outcome_all'
path_out_codelist_outcome           = f'{dbc}.{proj}_out_codelist_outcome'
path_out_codelist_comorbidity       = f'{dbc}.{proj}_out_codelist_comorbidity'
path_out_codelist_covid             = f'{dbc}.{proj}_out_codelist_covid'
path_out_codelist_quality_assurance = f'{dbc}.{proj}_out_codelist_quality_assurance'
path_out_codelist_cev             = f'{dbc}.{proj}_out_codelist_cev'

path_out_skinny                     = f'{dbc}.{proj}_skinny'
path_out_quality_assurance          = f'{dbc}.{proj}_quality_assurance'

path_out_inf_covariates_lsoa        = f'{dbc}.{proj}_out_inf_covariates_lsoa'
path_out_vacc_covariates_lsoa       = f'{dbc}.{proj}_out_vacc_covariates_lsoa'

path_out_inc_exc_inf_cohort         = f'{dbc}.{proj}_out_inc_exc_inf_cohort'
path_out_inc_exc_vacc_cohort        = f'{dbc}.{proj}_out_inc_exc_vacc_cohort'
path_out_inf_flow_inc_exc           = f'{dbc}.{proj}_out_inf_flow_inc_exc'
path_out_vacc_flow_inc_exc          = f'{dbc}.{proj}_out_vacc_flow_inc_exc'

path_out_inf_cohort                 = f'{dbc}.{proj}_out_inf_cohort'
path_out_vacc_cohort                = f'{dbc}.{proj}_out_vacc_cohort'

path_out_inf_covariates             = f'{dbc}.{proj}_out_inf_covariates'
path_out_vacc_covariates            = f'{dbc}.{proj}_out_vacc_covariates'

path_out_inf_exposures              = f'{dbc}.{proj}_out_inf_exposures'

path_out_inf_outcomes               = f'{dbc}.{proj}_out_inf_outcomes'
path_out_vacc_outcomes              = f'{dbc}.{proj}_out_vacc_outcomes'

path_out_inf_analysis               = f'{dbc}.{proj}_out_inf_analysis'
path_out_vacc_analysis              = f'{dbc}.{proj}_out_vacc_analysis'

path_out_inf_ecds_analysis               = f'{dbc}.{proj}_out_inf_ecds_analysis'
path_out_vacc_ecds_analysis              = f'{dbc}.{proj}_out_vacc_ecds_analysis'


# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------
proj_child_inf_start_date = '2020-01-01' 
proj_child_inf_end_date   = '2022-03-31'

proj_child_vacc_start_date = '2021-08-06' 
proj_child_vacc_end_date   = '2022-12-31'

# -----------------------------------------------------------------------------
# out tables 
# -----------------------------------------------------------------------------
#tmp_out_date = '20230313'
#data = [
#    ['codelist',  tmp_out_date]
#    ['cohort',    tmp_out_date]
#  , ['skinny',    tmp_out_date]  
#  , ['covariates', tmp_out_date]  
#  , ['exposures',  tmp_out_date]      
#  , ['outcomes',   tmp_out_date]  
#  , ['analysis',   tmp_out_date]    
#]
#df_out = pd.DataFrame(data, columns = ['dataset', 'out_date'])
# df_out

# COMMAND ----------

# function to extract the batch corresponding to the pre-defined archived_on date from the archive for the specified dataset
from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']  
  print(_path + ' (archived_on = ' + _archived_on + ')')
  
  # check path exists # commented out for runtime
#   _tmp_exists = spark.sql(f"SHOW TABLES FROM {_row['database']}")\
#     .where(f.col('tableName') == _row['table'])\
#     .count()
#   assert _tmp_exists == 1, f"path = {_path} not found"

  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _tmp_records = _tmp.count()
  print(f'  {_tmp_records:,} records')
  assert _tmp_records > 0, f"number of records == 0"

  # return dataframe
  return _tmp

# COMMAND ----------

print(f'Project:')
print("  {0:<22}".format('proj') + " = " + f'{proj}') 
print(f'')
print(f'Databases:')
print("  {0:<22}".format('db') + " = " + f'{db}') 
print("  {0:<22}".format('dbc') + " = " + f'{dbc}') 
print(f'')
print(f'Paths:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_archive')
# print(df_archive[['dataset', 'database', 'table', 'productionDate']].to_string())
print(f'')
tmp = vars().copy()
for var in list(tmp.keys()):
  if(re.match('^path_.*$', var)):
    print("  {0:<22}".format(var) + " = " + tmp[var])    
print(f'')
print(f'Dates:')    
print("  {0:<22}".format('proj_child_inf_start_date') + " = " + f'{proj_child_inf_start_date}') 
print("  {0:<22}".format('proj_child_inf_end_date') + " = " + f'{proj_child_inf_end_date}')
print("  {0:<22}".format('proj_child_inf_start_date') + " = " + f'{proj_child_vacc_start_date}') 
print("  {0:<22}".format('proj_child_inf_end_date') + " = " + f'{proj_child_vacc_end_date}')  
print(f'')
#print(f'composite_events:')   
#for i, c in enumerate(composite_events):
  #print('  ', i, c, '=', composite_events[c])
#print(f'')
# print(f'Out dates:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_out')
#print(df_out[['dataset', 'out_date']].to_string())
#print(f'')

# COMMAND ----------

print(f'Project:')
print("  {0:<22}".format('proj') + " = " + f'{proj}') 
print(f'')
print(f'Databases:')
print("  {0:<22}".format('db') + " = " + f'{db}') 
print("  {0:<22}".format('dbc') + " = " + f'{dbc}') 
print(f'')
print(f'Paths:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_archive')
# print(df_archive[['dataset', 'database', 'table', 'productionDate']].to_string())
print(f'')
tmp = vars().copy()
for var in list(tmp.keys()):
  if(re.match('^path_.*$', var)):
    print("  {0:<22}".format(var) + " = " + tmp[var])    
print(f'')
print(f'Dates:')    
print("  {0:<22}".format('proj_child_vacc_start_date') + " = " + f'{proj_child_vacc_start_date}') 
print("  {0:<22}".format('proj_child_vacc_end_date') + " = " + f'{proj_child_vacc_end_date}') 
print(f'')
#print(f'composite_events:')   
#for i, c in enumerate(composite_events):
  #print('  ', i, c, '=', composite_events[c])
#print(f'')
# print(f'Out dates:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_out')
#print(df_out[['dataset', 'out_date']].to_string())
#print(f'')

# COMMAND ----------

# # Check latest archived dates for each dataset of interest - checked on 2023-05-16

# tmp = spark.table(f'{dbc_old}.vaccine_status_{db}_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> Vaccine status dataset latest update : 2023-02-28 / 2023-03-31

# tmp = spark.table(f'{dbc_old}.deaths_{db}_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> Death dataset latest update : 2023-02-28 / 2023-03-31 / 2023-04-27

# tmp = spark.table(f'{dbc_old}.gdppr_{db}_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> GDPPR dataset latest update : 2023-02-28 / 2023-03-31 / 2023-04-27

# tmp = spark.table(f'{dbc_old}.hes_apc_all_years_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> HES APC dataset latest update : 2022-12-31/ 2023-03-31 / 2023-04-27

# tmp = spark.table(f'{dbc}.hes_op_all_years_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> HES Outpatient dataset latest update : 2022-12-31 / 2023-03-31 / 2023-04-27

# tmp = spark.table(f'{dbc}.hes_ae_all_years_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> HES A&E dataset latest update : 2022-12-31/  2023-03-31

# tmp = spark.table(f'{dbc}.sgss_{db}_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> SGSS dataset latest update : 2022-12-31 / 2023-03-31 / 2023-04-27

# tmp = spark.table(f'{dbc}.hes_cc_all_years_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> HES CC dataset latest update : 2023-03-31
  
# tmp = spark.table(f'{dbc}.sus_{db}_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> SUS dataset latest update : 2022-09-30
  
# tmp = spark.table(f'{dbc}.chess_{db}_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> CHESS dataset latest update : 2023-03-31
  
# tmp = spark.table(f'{dbc}.primary_care_meds_{db}_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> PMEDS dataset latest update : 2023-03-31 
  
# tmp = spark.table(f'{dbc}.lowlat_ecds_all_years_archive')
# tmpt = tab(tmp, 'archived_on')
# #-> ECDS dataset latest update :  2023-03-31
