# This script transfers infection data from Databricks to RStudio environment
# Author: Alexia Sampri

rm(list = ls())


# Setup Databricks connection --------------------------------------------------

con <- DBI::dbConnect(odbc::odbc(),
                      "Databricks",
                      timeout = 60,
                      PWD = rstudioapi::askForPassword("Password please:"))


library(DBI)
con <- dbConnect(
  odbc::odbc(),
  dsn = 'databricks',
  HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
  PWD = rstudioapi::askForPassword('Please enter Databricks PAT')
)



# Transfer data from DataBricks ------------------------------------------------


chunks <- 8
df <- NULL

for (i in 1:chunks) {
  
  print(paste0("Transferring chunk ",i," of ",chunks,"."))
  
  tmp <- DBI::dbGetQuery(con, paste0("SELECT * FROM dsa_391419_j3w9t_collab.ccu002_07_out_inf_analysis WHERE CHUNK='",i,"'"))
  
  df <- rbind(df,tmp)
}

data.table::fwrite(df,paste0("ccu002_07_out_inf_analysis",gsub("-","",Sys.Date()),".csv.gz"))



df <- NULL

for (i in 1:chunks) {
  
  print(paste0("Transferring chunk ",i," of ",chunks,"."))
  
  tmp <- DBI::dbGetQuery(con, paste0("SELECT * FROM dsa_391419_j3w9t_collab.ccu002_07_out_inf_ecds_analysis WHERE CHUNK='",i,"'"))
  
  df <- rbind(df,tmp)
}

data.table::fwrite(df,paste0("ccu002_07_out_inf_ecds_analysis",gsub("-","",Sys.Date()),".csv.gz"))


df <- NULL

for (i in 1:chunks) {
  
  print(paste0("Transferring chunk ",i," of ",chunks,"."))
  
  tmp <- DBI::dbGetQuery(con, paste0("SELECT * FROM dsa_391419_j3w9t_collab.ccu002_07_out_vacc_analysis WHERE CHUNK='",i,"'"))
  
  df <- rbind(df,tmp)
}

data.table::fwrite(df,paste0("ccu002_07_out_vax_analysis",gsub("-","",Sys.Date()),".csv.gz"))


df <- NULL

for (i in 1:chunks) {
  
  print(paste0("Transferring chunk ",i," of ",chunks,"."))
  
  tmp <- DBI::dbGetQuery(con, paste0("SELECT * FROM dsa_391419_j3w9t_collab.ccu002_07_out_vacc_ecds_analysis WHERE CHUNK='",i,"'"))
  
  df <- rbind(df,tmp)
}

data.table::fwrite(df,paste0("ccu002_07_out_vax_ecds_analysis",gsub("-","",Sys.Date()),".csv.gz"))

