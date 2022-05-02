#### Preparation ####

# Clear the environment
rm(list = ls())

# Load the required libraries
#install.packages('dplyr')
library(dplyr) #Data manipulation
#install.packages('readxl')
library(readxl) #read in Excel files
#install.packages('tibble')
library(tibble)
#install.packages('tidyverse')
library(tidyverse)
#install.packages('RSQLite')
library(RSQLite) #R SQLite driver package
#install.packages('DBO')
library(DBI) #Database driver package
#install.packages('lubridate')
library(lubridate) #Data manipulation
#install.packages("reshape2")
library(reshape2)
#Mapping of the directory path
setwd(paste0(getwd()))
getwd()

#Establish connection the the SQLite database
mydb <- dbConnect(RSQLite::SQLite(), "data/secure/sqlite/ndn.sqlite") 

#Load TAB delimited files downloaded from CAPI server

notification <- read.delim("data/secure/Covid_Surveilance.tab")
int_actions <- read.delim("data/secure/interview__actions.tab")
int_asmnt <- read.delim("data/secure/assignment__actions.tab")

#Write the loaded files to an SQLite database

dbWriteTable(mydb, "notification", notification, overwrite = TRUE)
dbWriteTable(mydb, "int_actions", int_actions, overwrite = TRUE)
dbWriteTable(mydb, "int_asmnt", int_asmnt, overwrite = TRUE)

#Write the code to do all processing here before disconnecting the SQLite database

#1. Geography
geography <- read.csv("C:/RAP/Health-RAP-Notification/other/ac.csv")   


dbWriteTable(mydb, "geography", geography, overwrite=TRUE)

#2.sex
gender <- read.csv("C:/RAP/Health-RAP-Notification/other/sex.csv")   


dbWriteTable(mydb, "gender", gender, overwrite=TRUE)

#3.source of health notifications

source_of_health <- read.csv("C:/RAP/Health-RAP-Notification/other/source_of_health.csv") 

dbWriteTable(mydb, "source_of_health", source_of_health, overwrite=TRUE)

#4. Disease

disease <- read.csv("C:/RAP/Health-RAP-Notification/other/diseases.csv") 

dbWriteTable(mydb, "disease", disease, overwrite=TRUE)

#5. Test type

type_test <- read.csv("C:/RAP/Health-RAP-Notification/other/test_type.csv")

dbWriteTable(mydb, "type_test", type_test, overwrite=TRUE)

#Rename of interview key id in the notifications in the data

colnames(notification)[1] <- 'id'
colnames(notification)[25] <- 'Dengue_Virus'
colnames(notification)[26] <- 'Zika_Virus'
colnames(notification)[27] <- 'Chikungunya_Virus'
colnames(notification)[28] <- 'Meningococcal_Disease'
colnames(notification)[29] <- 'Diarrhoea_increase'
colnames(notification)[30] <- 'Cholera'
colnames(notification)[31] <- 'Pneumonia'
colnames(notification)[32] <- 'Influenza'
colnames(notification)[33] <- 'Measles_Rubella'
colnames(notification)[34] <- 'Pertusis'
colnames(notification)[35] <- 'Tetanus'
colnames(notification)[36] <- 'Diphtheria'
colnames(notification)[37] <- 'Polio'
colnames(notification)[38] <- 'Typhoid'
colnames(notification)[39] <- 'Malaria'
colnames(notification)[40] <- 'Leptospirosis'
colnames(notification)[41] <- 'Tuberculosis'
colnames(notification)[42] <- 'Leprosy'
colnames(notification)[43] <- 'COVID_19'
colnames(notification)[44] <- 'HIV'
colnames(notification)[45] <- 'Other_disease_symp'

colnames(notification)[48] <- 'Whole_Blood'
colnames(notification)[49] <- 'Urine'
colnames(notification)[50] <- 'Dried_Blood_Spot'
colnames(notification)[51] <- 'covid_19_symp'
colnames(notification)[52] <- 'Other_test_symp'



dbWriteTable(mydb, "notification", notification, overwrite = TRUE)

# 1. Total Number of Patients by Source of Health Notifications and by Area Council

patients_ac <- dbGetQuery(mydb, "SELECT notification.province, geography.pname, notification.area_council, geography.acname, source_of_health.source, gender.gender, 
                                    COUNT (id) as population
                                    FROM notification
                                    INNER JOIN geography on notification.area_council = geography.acid
                                    INNER JOIN gender on notification.sex = gender.gender_id
                                    INNER JOIN source_of_health on notification.source = source_of_health.source_id
                                    GROUP BY  notification.province, notification.area_council, source_of_health.source, gender.gender
                               ORDER BY geography.provid, geography.acid 
                               ") 

patients_acpivot <- patients_ac %>%
  filter(population != "NA") %>%
  pivot_wider(names_from = source, values_from = population, values_fill = 0) %>%
  ungroup()

dbWriteTable(mydb, "patients_acpivot", patients_acpivot, overwrite=TRUE)

write.csv(patients_acpivot,"C:\\Census 2020\\Population-Census-2020\\data\\open\\team1\\Table Number of Patients by area council and source of notifications.csv", row.names = FALSE)


#2. Total number of Patients by sex, source of health notifications and by area council

patients_sex <- dbGetQuery(mydb, "SELECT notification.province, geography.pname, notification.area_council, geography.acname, source_of_health.source, gender.gender, 
                                  COUNT (id) as population
                                  FROM notification
                                  INNER JOIN geography on notification.area_council = geography.acid
                                  INNER JOIN gender on notification.sex = gender.gender_id
                                  INNER JOIN source_of_health on notification.source = source_of_health.source_id
                                  GROUP BY  notification.province, notification.area_council, source_of_health.source, gender.gender
                                  ORDER BY geography.provid, geography.acid 
                       ") 



patients_sexpivot <- patients_sex %>%
  filter(population != "NA") %>%
  pivot_wider(names_from = gender, values_from = population, values_fill = 0) %>%
  ungroup()

dbWriteTable(mydb, "patients_sexpivot", patients_sexpivot, overwrite=TRUE)

write.csv(patients_sexpivot,"C:\\Census 2020\\Population-Census-2020\\data\\open\\team1\\Table Number of Patients by sex, area council and source of notifications.csv", row.names = FALSE)



#3. Diseases suspected by sex, source of health notifications and area council

diseases_sex <- dbGetQuery(mydb, "SELECT notification.province, geography.pname, notification.area_council, geography.acname, source_of_health.source, 
                                  gender.gender,
                                  SUM (Dengue_Virus) as Dengue_Virus,
                                  SUM (Zika_Virus) as Zika_Virus,
                                  SUM (Chikungunya_Virus) as Chikungunya_Virus,
                                  SUM (Meningococcal_Disease) as Meningococcal_Disease,
                                  SUM (Diarrhoea_increase) as Diarrhoea_increase,
                                  SUM (Cholera) as Cholera,
                                  SUM (Pneumonia) as Pneumonia,
                                  SUM (Influenza) as Influenza,
                                  SUM (Measles_Rubella) as Measles_Rubella,
                                  SUM (Pertusis) as Pertusis,
                                  SUM (Tetanus) as Tetanus,
                                  SUM (Diphtheria) as Diphtheria,
                                  SUM (Polio) as Polio,
                                  SUM (Typhoid) as Typhoid,
                                  SUM (Malaria) as Malaria,
                                  SUM (Leptospirosis) as Leptospirosis,
                                  SUM (Tuberculosis) as Tuberculosis,
                                  SUM (Leprosy) as Leprosy,
                                  SUM (COVID_19) as COVID_19,
                                  SUM (HIV) as HIV,
                                  SUM (Other_disease_symp) as Other_disease_symp
                                  FROM notification
                                  INNER JOIN geography on notification.area_council = geography.acid
                                  INNER JOIN gender on notification.sex = gender.gender_id
                                  INNER JOIN source_of_health on notification.source = source_of_health.source_id
                                  GROUP BY  notification.province, notification.area_council, source_of_health.source, gender.gender
                                  ORDER BY geography.provid, geography.acid 

                                 ") 





dbWriteTable(mydb, "diseases_sexpivot", diseases_sexpivot, overwrite=TRUE)

write.csv(diseases_sexpivot,"C:\\Census 2020\\Population-Census-2020\\data\\open\\team1\\Table Number of Diseases suspected by sex, area council and source of notifications.csv", row.names = FALSE)



#4. Diseases suspected of COVID by sex, source of health notifications and area council

covid_sex <- dbGetQuery(mydb, "SELECT notification.province, geography.pname, notification.area_council, geography.acname, source_of_health.source, 
                                  gender.gender,
                                    SUM (COVID_19) as COVID_19
                                    FROM notification
                                   INNER JOIN geography on notification.area_council = geography.acid
                                  INNER JOIN gender on notification.sex = gender.gender_id
                                  INNER JOIN source_of_health on notification.source = source_of_health.source_id
                                  GROUP BY  notification.province, notification.area_council, source_of_health.source, gender.gender
                                  ORDER BY geography.provid, geography.acid 
                               ") 

covid_sexpivot <- covid_sex %>%
  filter(COVID_19 != "NA") %>%
  pivot_wider(names_from = gender, values_from = COVID_19, values_fill = 0) %>%
  ungroup()

dbWriteTable(mydb, "covid_sexpivot", covid_sexpivot, overwrite=TRUE)

write.csv(covid_sexpivot,"C:\\Census 2020\\Population-Census-2020\\data\\open\\team1\\Table Number of Diseases suspected  of COVID 19 by sex, area council and source of notifications.csv", row.names = FALSE)





#5. Total number of Patients suspected of COVID and other diseases by source of health notification and area council


covid <- dbGetQuery(mydb, "SELECT id FROM notification WHERE COVID_19 > 0 ")


covid_undelying <- dbGetQuery(mydb, "SELECT notification.province, geography.pname, notification.area_council, geography.acname, source_of_health.source, 
                                 (Dengue_Virus + 
                                 Zika_Virus +  
                                 Chikungunya_Virus + 
                                 Meningococcal_Disease + 
                                 Diarrhoea_increase + 
                                 Cholera + 
                                 Pneumonia + 
                                 Influenza +  
                                 Measles_Rubella + 
                                 Pertusis +
                                 Tetanus + 
                                 Diphtheria + 
                                 Polio + 
                                 Typhoid + 
                                 Malaria + 
                                 Leptospirosis + 
                                 Tuberculosis + 
                                  Leprosy + 
                                  HIV + 
                                  Other_disease_symp) as underlying_disease
                                  FROM notification 
                                  INNER JOIN geography on notification.area_council = geography.acid
                                  INNER JOIN gender on notification.sex = gender.gender_id
                                  INNER JOIN source_of_health on notification.source = source_of_health.source_id
                                  INNER JOIN covid on notification.id = covid.id
                                  GROUP BY  notification.province, notification.area_council, source_of_health.source, notification.id 
                                  ORDER BY geography.provid, geography.acid 
                               
                               ") 

covid_sexOtherpivot <- covid_sexOther%>%
  filter(population != "NA") %>%
  pivot_wider(names_from = sex, values_from = population, values_fill = 0) %>%
  ungroup()

dbWriteTable(mydb, "covid_sexOtherpivot", covid_sexOtherpivot, overwrite=TRUE)

write.csv(covid_sexOtherpivot,"C:\\Census 2020\\Population-Census-2020\\data\\open\\team1\\Table Number of Patients suspected of COVID and other diseases by sex, area council and source of notifications.csv", row.names = FALSE)



#6. Type of Laboratory test requested by source of health notifications and by area council.


type_test <- dbGetQuery(mydb, "SELECT notification.province, geography.pname, notification.area_council, geography.acname, source_of_health.source,  
                                    SUM (Whole_Blood) as Whole_Blood,
                                    SUM (Urine) as Urine,
                                    SUM (Dried_Blood_Spot) as Dried_Blood_Spot, 
                                    SUM (covid_19_symp) as covid_19_symp,
                                    SUM (Other_test_symp) as Other_test_symp
                                    FROM notification
                                    INNER JOIN geography on notification.area_council = geography.acid
                                    INNER JOIN gender on notification.sex = gender.gender_id
                                    INNER JOIN source_of_health on notification.source = source_of_health.source_id
                                     GROUP BY  notification.province, notification.area_council, source_of_health.source, notification.id 
                                     ORDER BY geography.provid, geography.acid 
                               ") 



dbWriteTable(mydb, "type_test", type_test, overwrite=TRUE)

write.csv(type_test,"C:\\Census 2020\\Population-Census-2020\\data\\open\\team1\\Type of Laboratory test requested byarea council and source of notifications.csv", row.names = FALSE)



### Close ####
#Disconnect SQLite database
dbDisconnect(mydb)










