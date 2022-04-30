#### Preparation ####

# Clear the environment
rm(list = ls())

# Load the required libraries
library(dplyr) #Data manipulation
library(readxl) #read in Excel files
library(tibble)
library(tidyverse)
library(RSQLite) #R SQLite driver package
library(DBI) #Database driver package
library(lubridate) #Data manipulation

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



#Disconnect SQLite database
dbDisconnect(mydb)










