#!/usr/bin/env Rscript

# Load necessary libraries
library(HMISprep)
library(HMISdata)
library(logger)
library(dplyr)
library(stringr)

# Set up logging
logger::log_info("Starting HMIS data processing job")

# Error handling wrapper
tryCatch({
  log_info("Processing HMIS data")
  # Client
  HMISprep::prep_client()
  gc(verbose = FALSE)

  # Project
  HMISprep::prep_project()
  gc(verbose = FALSE)

  # Disabilities
  Disabilities <- HMISdata::load_hmis_csv("Disabilities.csv", col_types = HMISdata::hmis_csv_specs$Disabilities)
  HMISdata::upload_hmis_data(Disabilities, file_name = "Disabilities.parquet", format = "parquet")
  rm(Disabilities)
  gc(verbose = FALSE)
  
  # Referrals
  HMISprep::prep_referrals()
  gc(verbose = FALSE)

  # Enrollment
  HMISprep::prep_enrollment()
  gc(verbose = FALSE)

  # Funder
  Funder <- HMISdata::load_hmis_csv("Funder.csv", col_types = HMISdata::hmis_csv_specs$Funder)
  HMISdata::upload_hmis_data(Funder, file_name = "Funder.parquet", format = "parquet")
  rm(Funder)
  gc(verbose = FALSE)

  # Health and DV
  HealthAndDV <- HMISdata::load_hmis_csv("HealthAndDV.csv", col_types = HMISdata::hmis_csv_specs$HealthAndDV)
  HMISdata::upload_hmis_data(HealthAndDV, file_name = "HealthAndDV.parquet", format = "parquet")
  rm(HealthAndDV)
  gc(verbose = FALSE)

  # Income Benefits
  IncomeBenefits <- HMISdata::load_hmis_csv("IncomeBenefits.csv", col_types = HMISdata::hmis_csv_specs$IncomeBenefits) |>
    dplyr::mutate(TotalMonthlyIncome = dplyr::if_else(IncomeFromAnySource == 0 & is.na(TotalMonthlyIncome),
                                                      0, TotalMonthlyIncome))
  HMISdata::upload_hmis_data(IncomeBenefits, file_name = "IncomeBenefits.parquet", format = "parquet")
  rm(IncomeBenefits)
  gc(verbose = FALSE)

  # Inventory
  Inventory <- HMISdata::load_hmis_csv("Inventory.csv", col_types = HMISdata::hmis_csv_specs$Inventory)
  HMISdata::upload_hmis_data(Inventory, file_name = "Inventory.parquet", format = "parquet")
  rm(Inventory)
  gc(verbose = FALSE)

  # Contacts
  Contacts <- HMISdata::load_looker_data(filename = "Contact", col_types = HMISdata::look_specs$Contact)
  HMISdata::upload_hmis_data(Contacts, file_name = "Contacts.parquet", format = "parquet")
  rm(Contacts)
  gc(verbose = FALSE)

  # Scores
  Scores <-  HMISdata::load_looker_data(filename = "Client_SPDAT", col_types = HMISdata::look_specs$Client_SPDAT) |>
    dplyr::filter(Deleted == "No") |>
    dplyr::mutate(Score = dplyr::if_else(is.na(Score), CustomScore, Score),
                  CustomScore = NULL) |>
    dplyr::mutate(Score = dplyr::if_else(stringr::str_detect(Name, "B-PAT"), Total, Score))
  HMISdata::upload_hmis_data(Scores, file_name = "Scores.parquet", format = "parquet")
  rm(Scores)
  gc(verbose = FALSE)

  # Users
  Users <- HMISdata::load_looker_data(filename = "User", col_types = HMISdata::look_specs$User)
  Users_link <- HMISdata::load_looker_data(filename = "UserNamesIDs", col_types = HMISdata::look_specs$UserNamesIDs)

  Users <- dplyr::left_join(Users, Users_link, by = c(UserID = "UserCreated"))
  HMISdata::upload_hmis_data(Users, file_name = "Users.parquet", format = "parquet")
  rm(Users, Users_link)
  gc(verbose = FALSE)

  # Services
  HMISprep::prep_services()
  gc(verbose = FALSE)

  # Cohorts
  HMISprep::prep_cohorts()
  gc(verbose = FALSE)

  # Program Lookup
  HMISprep::prep_program_lookup()
  gc(verbose = FALSE)

  logger::log_info("HMIS data processing completed successfully")
}, error = function(e) {
  logger::log_error("Error in HMIS data processing: {e$message}")
  # Optional: send notification about failure
  quit(status = 1)
})