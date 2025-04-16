#' @title Load Services as Services_enroll_extras
#'
#' @inherit data_quality_tables params return
#' @param Enrollment_extra_Client_Exit_HH_CL_AaE Custom Enrollment data frame
#'
#' @export
prep_services <- function() {
  tictoc::tic()
  Services = HMISdata::load_hmis_csv(file_name = "Services.csv", col_types = HMISdata::hmis_csv_specs$Services)
  Services_extras = HMISdata::load_looker_data(filename = "Services", col_types = HMISdata::look_specs$Services)
  Enrollment_extra_Client_Exit_HH_CL_AaE = HMISdata::load_hmis_parquet("Enrollment_extra_Client_Exit_HH_CL_AaE.parquet")

  # Create an in-memory DuckDB connection
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  # Get common column names (equivalent to UU::common_names)
  common_cols <- intersect(names(Services), names(Services_extras))

  # Select columns from Enrollment data
  Enrollment_subset <- Enrollment_extra_Client_Exit_HH_CL_AaE |>
    dplyr::select(
    "EnrollmentID",
    "PersonalID",
    "ProjectName",
    "EntryDate",
    "ExitAdjust",
    "ProjectID"
  )

  # Register tables with DuckDB
  duckdb::duckdb_register(con, "Services", Services)
  duckdb::duckdb_register(con, "Services_extras", Services_extras)
  duckdb::duckdb_register(con, "Enrollment_subset", Enrollment_subset)



  Services_enroll_extras <- dplyr::tbl(con, "Services") |>
    dplyr::left_join(dplyr::tbl(con, "Services_extras"), by = common_cols) |>
    dplyr::left_join(dplyr::tbl(con, "Enrollment_subset"), by = c("PersonalID", "EnrollmentID")) |>
    dplyr::distinct() |>
    dplyr::mutate(ServiceEndAdjust = ifelse(is.na(ServiceEndDate) | ServiceEndDate > Sys.Date(),
                                            Sys.Date(),
                                            ServiceEndDate)) |>
    dplyr::select(
      "UniqueID",
      "PersonalID",
      "ServiceID",
      "EnrollmentID",
      "ProjectName",
      "HouseholdID",
      "ServiceStartDate",
      "ServiceEndDate",
      "RecordType",
      "ServiceItemName",
      "FundName",
      "FundingSourceID",
      "ServiceAmount"
    ) |>
    dplyr::collect()


  duckdb::duckdb_unregister(con, "Services")
  duckdb::duckdb_unregister(con, "Services_extra")
  duckdb::duckdb_unregister(con, "Enrollment_subset")

  DBI::dbDisconnect(con)

  HMISdata::upload_hmis_data(Services_enroll_extras,
                             file_name = "Services_enroll_extras.parquet", format = "parquet")
  tictoc::toc()

}
