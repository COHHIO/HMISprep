#' @title Load Enrollment as Enrollment_extra_Client_Exit_HH_CL_AaE
#'
#' @inheritParams data_quality_tables
#' @param Enrollment_extras From Clarity Looker API Extras
#'
#' @export
#'

prep_enrollment <- function(Enrollment,
                            EnrollmentCoC,
                            Enrollment_extras,
                            Exit,
                            Client = HMISdata::load_hmis_parquet("Client.parquet"),
                            Project,
                            rm_dates = load_dates()) {

  Enrollment = HMISdata::load_hmis_csv("Enrollment.csv", col_types = HMISdata::hmis_csv_specs$Enrollment)
  Enrollment_extras = HMISdata::load_looker_data(filename = "Enrollment", col_types = HMISdata::look_specs$Enrollment)
  Exit = HMISdata::load_hmis_csv("Exit.csv", col_types = HMISdata::hmis_csv_specs$Exit)
  Project = HMISdata::load_hmis_parquet("Project.parquet")
  Referrals = HMISdata::load_hmis_parquet("Referrals.parquet")

  # Veteran Client_extras ----
  VeteranCE <- HMISdata::load_looker_data(filename = "Client", col_types = HMISdata::look_specs$Client)
  load_dates()

  # getting EE-related data, joining both to Enrollment
  Enrollment_extra_Client_Exit_HH_CL_AaE <- dplyr::left_join(Enrollment, Enrollment_extras, by = UU::common_names(Enrollment, Enrollment_extras)) |>
    # Add Exit
    Enrollment_add_Exit(Exit) |>
    # Add Households
    Enrollment_add_Household(Project, rm_dates = rm_dates) |>
    # Add Veteran Coordinated Entry
    Enrollment_add_VeteranCE(VeteranCE = VeteranCE) |>
    # # Add Client Location from EnrollmentCoC
    # Enrollment_add_ClientLocation(EnrollmentCoC) |>
    # # Add Client AgeAtEntry
    Enrollment_add_AgeAtEntry_UniqueID(Client) |>
    dplyr::left_join(dplyr::select(Client,-dplyr::any_of(
      c(
        "DateCreated",
        "DateUpdated",
        "UserID",
        "DateDeleted",
        "ExportID"
      )
    )),
    by = c("PersonalID", "UniqueID")) |>
    Enrollment_add_HousingStatus()

  UU::join_check(Enrollment, Enrollment_extra_Client_Exit_HH_CL_AaE)

  HMISdata::upload_hmis_data(Enrollment_extra_Client_Exit_HH_CL_AaE,
                             file_name = "Enrollment_extra_Client_Exit_HH_CL_AaE.parquet", format = "parquet")

}
