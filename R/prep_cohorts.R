# Some definitions:
# PH = PSH + RRH
# household = one or more people who present for housing/homeless services
# served = the Entry to Exit Date range crosses the Report Date range
# entered = the Entry Date is inside the Report Date range
# served_leaver = (regardless of Move-In) the Exit Date is inside the Report
#     Date range
# moved_in_leaver = a subset of served_leaver, these stays include a Move-In Date
#     where that's relevant (PH projects)
# moved_in = any stay in a non-PH project where the Entry to Exit Date range
#     crosses the Report Date range PLUS any stay in a PH project where the
#     Move In Date to the Exit Date crosses the Report Date range
# hohs = heads of household
# adults = all adults in a household
# clients = all members of the household
#' @export

prep_cohorts <- function(Enrollment_extra_Client_Exit_HH_CL_AaE,
                    rm_dates = load_dates()) {
  Enrollment_extra_Client_Exit_HH_CL_AaE <- HMISdata::load_hmis_parquet("Enrollment_extra_Client_Exit_HH_CL_AaE.parquet")

  vars <- list(we_want = c(
    "PersonalID",
    "UniqueID",
    "EnrollmentID",
    "DOB",
    "CountyServed",
    "ProjectName",
    "ProjectID",
    "ProjectType",
    "HouseholdID",
    "AgeAtEntry",
    "RelationshipToHoH",
    "VeteranStatus",
    "EntryDate",
    "EntryAdjust",
    "MoveInDate",
    "MoveInDateAdjust",
    "ExitDate",
    "ExitAdjust",
    "Destination"
  ))


  # Transition Aged Youth
  tay <-  Enrollment_extra_Client_Exit_HH_CL_AaE |>
    dplyr::select(tidyselect::all_of(vars$we_want)) |>
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(
      TAY = dplyr::if_else(max(AgeAtEntry) < 25 & max(AgeAtEntry) >= 16, 1, 0)
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(TAY == 1 & !is.na(ProjectName))

  #	Leavers and Stayers	who were Served During Reporting Period	All
  co_clients_served <-  Enrollment_extra_Client_Exit_HH_CL_AaE  |>
    chrt_filter_select(served = TRUE,
                       vars = vars$we_want,
                       rm_dates = rm_dates)

  # Leaver and Stayer HoHs who were served during the reporting period
  co_hohs_served <-  co_clients_served  |>
    dplyr::filter(RelationshipToHoH == 1)

  #	Leavers and Stayers	who were Served During Reporting Period	Adults
  co_adults_served <-  co_clients_served |>
    dplyr::filter(AgeAtEntry > 17)

  #	Leavers and Stayers	who	Entered During Reporting Period	HoHs
  co_hohs_entered <-  Enrollment_extra_Client_Exit_HH_CL_AaE |>
    chrt_filter_select(entered = TRUE,
                       vars = vars$we_want,
                       rm_dates = rm_dates) |>
    dplyr::filter(RelationshipToHoH == 1)

  #	Leavers	who were Served During Reporting Period (and Moved In)	All
  co_clients_moved_in_leavers <-  Enrollment_extra_Client_Exit_HH_CL_AaE |>
    chrt_filter_select(exited = TRUE,
                       stayed = TRUE,
                       vars = vars$we_want,
                       rm_dates = rm_dates)

  #	Leaver hohs	who were Served (and Moved In) During Reporting Period	HoHs
  co_hohs_moved_in_leavers <-  co_clients_moved_in_leavers |>
    dplyr::filter(RelationshipToHoH == 1)

  #	Leavers	who were Served During Reporting Period (and Moved In)	Adults
  co_adults_moved_in_leavers <-  co_clients_moved_in_leavers |>
    dplyr::filter(AgeAtEntry > 17)

  HMISdata::upload_hmis_data(tay, file_name = "tay.parquet", format = "parquet")
  HMISdata::upload_hmis_data(co_clients_served, file_name = "co_clients_served.parquet", format = "parquet")
  HMISdata::upload_hmis_data(co_hohs_served, file_name = "co_hohs_served.parquet", format = "parquet")
  HMISdata::upload_hmis_data(co_adults_served, file_name = "co_adults_served.parquet", format = "parquet")
  HMISdata::upload_hmis_data(co_hohs_entered, file_name = "co_hohs_entered.parquet", format = "parquet")
  HMISdata::upload_hmis_data(co_clients_moved_in_leavers, file_name = "co_clients_moved_in_leavers.parquet", format = "parquet")
  HMISdata::upload_hmis_data(co_hohs_moved_in_leavers, file_name = "co_hohs_moved_in_leavers.parquet", format = "parquet")
  HMISdata::upload_hmis_data(co_adults_moved_in_leavers, file_name = "co_adults_moved_in_leavers.parquet", format = "parquet")
}


