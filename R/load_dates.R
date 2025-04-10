#' Load hard coded dates
#'
#' @param error
#'
#' @return List of dates used in Rm apps
#' @export
#'
#' @examples
load_dates <- function(error = FALSE) {
  rm_dates <- list()
  first <- lubridate::floor_date(Sys.Date(), "year")

  # List of hard coded dates
  hc <- list(
    data_goes_back_to =  first - lubridate::years(2)
  )

  hc$unsheltered_data_start <- hc$data_goes_back_to

  # the default ReportStart for DQ reporting
  hc$check_dq_back_to <-  lubridate::make_date(lubridate::year(hc$data_goes_back_to), month = 10, day = 1)
  hc$outreach_to_cls <- hc$check_dq_back_to
  hc$check_eligibility_back_to <- hc$check_dq_back_to - lubridate::years(3)
  hc$spm_range <- lubridate::interval(hc$check_dq_back_to, lubridate::`year<-`(hc$check_dq_back_to, lubridate::year(hc$check_dq_back_to) + 1) - lubridate::days(1))
  hc$project_eval_start = hc$data_goes_back_to + lubridate::years(1)
  hc$project_eval_end = lubridate::ceiling_date(hc$project_eval_start, "year") - lubridate::days(1)
  hc$project_eval_docs_due = lubridate::make_date(lubridate::year(hc$project_eval_end), 4, 23)
  hc$lsa_range <- hc$check_dq_back_to |>
    {\(x) {lubridate::interval(x, x + lubridate::years(1) - lubridate::days(1))}}()

  rm_dates$hc <- append(hc, purrr::map(
    c(psh_started_collecting_move_in_date = "10012017",
      began_requiring_spdats = "01012019",
      prior_living_situation_required = "10012016",
      no_more_svcs_on_hh_members = "02012019"
    ),
    lubridate::mdy
  ))

  rm(hc)

  # Dates from Metadata -----------------------------------------------------

  Export <- HMISdata::load_hmis_csv("Export.csv", col_types = HMISdata::hmis_csv_specs$Export)

  rm_dates$meta_HUDCSV <- list(
    Export_Date = Export[["ExportDate"]][1],
    Export_Start = Export[["ExportStartDate"]][1],
    Export_End = Export[["ExportEndDate"]][1]
  )

  rm(Export)

  # Calculated Dates --------------------------------------------------------
  Exit <- HMISdata::load_hmis_csv("Exit.csv", col_types = HMISdata::hmis_csv_specs$Exit)

  rm_dates$calc <- list(data_goes_back_to =
                          Exit %>%
                          dplyr::arrange(ExitDate) %>%
                          utils::head(1) %>%
                          dplyr::pull(ExitDate))


  rm(Exit)

  rm_dates$calc$full_date_range <-
    lubridate::interval(rm_dates$meta_HUDCSV$Export_End,
                        rm_dates$calc$data_goes_back_to)

  rm_dates$calc$two_yrs_prior_end <-
    lubridate::floor_date(Sys.Date(), "month") - lubridate::days(1)
  rm_dates$calc$two_yrs_prior_start <-
    lubridate::as_date(lubridate::floor_date(rm_dates$calc$two_yrs_prior_end, "month") - lubridate::years(2) + lubridate::dmonths(1))

  rm_dates$calc$two_yrs_prior_range <- lubridate::interval(rm_dates$calc$two_yrs_prior_start,
                                                           rm_dates$calc$two_yrs_prior_end)

  if(rm_dates$meta_HUDCSV$Export_Start > rm_dates$hc$data_goes_back_to |
     rm_dates$meta_HUDCSV$Export_End != Sys.Date())
    stop_with_instructions("The HUD CSV Export is not up to date", error = error)

  gc()

  return(rm_dates)
}
