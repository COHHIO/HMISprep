#' @title Fetch Program Lookup table
#'
#' @param program_lookup Custom program data frame
#'
#' @return \code{(tbl)}
#' @export

prep_program_lookup <- function() {
  program_lookup <- HMISdata::load_looker_data(filename = "Program_lookup",
                                               col_types = HMISdata::look_specs$Program_lookup)

  program_lookup |>
    dplyr::mutate(dplyr::across(c(dplyr::ends_with("Active")), ~dplyr::if_else(.x %in% c("Active"), TRUE, FALSE))) |>
    dplyr::rename(AgencyAdministrator = `Property Manager`,
                  StartDate = `Start Date`,
                  EndDate = `End Date`,
                  LastUpdatedDate = `Last Updated Date`) |>
    dplyr::group_by(ProgramName) |>
    dplyr::filter(StartDate == max(StartDate)) |>
    dplyr::ungroup() |>
    dplyr::arrange(dplyr::desc(AgencyAdministrator)) |>
    dplyr::distinct(ProgramID, ProgramName, .keep_all = TRUE) |>
    clarity.looker::make_linked_df(ProgramName, type = "program_edit") |>
    clarity.looker::make_linked_df(AgencyName, type = "agency_switch") |>
    clarity.looker::make_linked_df(AgencyAdministrator, type = "admin")

  HMISdata::upload_hmis_data(program_lookup, file_name = "program_lookup.parquet", format = "parquet")
}
