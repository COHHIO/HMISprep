#' @title Load Project & extras pre-requisites for `load_export`
#'
#' @inheritParams data_quality_tables
#' @param Regions From public data. See `load_public`
#' @param ProjectCoC From HUD CSV export
#' @inherit load_export return
#' @export

prep_project <- function() {
  Regions <- HMISdata::Regions

  # Project_extras -----------------------------------------------------------------
  ProjectCoC <- HMISdata::load_hmis_csv("ProjectCoC.csv",
                                        col_types = HMISdata::hmis_csv_specs$ProjectCoC)

  # Project_extras -----------------------------------------------------------------
  provider_extras <- HMISdata::load_looker_data(filename = "Project", col_types = HMISdata::look_specs$Project)

  provider_extras <- pe_add_ProjectType(provider_extras) |>
    pe_add_regions(Regions, dirs = dirs) |>
    pe_add_GrantType()

  # Add HMISParticipation (HMISParticipating column no longer in Project.csv)
  HMISParticipation <- HMISdata::load_hmis_csv("HMISParticipation.csv",
                                               col_types = HMISdata::hmis_csv_specs$HMISParticipation) |>
    dplyr::select(HMISParticipationID, ProjectID, HMISParticipationType,
                  HMISParticipationStatusStartDate, HMISParticipationStatusEndDate) |>
    dplyr::group_by(ProjectID) |>
    dplyr::filter(HMISParticipationStatusStartDate == max(HMISParticipationStatusStartDate)) |>
    dplyr::ungroup()

  provider_extras <- provider_extras |>
    dplyr::mutate(ProjectID = as.character(ProjectID)) |>
    dplyr::left_join(HMISParticipation, by = "ProjectID")

  # Rminor: Coordinated Entry Access Points [CEAP]
  APs <- pe_create_APs(provider_extras, ProjectCoC, dirs = dirs)


  .Project <- HMISdata::load_hmis_csv("Project.csv", col_types = HMISdata::hmis_csv_specs$Project) |>
    dplyr::mutate(ProjectID = as.character(ProjectID))

  Project <- .Project |>
    dplyr::select(-ProjectCommonName) |>
    {\(x) {dplyr::left_join(x, provider_extras |>
                              dplyr::select(- dplyr::matches("FundingSourceID")) |>
                              dplyr::distinct(ProjectID, .keep_all = TRUE),
                            by = UU::common_names(x, provider_extras))}}()
  UU::join_check(.Project, Project)

  mahoning_projects <- dplyr::filter(ProjectCoC, CoCCode %in% "OH-504") |>
    dplyr::select(ProjectID) |>
    dplyr::left_join(dplyr::select(Project, ProjectID, ProjectTypeCode, ProjectName), by = "ProjectID") |>
    Project_rm_zz() |>
    dplyr::distinct(ProjectID, .keep_all = TRUE)

  HMISdata::upload_hmis_data(Project, file_name = "Project.parquet", format = "parquet")
  HMISdata::upload_hmis_data(APs, file_name = "APs.parquet", format = "parquet")
  HMISdata::upload_hmis_data(mahoning_projects, file_name = "mahoning_projects.parquet", format = "parquet")
}


#' @title Add ProjectType (dbl) to provider_extras
#'
#' @inheritParams pe_create_APs
#' @return \code{(data.frame)}

pe_add_ProjectType <- function(provider_extras) {
  PT <- HMIS::hud_translations$`2.02.6 ProjectType`(table = TRUE) |>
    tibble::add_row(Value = 12, Text = "Homeless Prevention")

  purrr::map_dbl(provider_extras$ProjectTypeCode, ~PT$Value[agrepl(stringr::str_remove(.x, "\\s\\([\\w\\s]+\\)$"), PT$Text)])
  provider_extras |>
    dplyr::rowwise() |>
    dplyr::mutate(ProjectType = PT$Value[agrepl(stringr::str_remove(ProjectTypeCode, "\\s\\([\\w\\s]+\\)$"), PT$Text)], .after = "ProjectTypeCode")
}

#' @title Add the Corresponding Region for each Project by way of Geocode matching
#'
#' @description This function adds a `Regions` column to the `provider_extras` data frame by matching geocodes.
#' It uses data from the `hud_load` function from the `clarity.looker` package to retrieve region information.
#'
#' @param provider_extras \code{(data.frame)} An extra from the Program Descriptor model. See `?clarity_api` for retrieving a Look with this info.
#' @param Regions \code{(data.frame)} Data frame containing region information. Defaults to loading from `clarity.looker::hud_load("Regions", dirs$public)`.
#' @param dirs \code{(list)} List of directories used in the function, including `dirs$public`.
#'
#' @return \code{(data.frame)} `provider_extras` with an additional `Regions` column.
#' @export

pe_add_regions <- function(provider_extras, Regions = HMISdata::Regions, dirs) {
  # geocodes is created by `hud.extract` using the hud_geocodes.R functions
  geocodes <- HMISdata::Geocodes
  # This should map a county to every geocode
  out <- provider_extras |>
    dplyr::left_join(geocodes |> dplyr::select(GeographicCode, County), by = c(Geocode = "GeographicCode")) |>
    dplyr::filter(!Geocode %in% c("000000", "429003", "399018"))


  out <- out |>
    dplyr::left_join(Regions |> dplyr::select(- RegionName), by = "County") |>
    dplyr::rename(ProjectRegion = "Region",
                  ProjectCounty = "County")

  .need_filled <- out |>
    dplyr::filter(is.na(ProjectCounty)) |>
    dplyr::distinct(Geocode, .keep_all = TRUE) |>
    nrow()
  if (.need_filled > 0)
    cli::cli_warn(cli::cli_format("Some geocodes did not match a county. See {.path .deprecated/fill_geocodes.R} for a function to fix this issue."))
  # Special cases
  #  St. Vincent de Paul of Dayton serves region 13
  out[out$Geocode %in% c("391361", "391362"), "ProjectRegion"] <- 13

  # Missing Regions
  # missing_region <- out |>
  #   dplyr::filter(is.na(ProjectRegion) & ProgramCoC == "OH-507")
  # missing_region |>
  #   dplyr::pull(ProjectCounty) |>
  #   unique()

  out |> dplyr::filter(!is.na(ProjectRegion))
}


#' @title Add GrantType column to provider_extras
#' @description GrantType indicates if the program is funded by one of HOPWA, PATH, SSVF, or RHY
#'
#' @inheritParams pe_create_APs
#'
#' @return A data frame containing providers with numeric codes replaced.
#' @export
pe_add_GrantType <- function(provider_extras) {
  hash <- HMIS::hud_translations$`2.06.1 FundingSource`(table = TRUE)

  gt <- list(HOPWA = c(13:19), PATH = 21, SSVF = 33, RHY = 22:26)
  provider_extras |>
    dplyr::mutate(GrantType = dplyr::case_when(
      FundingSourceCode %in% gt$HOPWA ~ "HOPWA",
      FundingSourceCode %in% gt$PATH ~ "PATH",
      FundingSourceCode %in% gt$SSVF ~ "SSVF",
      FundingSourceCode %in% gt$RHY ~ "RHY"
    ))
}

#' @title Add Access Points to Provider_extras
#' @description Create a data frame of Coordinated Entry Access Points with information about the Counties & Populations Served.
#'
#' @param provider_extras \code{(data.frame)} provider_extras with Regions, see `pe_add_regions`.
#' @param ProjectCoC \code{(data.frame)} Data frame containing information about the Project Continuum of Care (CoC).
#' @param dirs \code{(list)} List of directories used in the function.
#'
#' @return \code{(data.frame)} A data frame of Coordinated Entry Access Points with Counties & Populations Served.
#' @export

pe_create_APs = function(provider_extras, ProjectCoC, dirs) {

  Regions <- HMISdata::Regions

  APs <- provider_extras |>
    dplyr::select( !tidyselect::starts_with("CoCComp") & !Geocode:ZIP) |>
    dplyr::filter(ProjectTypeCode == "Coordinated Entry") |>
    tidyr::pivot_longer(tidyselect::starts_with("AP"), names_to = "TargetPop", names_pattern = "(?<=^APCounties)(\\w+)", values_to = "CountiesServed") |>
    dplyr::filter(!is.na(CountiesServed)) |>
    dplyr::select(!tidyselect::starts_with("AP") & !ProjectTypeCode)

  project_addresses <- ProjectCoC |>
    dplyr::select(ProjectID, CoCCode, Address1, Address2, City, State, ZIP) |>
    dplyr::mutate(ProjectID = as.character(ProjectID)) |>
    dplyr::distinct() |>
    dplyr::mutate(
      City = paste0(City, ", ", State, " ", ZIP),
      Addresses = dplyr::coalesce(Address1, Address2)
    )

  # Programs serve multiple Counties which may fall into multiple regions. This creates a row for each Region served by a Program such that Coordinated Entry Access Points will show all the appropriate programs when filtering by Region.
  # @Rm
  APs <-
    if (nrow(APs) == 0) {
      # Return an empty dataframe with desired column names
      APs <- tibble::tibble(
        ProjectID = integer(),
        OrganizationName = character(),
        ProjectName = character(),
        TargetPop = character(),
        ProjectCountyServed = character(),
        Hours = character(),
        Phone = character(),
        OrgLink = character(),
        CoCCode = character(),
        Addresses = character(),
        City = character()
      )
    } else {
      purrr::pmap_dfr(APs, ~{
        .x <- tibble::tibble(...)
        .counties <- trimws(stringr::str_split(.x$CountiesServed, ",\\s")[[1]])

        .x |>
          dplyr::select(- ProjectRegion) |>
          dplyr::bind_cols(Region = unique(Regions$Region[Regions$County %in% .counties]))
      }) |>
        dplyr::distinct_all() |>
        dplyr::mutate(OrgLink = dplyr::if_else(!is.na(Website), paste0(
          "<a href='",
          Website,
          "' target='_blank'>",
          ProjectName,
          "</a><small> (#",
          ProjectID,
          ")</small>"
        ), paste0(ProjectName,
                  "<small> (#",
                  ProjectID,
                  ")</small>"))) |>
        dplyr::left_join(project_addresses, by = "ProjectID")  |>
        dplyr::select(ProjectID, OrganizationName, ProjectName, TargetPop,
                      "ProjectCountyServed" = CountiesServed
                      #, ProjectAreaServed
                      , Hours, Phone, OrgLink, CoCCode, Addresses, City)
    }

  APs
}

#' @title Filter Projects that have been retired (`zz`'d)
#'
#' @inheritParams data_quality_tables
#'
#' @return A data frame containing projects that don't start with "zz".
#' @export
Project_rm_zz <- function(Project) {
  dplyr::filter(Project, stringr::str_detect(ProjectName, "^zz", negate = TRUE))
}
