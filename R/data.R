# Project Groupings -------------------------------------------------------

# GPD_project_ids <- c(751, 776, 749, 1229, 127, 550)
#
# fake_projects <- c(1027, 1849, 1028, 1033, 1032, 1029, 1931, 1030, 1031, 1317)

#' @title Categories of HUD CSV Columns
#' @description Currently only groupings of Client columns
#' @export
col_cats = list(Client = list(
  gender = c(
    "Woman",
    "Man",
    "NonBinary",
    "CulturallySpecific",
    "Transgender",
    "Questioning",
    "DifferentIdentity",
    "GenderNone",
    "DifferentIdentityText"
  ),
  race = c(
    "AmIndAKNative",
    "Asian",
    "BlackAfAmerican",
    "HispanicLatinaeo",
    "MidEastNAfrican",
    "NativeHIPacific",
    "White",
    "RaceNone",
    "AdditionalRaceEthnicity"
  )
))

#' @title FY 2022 HUD CSV Data groupings
#' @description This list provides common groupings of specific data elements in the HUD CSV for ease of reference
#' @seealso destinations
#' @export

data_types = list(Project = list(ProjectType = list( # formerly project_types
  es = c(0, 1),
  th = 2,
  sh = 8,
  so = 6,
  lh = c(0, 1, 2, 8),
  lh_hp = c(0, 1, 2, 8, 12),
  lh_so_hp = c(0, 1, 2, 4, 8, 12),
  lh_at_entry = c(0, 1, 2, 3, 4, 8, 9, 13),
  lh_ph_hp = c(0, 1, 2, 3, 4, 8, 9, 12, 13),
  ph = c(3, 9, 13),
  psh = 3,
  rrh = 13,
  coc_funded = c(2, 3, 13),
  w_beds = c(0, 2, 3, 8, 9),
  ap = 14,
  ce = 14
)),
CurrentLivingSituation = list(CurrentLivingSituation = list( #formerly living_situations
  homeless = c(0, 116, 101, 118),
  likely_homeless = c(0, 161, 101, 118, 17, 207),
  institutional = c(215, 206, 207, 225, 204, 205),
  temp_psh = c(329, 314, 302, 332, 313, 336, 312, 422, 435, 423, 426, 327, 428, 419, 431, 433, 434, 410, 420, 421, 411),
  other = c(30, 17, 24, 37, 8, 9, 99)
)),
Exit = list(Destination = list( # formerly destinations
  perm = c(3, 411, 421, 435, 410, 426, 423, 422),
  temp = c(0, 1, 2, 302, 329, 312, 313, 314, 116, 101, 118, 327, 332),
  institutional = c(204:207, 215, 225, 327, 329),
  other = c(8, 9, 17, 24, 30, 37, 99)
)))

#' @inherit data_types title description
#' @export
project_types <- data_types$Project$ProjectType

#' @inherit data_types title description
#' @export
living_situations <- data_types$CurrentLivingSituation$CurrentLivingSituation

#' @inherit data_types title description
#' @export
destinations <- data_types$Exit$Destination

#' Colors for the various level of housing status used in the prioritization report
#' @export
prioritization_colors <- c(
  "Housed",
  "Entered RRH",
  "Permanent Housing Track",
  "Follow-up needed",
  "No current Entry",
  "Not referred",
  "No Entry"
) |>
  {\(x) {rlang::set_names(grDevices::colorRampPalette(c("#45d63e", "#ff2516"), space = "Lab")(length(x)), x)}}()




