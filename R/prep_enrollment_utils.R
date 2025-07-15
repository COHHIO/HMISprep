#' @title Add Exit data to Enrollments
#'
#' @param Enrollment \code{(data.frame)} HUD CSV Export Item
#' @param Exit \code{(data.frame)} HUD CSV Export Item
#'
#' @return \code{(data.frame)} Enrollments with `ExitDate`, `Destination`, `OtherDestination` and derived column `ExitAdjust`
#' @export

Enrollment_add_Exit <- function(Enrollment, Exit) {
  out <- Enrollment |>
    dplyr::left_join(
      Exit |>
        dplyr::select(EnrollmentID, ExitDate, Destination, DestinationSubsidyType,
                      OtherDestination), by = "EnrollmentID")  |>
    dplyr::mutate(ExitAdjust = dplyr::if_else(is.na(ExitDate) |
                                                ExitDate > Sys.Date(),
                                              Sys.Date(), ExitDate))

  UU::join_check(Enrollment, out)
  out
}

#' Add Household Information to Enrollment from Project
#'
#' @param Enrollment with Exit data. See `Enrollment_add_Exit`
#' @inheritParams data_quality_tables
#' @inheritParams R6Classes
#' @return \code{(data.frame)} of Enrollment with Household Columns `MoveInDateAdjust` appended
#' @export

Enrollment_add_Household = function(Enrollment, Project, rm_dates) {

  # getting HH information
  # only doing this for RRH and PSHs since Move In Date doesn't matter for ES, etc.

  small_project <- Project |>
    dplyr::select(ProjectID, ProjectType, ProjectName, FundingSourceCode,
                  NonFederalFundingSourceCode) |>
    dplyr::distinct()
  # TODO Check to see if Enrollment data has the MoveInDate
  # TODO Does Move-in Date in Clarity auto-populate from previous enrollments?
  HHMoveIn <- Enrollment |>
    dplyr::left_join(
      # Adding ProjectType to Enrollment too bc we need EntryAdjust & MoveInAdjust
      small_project,
      by = "ProjectID") |>
    dplyr::filter(ProjectType %in% c(3, 9, 13)) |>
    dplyr::mutate(
      AssumedMoveIn = dplyr::if_else(
        EntryDate < rm_dates$hc$psh_started_collecting_move_in_date &
          ProjectType %in% c(3, 9),
        1,
        0
      ),
      ValidMoveIn = dplyr::case_when(
        AssumedMoveIn == 1 ~ EntryDate,
        AssumedMoveIn == 0 &
          ProjectType %in% c(3, 9) &
          EntryDate <= MoveInDate &
          ExitAdjust > MoveInDate ~ MoveInDate,
        # the Move-In Dates must fall between the Entry and ExitAdjust to be
        # considered valid and for PSH the hmid cannot = ExitDate
        MoveInDate <= ExitAdjust &
          MoveInDate >= EntryDate &
          ProjectType == 13 ~ MoveInDate
      )
    ) |>
    dplyr::filter(!is.na(ValidMoveIn)) |>
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(HHMoveIn = min(ValidMoveIn, na.rm = TRUE)) |>
    dplyr::ungroup() |>
    dplyr::select(HouseholdID, HHMoveIn) |>
    unique()

  HHEntry <- Enrollment |>
    dplyr::left_join(small_project, by = "ProjectID") |>
    dplyr::group_by(HouseholdID) |>
    dplyr::mutate(FirstEntry = min(EntryDate, na.rm = TRUE)) |>
    dplyr::ungroup() |>
    dplyr::select(HouseholdID, "HHEntry" = FirstEntry) |>
    unique() |>
    dplyr::left_join(HHMoveIn, by = "HouseholdID")

  out <- Enrollment |>
    dplyr::left_join(small_project, by = "ProjectID") |>
    dplyr::left_join(HHEntry, by = "HouseholdID") |>
    dplyr::mutate(
      # Deprecated in Clarity ----
      # Fri Jan 28 21:05:08 2022
      # Puts EntryDate as MoveInDate for projects that don't use a MoveInDate
      MoveInDateAdjust = dplyr::case_when(
        EntryDate >= HHMoveIn ~ EntryDate,
        !is.na(HHMoveIn) & HHMoveIn <= ExitAdjust ~ HHMoveIn,
        TRUE ~ NA),
      EntryAdjust = EntryDate
    )
  UU::join_check(Enrollment, out)
  out
}

#' Add Veteran Coordinated Entry Date to Enrollments
#'
#' @param Enrollment that includes Exit Data. See `Enrollment_add_Exit`
#' @param VeteranCE HUD Extra that includes Veteran Coordinated Entry data
#'
#' @return \code{(data.frame)} Enrollment with the following columns added `PHTrack`, `ExpectedPHDate`, `ExitAdjust`
#' @export
Enrollment_add_VeteranCE = function(Enrollment, VeteranCE) {

  Enrollment |>
    # Join Veteran data
    dplyr::left_join(VeteranCE  |>
                       dplyr::select(EnrollmentID, PHTrack, ExpectedPHDate) |>
                       # dplyr::mutate(ExpectedPHDate = dplyr::if_else(ExpectedPHDate == as.Date("0000-00-00"), NA, ExpectedPHDate)) |> # readr automatically converts invalid dates to NA
                       dplyr::distinct(EnrollmentID, .keep_all = TRUE), by = "EnrollmentID") |>
    dplyr::mutate(
      ExitAdjust = dplyr::if_else(
        is.na(ExitDate) |
          ExitDate > lubridate::today(),
        lubridate::today(),
        ExitDate
      )
    )
}

#' @title Add AgeAtEntry to Enrollment
#' @description AgeAtEntry is the time elapsed from the Date of Birth `DOB` to the `EntryDate`
#' @inheritParams data_quality_tables
#'
#' @return \code{(data.frame)} Enrollment with `AgeAtEntry` column
#' @export

Enrollment_add_AgeAtEntry_UniqueID <- function(Enrollment, Client) {
  dplyr::left_join(Enrollment, dplyr::select(Client, UniqueID, PersonalID, DOB) |> dplyr::distinct(PersonalID, .keep_all = TRUE), by = c("PersonalID")) |>
    dplyr::mutate(AgeAtEntry = age_years(DOB, EntryDate)) |>
    dplyr::select(-DOB)
}

#' Add HousingStatus indicating the client's current housing status and Situation with details on that status.
#' @param Enrollment_extra_Client_Exit_HH_CL_AaE Custom Enrollment data frame
#' @param PH ProjectType codes considered Permanently Housed.  See `HMIS::hud_translations$ProjectType(table = TRUE)` & `?data_types`
#' @param Referrals Referrals data frame
#' @param prioritization_colors Custom data frame of prioritization colors
#' @return Input data with HousingStatus & Situation column
#' @export

Enrollment_add_HousingStatus <-
  function(Enrollment_extra_Client_Exit_HH_CL_AaE,
           PH = data_types$Project$ProjectType$ph,
           Referrals = HMISdata::load_hmis_parquet("Referrals.parquet")) {

    .nms <- names(Enrollment_extra_Client_Exit_HH_CL_AaE)
    .cols <- list(
      id = paste0(c("Personal", "Unique"), "ID"),
      req = c(
        "PersonalID",
        "ProjectName",
        "ProjectType",
        "ExpectedPHDate",
        "MoveInDateAdjust",
        "EntryDate",
        "PHTrack"
      ),
      ref = c(paste0(
        "R_",
        c(
          "ReferringPTC",
          "ReferringProjectName",
          "AcceptedDate",
          "CurrentlyOnQueue",
          "ConnectedMoveInDate"
        )
      ),
      "R_ReferredProjectName")
    )
    .cols$grp <- na.omit(
      stringr::str_extract(UU::common_names(Enrollment_extra_Client_Exit_HH_CL_AaE, Referrals), UU::regex_or(.cols$id))
    )[1]
    .cols$sym <- rlang::sym(.cols$grp)



    # if (!any(.cols$id %in% .nms) || !all(.cols$req %in% .nms))
    #   stop_with_instructions("data requires PersonalID or UniqueID & the following columns:\n", paste0(.cols$req, collapse = ",\n"))

    # Get the Last enrollment
    last_enroll <- Enrollment_extra_Client_Exit_HH_CL_AaE |>
      dplyr::group_by(PersonalID) |>
      dplyr::summarise(EnrollmentID = recent_valid(EnrollmentID, as.numeric(EnrollmentID)))

    out <- Enrollment_extra_Client_Exit_HH_CL_AaE |>
      # dplyr::filter(EnrollmentID %in% last_enroll$EnrollmentID) |>
      dplyr::select(!!.cols$sym, dplyr::any_of(.cols$req), EnrollmentID) |>
      dplyr::group_by(!!.cols$sym) |>
      # Get the latest entry
      # dplyr::slice_max(EntryDate, n = 1L) |>
      # apply human-readable status labels
      dplyr::mutate(PTCStatus = factor(
        dplyr::if_else(
          ProjectType %in% PH, "PH", "LH"
        ),
        levels =
          c("LH",
            "PH")
      ))

    referrals_expr <- rlang::exprs(
      housed = R_ExitHoused == "Housed",
      is_last = R_IsLastReferral == "Yes",
      is_last_enroll = R_IsLastEnrollment == "Yes",
      is_active = R_ActiveInProject == "Yes",
      accepted = stringr::str_detect(R_ReferralResult, "accepted$"),
      coq = R_ReferralCurrentlyOnQueue == "Yes"
    )
    referral_result_summarize <- purrr::map(referrals_expr, ~rlang::expr(isTRUE(any(!!.x, na.rm = TRUE))))

    # Create a summary of last referrals & whether they were accepted
    # Get housed
    # Don't want queue status to determine if client shows up on prioritization / vets report
    .housed <- Referrals |>
      dplyr::group_by(!!.cols$sym) |>
      # summarise specific statuses: accepted, housed or currently on queue
      dplyr::summarise(housed = !!referral_result_summarize$housed,
                       .groups = "drop") |>
      dplyr::filter(housed)



    out <- out |>
      dplyr::mutate(housed = !!.cols$sym %in% .housed[[.cols$grp]])


    if (!all(.cols$ref %in% .nms))
      out <- dplyr::left_join(out,
                              # Remove R_ReferralResult because the computation in Looker is bugged. A person can be simultaneously Accepted & Rejected
                              Referrals |> dplyr::mutate_all(as.character) |>
                                dplyr::select( - R_ReferralResult) |>
                                dplyr::distinct(R_ReferralID, .keep_all = TRUE),
                              by = UU::common_names(out, Referrals))


    sit_expr = rlang::exprs(
      ph_date = !is.na(ExpectedPHDate),
      ph_date_pre = Sys.Date() < ExpectedPHDate,
      ph_date_post = Sys.Date() > ExpectedPHDate,
      ptc_has_entry = PTCStatus == "PH",
      ptc_no_entry = PTCStatus == "LH",
      is_ph = (R_ReferringPTC %|% ProjectType) %in% data_types$Project$ProjectType$ph,
      is_lh = (R_ReferringPTC %|% ProjectType) %in% c(data_types$Project$ProjectType$lh, 4, 11),
      moved_in = !is.na(MoveInDateAdjust) & MoveInDateAdjust >= EntryDate,
      referredproject = !is.na(R_ReferredProjectName),
      referreddate = !is.na(R_ReferralAcceptedDate),
      ph_track = !is.na(PHTrack) & PHTrack != "None"
    )

    out <- dplyr::mutate(
      out |> dplyr::mutate(ProjectType = as.character(ProjectType)),
      MoveInDateAdjust = as.Date(MoveInDateAdjust),
      EntryDate = as.Date(EntryDate),
      R_ReferralAcceptedDate = as.Date(R_ReferralAcceptedDate),
      # ExpectedPHDate = dplyr::if_else(is.na(ExpectedPHDate), R_ReferralConnectedMoveInDate, ExpectedPHDate),
      Situation = dplyr::case_when(
        housed ~ "Housed",
        (!!sit_expr$ptc_has_entry | !!sit_expr$is_ph) & !!sit_expr$moved_in ~ "Housed",
        (!!sit_expr$ptc_has_entry | !!sit_expr$is_ph) & !(!!sit_expr$moved_in) ~ paste("Entered RRH/PSH but has not moved in:",
                                                                                       R_ReferringProjectName %|% ProjectName),
        !!sit_expr$ph_track &
          !!sit_expr$ph_date &
          !!sit_expr$ph_date_pre ~ paste("Permanent Housing Track. Track:", PHTrack,"Expected Move-in:", ExpectedPHDate),
        !!sit_expr$ph_track &
          !!sit_expr$ph_date &
          !!sit_expr$ph_date_post &
          !(!!sit_expr$moved_in) ~ paste("Follow-up needed on PH Track, client is not yet moved in:", PHTrack,"Expected Move-in:", ExpectedPHDate),
        # Clients with an accepted referral but no entry in RRH/PSH
        !!sit_expr$ptc_no_entry &
          !!sit_expr$referredproject &
          !!sit_expr$referreddate ~
          paste(
            "No current Entry into RRH or PSH but",
            R_ReferredProjectName,
            "accepted this household's referral on",
            R_ReferralAcceptedDate
          ),
        # Clients with a referral but no accepted date
        !!sit_expr$ptc_no_entry &
          !!sit_expr$referredproject &
          !(!!sit_expr$referreddate) ~
          paste(
            "No current Entry into RRH or PSH but was referred to",
            R_ReferredProjectName,
            "on",
            R_ReferredDate
          ),
        !R_ReferralCurrentlyOnQueue == "Yes" | is.na(R_ReferralCurrentlyOnQueue) ~ "Not referred to Community Queue, may need referral to CQ.",
        !!sit_expr$ptc_no_entry &
          !(!!sit_expr$referredproject) &
          !(!!sit_expr$ph_track) ~
          "No Entry or accepted Referral into PSH/RRH, and no current Permanent Housing Track",
        TRUE ~ "No Entry or accepted Referral into PSH/RRH, and no current Permanent Housing Track"
      ),
      HousingStatus = factor(stringr::str_extract(Situation, UU::regex_or(names(prioritization_colors))), levels = names(prioritization_colors)),
      housed = NULL
    ) |>
      dplyr::select(- dplyr::any_of(c(.cols$req, "UniqueID")))

    out <-
      dplyr::left_join(
        Enrollment_extra_Client_Exit_HH_CL_AaE,
        dplyr::select(out,-PTCStatus),
        by = c(.cols$grp, "EnrollmentID" = "EnrollmentID")
      ) |>
      dplyr::select(PersonalID, HousingStatus, Situation, dplyr::everything()) |>
      dplyr::distinct()

    return(out)
  }

# Age Function ------------------------------------------------------------

age_years <- function(earlier, later)
{
  floor(lubridate::time_length(lubridate::interval(earlier, later), "years"))
}


#' @title Retrieve the most recent valid value
#' Given a timeseries `t` and a `v`, return the most recent valid value
#' @param t \code{(Date/POSIXct)}
#' @param v \code{(vector)}
#'
#' @return \code{(vector)} of unique, most recent value(s if there is a tie) of `v`
#' @export

recent_valid <- function(t, v) {
  unique(v[t == valid_max(t[!is.na(v)])])
}

#' @title Find a valid maximum from the input values, or if none is present, return the values
#'
#' @param x \code{(numeric/real)}
#'
#' @return Maximum value
#' @export

valid_max = function(x) {
  out <- na.omit(x)
  if (UU::is_legit(out)) {
    out2 <- max(out)
    if (!is.infinite(out2))
      out <- out2
  } else {
    out <- x
  }
  out
}
