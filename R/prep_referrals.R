#' @title Load Referrals
#' @description Loads Referrals, Referrals_full (unfiltered), and referral_result_summarize with filtering expressions used later
#' @inherit data_quality_tables params return
#' @export

prep_referrals <- function()
{

  Referrals <- HMISdata::load_looker_data(filename = "CE_Referrals_new",
                                                      col_types = HMISdata::look_specs$CE_Referrals_new) |>
    dplyr::rename_with(.cols = - dplyr::matches("(?:^PersonalID)|^(?:^UniqueID)"), rlang::as_function(~paste0("R_",.x))) |>
    dplyr::mutate(R_ReferringPTC = stringr::str_remove(R_ReferringPTC, "\\s\\(disability required(?: for entry)?\\)$"),
                  R_ReferringPTC = dplyr::if_else(R_ReferringPTC == "Homeless Prevention", "Homelessness Prevention", R_ReferringPTC),
                  R_ReferringPTC = dplyr::if_else(R_ReferringPTC == "", NA_character_, R_ReferringPTC),
                  R_ReferringPTC = HMIS::hud_translations$`2.02.6 ProjectType`(R_ReferringPTC))

  # Full needed for dqu_aps
  Referrals_full <- Referrals

  referrals_expr <- rlang::exprs(
    housed = R_ExitHoused == "Housed",
    is_last = R_IsLastReferral == "Yes",
    is_last_enroll = R_IsLastEnrollment == "Yes",
    is_active = R_ActiveInProject == "Yes",
    accepted = stringr::str_detect(R_ReferralResult, "accepted$"),
    coq = R_ReferralCurrentlyOnQueue == "Yes"
  )
  referral_result_summarize <- purrr::map(referrals_expr, ~rlang::expr(isTRUE(any(!!.x, na.rm = TRUE))))

  Referrals <- Referrals |>
    dplyr::rename(R_ReferralResult = "R_Coordinated Entry Event Referral Result") |>
    filter_dupe_soft(!!referrals_expr$is_last_enroll,
                     !!referrals_expr$is_last,
                     !!referrals_expr$is_active,
                     !is.na(R_ReferralResult),
                     !!referrals_expr$housed & !!referrals_expr$accepted,
                     key = PersonalID)

  HMISdata::upload_hmis_data(Referrals, file_name = "Referrals.parquet", format = "parquet")
  HMISdata::upload_hmis_data(Referrals_full, file_name = "Referrals_full.parquet", format = "parquet")
}


#' @title Filter duplicates without losing any values from `key`
#'
#' @param .data \code{(data.frame)} Data with duplicates
#' @param ... \code{(expressions)} filter expressions with which to filter
#' @param key \code{(name)} of the column key that will be grouped by and for which at least one observation will be preserved.
#'
#' @return \code{(data.frame)} without duplicates
#' @export

filter_dupe_soft <- function(.data, ..., key) {
  .key <- rlang::enexpr(key)
  out <- .data
  x <- janitor::get_dupes(.data, !!.key) |>
    dplyr::arrange(PersonalID)

  clients <- dplyr::pull(x, !!.key) |> unique()
  .exprs <- rlang::enquos(...)
  to_add <- list()
  for (ex in .exprs) {
    new <- dplyr::filter(x, !!ex)

    new_n <- dplyr::summarise(dplyr::group_by(new, !!.key), n = dplyr::n())
    .to_merge <- dplyr::filter(new_n, n == 1) |> dplyr::pull(!!.key)

    # if some were reduced but not to one
    .reduced <- dplyr::left_join(new_n,
                                 dplyr::summarise(dplyr::group_by(x, !!.key), n = dplyr::n()), by = rlang::expr_deparse(.key), suffix = c("_new", "_old")) |>
      dplyr::filter(n_new < n_old & n_new > 1) |>
      dplyr::pull(!!.key)

    if (UU::is_legit(.to_merge)) {
      # remove rows where key is reduced to one, bind the deduplicated rows
      to_add <- append(to_add, list(dplyr::select(new, -dupe_count) |>
                                      dplyr::filter(!!.key %in% .to_merge)))
      # filter to_merge from dupes
      x <- dplyr::filter(x, !((!!.key) %in% .to_merge))
    }

    if (UU::is_legit(.reduced)) {
      # filter reduced from dupes
      x <- dplyr::filter(x, !((!!.key) %in% .reduced ) # is not one that was reduced
                         | (!!.key %in% .reduced & !!ex) # or matched the filter
      )

    }
  }
  to_add <- dplyr::bind_rows(to_add)
  out <- dplyr::filter(out, !(!!.key %in% c(to_add[[.key]], x[[.key]]))) |>
    dplyr::bind_rows(to_add, x) |>
    dplyr::select(-dplyr::any_of("dupe_count"))

  if (anyDuplicated(out[[.key]])) {
    rlang::warn("Duplicates still exist.")
  }
  out
}
