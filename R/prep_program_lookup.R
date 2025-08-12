#' @title Fetch Program Lookup table
#'
#' @param program_lookup Custom program data frame
#'
#' @return \code{(tbl)}
#' @export

prep_program_lookup <- function() {
  program_lookup <- HMISdata::load_looker_data(filename = "Program_lookup",
                                               col_types = HMISdata::look_specs$Program_lookup)

  program_lookup <- program_lookup |>
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
    make_linked_df(ProgramName, type = "program_edit") |>
    make_linked_df(AgencyName, type = "agency_switch") |>
    make_linked_df(AgencyAdministrator, type = "admin")

  HMISdata::upload_hmis_data(program_lookup, file_name = "program_lookup.parquet", format = "parquet")
}

#' @title Make UniqueID or EnrollmentID into a Clarity hyperlink
#' @param .data \code{(data.frame)} The following columns are required for the specified link type:
#' \itemize{
#'   \item{\code{PersonalID & UniqueID}}{ for Profile link}
#'   \item{\code{PersonalID & EnrollmentID}}{ for Enrollment link}
#'   \item{\code{PersonalID & EnrollmentID}}{ for Enrollment link}
#' }
#' @param link_text \code{(name)} unquoted of the column to unlink.
#' @param unlink \code{(logical)} Whether to turn the link back into the respective columns from which it was made.
#' @param new_ID \code{(name)} unquoted of the column to be created with the data from the linked column. (`PersonalID` will be recreated automatically if it doesn't exist).
#' @inheritParams make_link
#' @return \code{(data.frame)} With `UniqueID` or `EnrollmentID` as a link
#' data.frame(a = letters, b = seq_along(letters)) |>  dplyr::mutate(a = make_link(a, b)) |> make_linked_df(a, unlink = TRUE)
#' @export
make_linked_df <- function(.data, link_text, unlink = FALSE, new_ID, type = NULL, chr = TRUE) {
  stopifnot(!is.null(names(.data)))
  if (nrow(.data) == 0)
    return(.data)
  out <- .data
  .data_nm <- rlang::expr_deparse(rlang::call_args(match.call())$.data)
  link_text <- rlang::enexpr(link_text)
  has_new_ID <- !missing(new_ID)
  if (has_new_ID)
    new_ID <- rlang::enexpr(new_ID)
  link_chr <- rlang::expr_deparse(link_text)

  .type <- link_type(type, link_text, rlang::expr_deparse(link_text), new_ID)
  ID <- switch(link_chr,
               UniqueID = "PersonalID",
               EnrollmentID = "PersonalID",
               ProjectName = "ProjectID",
               ProgramName = "ProgramID",
               AgencyName = "AgencyID",
               AgencyAdministrator = "AgencyAdministrator") %||% switch(rlang::expr_deparse(new_ID),
                                                                        UniqueID = "PersonalID",
                                                                        EnrollmentID = "PersonalID",
                                                                        ProjectName = "ProjectID",
                                                                        ProgramName = "ProgramID",
                                                                        AgencyName = "AgencyID",
                                                                        AgencyAdministrator = "AgencyAdministrator")
  .col <- .data[[link_text]]
  if (is.null(.col))
    rlang::abort(glue::glue("{link_chr} not found in `.data`"), trace = rlang::trace_back())

  if (unlink) {
    # TODO handle shiny.tag
    if (is_link(.col)) {
      if (!ID %in% names(.data))
        out[[ID]] <- stringr::str_extract(.col, switch(.type,
                                                       enrollment = ,
                                                       profile = "(?<=clients?\\/)\\d+",
                                                       program_edit = "(?<=edit\\/)\\d+",
                                                       agency_switch = "(?<=switch\\/)\\d+"),
                                          admin = "(?<=edit\\/)\\d+")

      if (has_new_ID)
        link_text <- new_ID
      out[[link_text]] <- stringr::str_extract(.col, switch(.type,
                                                            agency_switch = ,
                                                            program_edit = ,
                                                            admin = ,
                                                            profile = "(?<=\\>)[:alnum:]+(?=\\<)",
                                                            enrollment = "\\d+(?=\\/enroll)"))
    } else
      rlang::warn(glue::glue("{.data_nm}: `{link_chr}` is not a link"))

  } else {
    if (is_link(.col)) {
      rlang::inform(glue::glue("{.data_nm}: `{link_chr}` is already a link."))
    } else {
      ID <- rlang::sym(ID)
      out <- .data |>
        dplyr::mutate(!!link_text := make_link(!!ID, !!link_text, chr = chr, type = .type))
    }
  }

  out
}

link_type <- function(x, link_text, link_chr, new_ID) {
  .type <- x %||% switch(link_chr,
                         UniqueID = "profile",
                         EnrollmentID = "enrollment",
                         ProjectName = "program_edit",
                         ProgramName = "program_edit",
                         AgencyName = "agency_switch",
                         AgencyAdministrator = "admin") %||% switch(rlang::expr_deparse(new_ID),
                                                                    UniqueID = "profile",
                                                                    EnrollmentID = "enrollment",
                                                                    ProjectName = "program_edit",
                                                                    ProgramName = "program_edit",
                                                                    AgencyName = "agency_switch",
                                                                    AgencyAdministrator = "admin") %||%
    ifelse(any(stringr::str_detect(link_text, "[A-F]"), na.rm = TRUE), "profile", "enrollment")
  UU::match_letters(.type, "profile", "enrollment", "program_edit", "agency_switch", "admin", n = 5)
}


is_link <- function(.col) {
  any(stringr::str_detect(.col, "^\\<a"), na.rm = TRUE) || isTRUE(try(inherits(.col[[1]], "shiny.tag"), silent = TRUE))
}

make_link <- function(ID, link_text, type = NULL, chr = TRUE) {
  href <- getOption("HMIS")$Clarity_URL %||% "https://cohhio.clarityhs.com"
  .type = link_type(type, link_text, rlang::expr_deparse(link_text))
  sf_args <- switch(
    .type,
    profile = list(
      "<a href=\"%s/client/%s/profile\" target=\"_blank\">%s</a>",
      href,
      ID,
      link_text
    ),
    enrollment = list(
      "<a href=\"%s/clients/%s/program/%s/enroll\" target=\"_blank\">%s</a>",
      href,
      ID,
      link_text,
      link_text
    ),
    agency_switch = list(
      "<a href=\"%s/manage/agency/switch/%s\" target=\"_blank\">%s</a>",
      href,
      ID,
      link_text
    ),
    admin = list(
      "<a href=\"%s/manage/staff/edit/%s\" target=\"_blank\">%s</a>",
      href,
      ID,
      link_text
    ),
    program_edit = list(
      "<a href=\"%s/manage/program/edit/%s\" target=\"_blank\">%s</a>",
      href,
      ID,
      link_text)
  )

  if (chr) {
    out <- do.call(sprintf, sf_args)
  } else {
    href <- httr::parse_url(href)
    if (!identical(length(ID), length(link_text))) {
      l <- list(PersonalID = ID, ID = link_text)
      big <- which.max(purrr::map_int(l, length))
      i <- seq_along(l)
      small <- subset(i, subset = i != big)
      assign(names(l)[small], rep(l[[small]], length(l[[big]])))
    }
    out <- purrr::map2(ID, link_text, ~{
      href$path <- switch(.type,
                          profile = c("client",.x, "profile"),
                          enrollment = c("clients",.x, "program", .y, "enroll"),
                          agency_switch = c("manage","agency", "switch", .x),
                          program_edit = c("manage","program", "edit", .x),
                          admin = c("manage", "staff", "edit", .x)
      )
      htmltools::tags$a(href = httr::build_url(href), .y, target = "_blank")
    })
  }
  out
}
