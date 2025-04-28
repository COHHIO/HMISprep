#' @title Upload redacted client table to AWS
#'
#' @inheritParams data_quality_tables
#' @inherit load_export return
#' @export

prep_client <- function() {
  Client <- HMISdata::load_hmis_csv("Client.csv",
                                    col_types = HMISdata::hmis_csv_specs$Client)
  Client <- Client_redact(Client)

  Client_UniqueID <- HMISdata::load_looker_data(filename = "Client_UniqueID", col_types = HMISdata::look_specs$Client_UniqueID)

  Client <- Client_add_UniqueID(Client, Client_UniqueID)

  HMISdata::upload_hmis_data(Client, file_name = "Client.parquet", format = "parquet")
}

#' @title Redact PII from Client HUD Export
#' @description This redacts all PII (except DOB) from Client HUD Export
#' @family Client functions
#' @inheritParams data_quality_tables
#' @return \code{(tibble)} Redacted Client HUD Export
#' @export

Client_redact <- function(Client) {
  Client |>
    Client_filter() |>
    dplyr::mutate(
      FirstName = dplyr::case_when(
        NameDataQuality %in% c(8, 9) ~ "DKR",
        NameDataQuality == 2 ~ "Partial",
        NameDataQuality == 99 |
          is.na(NameDataQuality) |
          FirstName == "Anonymous" ~ "Missing",
        !(
          NameDataQuality %in% c(2, 8, 9, 99) |
            is.na(NameDataQuality) |
            FirstName == "Anonymous"
        ) ~ "ok"
      ),
      LastName = NULL,
      MiddleName = NULL,
      NameSuffix = NULL,
      SSN = dplyr::case_when(
        substr(SSN, 1, 5) == "00000" ~ "Four Digits Provided",
        (is.na(SSN) & !SSNDataQuality %in% c(8, 9)) |
          is.na(SSNDataQuality) | SSNDataQuality == 99 ~ "Missing",
        SSNDataQuality %in% c(8, 9) ~ "DKR",
        # (nchar(SSN) != 9 & SSNDataQuality != 2) |
        substr(SSN, 1, 3) %in% c("000", "666") |
          substr(SSN, 1, 1) == 9 |
          substr(SSN, 4, 5) == "00" |
          substr(SSN, 6, 9) == "0000" |
          SSN %in% c(
            111111111,
            222222222,
            333333333,
            444444444,
            555555555,
            777777777,
            888888888,
            123456789
          ) ~ "Invalid"
      )
    ) |>
    dplyr::mutate(SSN = dplyr::case_when(is.na(SSN) ~ "ok",!is.na(SSN) ~ SSN))
}

#' @title Add the UniqueID to the Client export
#'
#' @param Client \code{(data.frame)} A data frame containing client information
#' @param Client_UniqueIDs \code{(data.frame)} A custom look linking PersonalID & UniqueID
#' @param app_env \code{(environment)} The application environment, defaults to the result of `get_app_env(e = rlang::caller_env())`.
#'
#' @return \code{(data.frame)} with UniqueID column
#' @export

Client_add_UniqueID <- function(Client, Client_UniqueIDs) {
  names(Client_UniqueIDs) <- c("UniqueID", "PersonalID")

  out <- dplyr::left_join(Client, dplyr::distinct(Client_UniqueIDs, PersonalID, UniqueID), by = "PersonalID")
  UU::join_check(Client, out)
  out
}

#' @title Filter out specific Clients
#' @description Often used to filter test/training demo clients
#' @param x \code{(data.frame)} With PersonalID or UniqueID column
#' @param clients \code{(character)} of PersonalIDs to filter with names corresponding their UniqueIDs (Clarity only)
#' @family Client functions
#' @return \code{(data.frame)} without `clients_to_filter`
#' @export

Client_filter <- function(x, clients = getOption("HMIS")$clients_to_filter) {

  if (is.data.frame(x) && UU::is_legit(clients)) {
    nms <- na.omit(stringr::str_extract(colnames(x), UU::regex_or(c("PersonalID", "UniqueID"))))
    if (UU::is_legit(nms))
      for (nm in nms) {
        x <- dplyr::filter(x, !(!!rlang::sym(nm)) %in% !!purrr::when(nm, . == "PersonalID" ~ clients, ~ names(clients)))
      }
  }

  x
}
