#' @title Count number of households in the dataset. Used in `prep_cohorts`
#'
#' @param served \code{(data.frame)}
#'
#' @return \code{(data.frame)} With a summary column using \link[dplyr]{n} named similarly to the input object name
#' @export

chrt_hoh_count <- function(served) {
  count_colname <- stringr::str_remove(rlang::expr_deparse(rlang::enexpr(served)), "co\\_")
  served |>
    dplyr::distinct(PersonalID, ProjectName) |>
    dplyr::group_by(ProjectName) |>
    dplyr::summarise(!!count_colname := dplyr::n())
}

#' @title Quickly perform filter & select routine used in `cohorts`
#'
#' @description All _*_between_ filters use `rm_dates$calc$data_goes_back_to` as the beginning of the period and `rm_dates$meta_HUDCSV$Export_End` as the end. See `dates` for details on these values.
#' @param x \code{(data.frame)} Must include columns necessary for the _*_between_ function used or any additional filter expressions supplied.
#' @param ... \code{(expression)}s used to filter data
#' @param stayed \code{(logical)} Uses \link[HMIS]{stayed_between}
#' @param served \code{(logical)} Uses \link[HMIS]{served_between}
#' @param exited \code{(logical)} Uses \link[HMIS]{exited_between}
#' @param entered \code{(logical)} Uses \link[HMIS]{entered_between}
#' @inheritParams data_quality_tables
#' @param vars \code{(character)} vector of column names to retrain
#' @inheritParams HMIS::between_df
#' @param app_env \code{(environment)} The application environment, defaults to the result of `get_app_env(e = rlang::caller_env())`.
#'
#' @return \code{(data.frame)} Output with filter and select applied.
#' @export

chrt_filter_select <- function(x,
                               ...,
                               stayed = FALSE,
                               served = FALSE,
                               exited = FALSE,
                               entered = FALSE,
                               rm_dates,
                               vars,
                               start = rm_dates$calc$data_goes_back_to,
                               end = rm_dates$meta_HUDCSV$Export_End,
                               app_env = get_app_env(e = rlang::caller_env())
) {

  addtl_filters <- rlang::enexprs(...)

  out <- x

  if (served)
    out <- out |> HMIS::served_between(start, end)
  if (stayed)
    out <- out |> HMIS::stayed_between(start, end)
  if (exited)
    out <- out |> HMIS::exited_between(start, end)
  if (entered)
    out <- out |> HMIS::entered_between(start, end)

  if (UU::is_legit(addtl_filters))
    out <- dplyr::filter(out, !!!addtl_filters)

  out |>
    dplyr::select(dplyr::all_of(vars))
}

