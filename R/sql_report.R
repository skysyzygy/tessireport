#' sql_report
#'
#' Report that wraps the functionality of [tessilake::read_sql] and [send_xlsx]
#'
#' @param report sql_report object
#' @param ... additional parameters passed on to [tessilake::read_sql] and [send_xlsx]
#' @name sql_report
#' @note Useful additional parameters include:
#' * `subject`: character subject of the email, default is `sql_report` and the current date.
#' * `body`: character body of the email, default is a human readable message indicating the computer name.
#' * `emails`: character email addresses to send the email to (first will be sender as well)
#' * `dry_run`: boolean do not send the email if TRUE
#' * `html`: boolean indicating whether the body of the email should be parsed as HTML. Default is `TRUE`.
#' * `inline`: boolean indicating whether images in the HTML file should be embedded inline.
#' * `file.names`: character name of the filename to show in the email. The default is `sql_report_<today's date>.xlsx`
#' * `freshness`: difftime, the returned data will be at least this fresh.

sql_report <- new_report(class="sql_report")

#' @describeIn sql_report reads sql_report data
#' @param query character sql query to generate the report
#' @importFrom tessilake read_sql
#' @importFrom checkmate expect_character
#' @importFrom dplyr collect
#' @importFrom utils modifyList
#' @export
read.sql_report <- function(report, query, ...) {
  . <- NULL

  expect_character(query, len = 1)

  # compute args from the formals of `read_sql` and `...` plus `query` and enforcing non-incremental loads
  args <- modifyList(rlang::list2(...), list(query=query, incremental=FALSE)) %>%
    .[intersect(names(.), rlang::fn_fmls_names(tessilake::read_sql))]

  report$data <- do.call(read_sql,args) %>% collect

  report
}


#' @describeIn sql_report sends sql_report data
#'
#' @importFrom tessilake read_sql
#' @export
output.sql_report <- function(report, query, ...) {
  sql_report <- report$data

  send_xlsx(table = sql_report, ...)

  report
}

process.sql_report <- write.sql_report <- function(x,...) x
