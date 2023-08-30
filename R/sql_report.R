#' sql_report
#'
#' Report that wraps the functionality of [tessilake::read_sql] and [send_xlsx]
#'
#' @param report sql_report object
#' @param ... not used
#' @name sql_report

sql_report <- new_report(class="sql_report")

#' @describeIn sql_report reads sql_report data
#' Loads data from sql
#' * p2: unsubscribes and hard bounces by list (uses p2_stream_enriched from tessistream)
#' * tessi: emails, addresses, logins, memberships, constituencies, MGOs (attributes)
#' @importFrom tessilake read_sql
#' @export
read.sql_report <- function(query) {
  expect_character(query, len = 1)

}
