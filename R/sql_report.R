#' sql_report
#'
#' Report that wraps the functionality of [tessilake::read_sql] and [send_xlsx]
#'
#' @param report unsubscribe_report object
#' @param ... not used
#' @param customers integer vector of customer numbers to load
#' @name unsubscribe_report

unsubscribe_report <- new_report(class="unsubscribe_report")

#' @describeIn unsubscribe_report reads unsubscribe_report data
#' Loads data from
#' * p2: unsubscribes and hard bounces by list (uses p2_stream_enriched from tessistream)
#' * tessi: emails, addresses, logins, memberships, constituencies, MGOs (attributes)
#' @importFrom checkmate assert_class assert_integerish
#' @importFrom tessilake read_cache read_tessi
#' @importFrom dplyr filter collect
#' @importFrom data.table setDT
#' @export
