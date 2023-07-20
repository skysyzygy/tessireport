#' unsubscribe_report
#'
#' Reports on unsubscribes and bad addresses for a group of customers
#'
#' @param report report object
#' @param customers integer vector of customer numbers to load
#' @name unsubscribe_report

NULL

#' @describeIn unsubscribe_report creates a new unsubscribe_report object
new_unsubscribe_report <- \() new_report(class="unsubscribe_report")

#' @describeIn unsubscribe_report reads unsubscribe_report data
#' @importFrom checkmate assert_class assert_integerish
#' @importFrom tessilake read_cache read_tessi
#' @importFrom dplyr filter collect
#' @importFrom data.table setDT
#' @export
read.unsubscribe_report <- function(report, customers) {
  assert_class(report, "unsubscribe_report")
  assert_integerish(customers)

  # load email/unsubscribe data
  report$email_events <- read_cache("p2_stream_enriched","deep","stream")
  report$emails <- read_tessi("emails")

  # load address data
  report$addresses <- read_tessi("addresses")

  # load login data
  report$logins <- read_tessi("logins")

  # load MGO data
  report$MGOs <- read_tessi("attributes") %>%
    filter(grepl("Major Gifts|PSD signatory",keyword_desc))

  # load constituency data
  report$constituencies <- read_tessi("constituencies")

  # load membership data
  report$memberships <- read_tessi("memberships")

  # load name data
  report$customers <- read_tessi("customers")

  for(obj in names(report))
    report[[obj]] <-
                filter(report[[obj]], customer_no %in% customers) %>%
                collect %>% setDT

  report
}

#' @describeIn unsubscribe_report process unsubscribe_report data
#' @importFrom dplyr left_join if_else
#' @export
process.unsubscribe_report <- function(report) {
  assert_class(report, "unsubscribe_report")

  # Email issues
  primary_emails <- report$emails[primary_ind == "Y"]
  latest_email_events <- setkey(report$email_events,customer_no,timestamp) %>%
                               .[event_subtype != c("Open","Click"), .SD[.N], by = "customer_no"]
  bad_emails <- rbind(latest_email_events[grepl("Unsub", event_subtype),
                               .(customer_no, message = paste(event_subtype,"from",list_name), timestamp)],
                      latest_email_events[grepl("Bounce", event_subtype),
                                          .(customer_no, message = event_subtype, timestamp)])

  missing_emails <- report$customers[!primary_emails,on = "customer_no",
                               .(customer_no, message = "No primary email")]

  # Mailing address issues
  primary_addresses <- report$addresses[primary_ind == "Y"]
  bad_addresses <- primary_addresses[last_updated_by == "NCOA$DNM",
                                .(customer_no, message = "Bad primary mailing address", timestamp = last_update_dt)]
  missing_addresses <- report$customers[!primary_addresses,on = "customer_no",
                                .(customer_no, message = "No primary mailing address")]

  # Login issues
  bad_logins <- primary_emails[report$logins[primary_ind == "Y"], on = "customer_no"][trimws(tolower(login)) != trimws(tolower(address)),
                 .(customer_no, message = "Email does not match login", timestamp = pmax(last_update_dt, i.last_update_dt))]

  # Inactive customers
  inactive <- report$customers[inactive_desc != "Active",
                               .(customer_no, message = paste("Customer is inactive:", inactive_reason_desc), timestamp = last_update_dt)]

  #Additional info
  MGOs <- report$MGOs[,.(MGOs = paste(keyword_value, collapse = ", ")), by = "customer_no"]
  constituencies <- report$constituencies[,.(constituencies = paste(constituency_short_desc, collapse = ", ")), by = "customer_no"]
  latest_memberships <- setkey(report$memberships,expr_dt,memb_amt) %>%
    .[!current_status_desc %in% c("Cancelled", "Deactivated"), .SD[.N], by = "customer_no"]
  name <- report$customers[,.(name = paste(na.omit(c(fname, lname)), collapse = " ")), by = "customer_no"]

  out <- rbind(bad_emails, missing_emails, bad_addresses, missing_addresses, bad_logins, inactive,
               fill = TRUE) %>%
    left_join(latest_memberships[,.(customer_no, memb_level, expr_dt)], by = "customer_no") %>%
    left_join(MGOs, by = "customer_no") %>%
    left_join(constituencies, by = "customer_no") %>%
    left_join(name, by = "customer_no")

  list(report = out)

}
