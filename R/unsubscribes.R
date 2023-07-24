#' unsubscribe_report
#'
#' Reports on unsubscribes and bad addresses for a group of customers
#'
#' @param report report object
#' @param customers integer vector of customer numbers to load
#' @name unsubscribe_report

NULL

#' @describeIn unsubscribe_report creates a new unsubscribe_report object
unsubscribe_report <- \() new_report(class="unsubscribe_report")

#' @describeIn unsubscribe_report reads unsubscribe_report data
#' Loads data from
#' * p2: unsubscribes and hard bounces by list (uses p2_stream_enriched from tessistream)
#' * tessi: emails, addresses, logins, memberships, constituencies, MGOs (attributes)
#' @param ... not used
#' @importFrom checkmate assert_class assert_integerish
#' @importFrom tessilake read_cache read_tessi
#' @importFrom dplyr filter collect
#' @importFrom data.table setDT
#' @export
read.unsubscribe_report <- function(report, customers, ...) {
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
#' * Checks if emails are unsubscribed, bounced, or don't exist
#' * Checks if mailing addresses have a NCOA flag or don't exist
#' * Checks if the customer primary login doesn't match the primary email
#' * Checks if customers are inactive
#' * Adds identifying info: MGO, constituencies, membership expiration date and level
#' @param ... not used
#' @importFrom dplyr left_join if_else
#' @export
process.unsubscribe_report <- function(report, ...) {
  assert_class(report, "unsubscribe_report")

  # Email issues
  primary_emails <- report$emails[primary_ind == "Y"]
  latest_email_events <- setkey(report$email_events,customer_no,timestamp) %>%
                               .[event_subtype != c("Open","Click"), .SD[.N],
                                 by = list(customer_no, ifelse(event_subtype == "Unsubscribe", listid, NA)),
                                 .SDcols = colnames(report$email_events)]
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

  report$report <- rbind(bad_emails, missing_emails, bad_addresses, missing_addresses, bad_logins, inactive,
               fill = TRUE) %>%
    left_join(latest_memberships[,.(customer_no, memb_level, expr_dt)], by = "customer_no") %>%
    left_join(MGOs, by = "customer_no") %>%
    left_join(constituencies, by = "customer_no") %>%
    left_join(name, by = "customer_no")

  report

}

#' @describeIn unsubscribe_report send the unsubscribe_report as emails to MGOs
#' Routes based on the following rules:
#' * MGO/PSD signatory -> send to users with matching name
#' * GOV -> send to Government Affairs (cluna)
#' * CP# -> send to Strategic Partnerships (ashah)
#' * Patron -> send to Patron Program (jhindle)
#' * Other -> send to Dev Ops (kburke)
#' @param ... not used
#' @importFrom tessilake read_sql
#' @importFrom dplyr case_when
#' @export
output.unsubscribe_report <- function(report, since = Sys.Date() - 30, until = Sys.Date() + 30, ...) {
  assert_class(report, "unsubscribe_report")

  filtered_report <- report$report[timestamp > since | expr_dt > since & expr_dt < until,
                                   .(`Tessi #`=customer_no, name, message, timestamp, memb_level, expr_dt, MGOs, constituencies)]

  # routing rules

  filtered_report[,I:=.I]
  users <- read_sql("select userid, fname, lname from T_METUSER where inactive = 'N'") %>% collect %>% lapply(trimws) %>% setDT
  filtered_report <- merge(filtered_report, users[,.(I = grep(paste0(fname,".+",lname),filtered_report$MGOs)),by="userid"], by = "I", all = T)
  filtered_report$I <- NULL

  filtered_report[is.na(userid), userid := case_when(grepl("GOV", constituencies) ~ "eleszynski",
                                                     grepl("CP\\d", constituencies) ~ "lmcgee",
                                                     grepl("\\+",constituencies) ~ c("apratama","jhindle"),
                                                     TRUE ~ c("kburke","esearles"))]

  filtered_report[,userid := paste0(userid, "@bam.org")]
  split(filtered_report, by = "userid", keep.by = FALSE) %>%
    purrr::iwalk(send_unsubscribe_report_table)

}

#' send_unsubscribe_report_table
#'
#' @param table data.table to send
#' @param email character email address to send the email to
#' @param dry_run boolean do not send the email if TRUE
#' @importFrom checkmate assert_data_table assert_character
send_unsubscribe_report_table <- function(table, email, dry_run = FALSE) {
  assert_data_table(table)
  assert_character(email, len = 1)

  filename <- tempfile("unsubscribe_report",fileext = ".xlsx")
  tessistream:::write_xlsx(table, filename)
  if(!dry_run)
    tessistream:::send_email(subject = paste("Contact warning report for",Sys.Date()),
                             emails = c(config::get("tessiflow.email"), email),
                             body = "
                             <p>Hi,
                             <p>This is an report of contact issues for constituents your portfolio.
                             <p>Please contact Sky or Kathleen if you have any questions.
                             <p>Sincerely,
                             <p>Sky's computer",
                             html = TRUE,
                             attach.files = filename,
                             file.names = paste0("contact_report_",Sys.Date(),".xlsx"))

}
