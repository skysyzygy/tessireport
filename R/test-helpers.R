make_unsubscribe_report_fixtures <- function() {
  report <- list()
  report$email_events <- read_cache("p2_stream_enriched","deep","stream") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect
  report$emails <- read_tessi("emails") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect
  report$addresses <- read_tessi("addresses") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect
  report$logins <- read_tessi("logins") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect
  report$MGOs <- read_tessi("attributes") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect
  report$constituencies <- read_tessi("constituencies") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect
  report$memberships <- read_tessi("memberships") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect
  report$customers <- read_tessi("customers") %>% filter(customer_no %in% c(8993321,8992917,8992918)) %>% collect

  saveRDS(report,rprojroot::find_testthat_root_file("unsubscribe_report.Rds"))
}
