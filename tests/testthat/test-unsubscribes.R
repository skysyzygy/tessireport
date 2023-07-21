withr::local_package("checkmate")
withr::local_package("mockery")

customers <- c(8993321,8992917,8992918)
fixture <- readRDS(rprojroot::find_testthat_root_file("unsubscribe_report.Rds"))


# read.unsubscribe_report -------------------------------------------------

test_that("read.unsubscribe_report loads email, address, MGO, membership, and login data", {

  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)

  expect_names(names(report), must.include = c("memberships", "MGOs", "emails", "email_events", "addresses", "logins",
                                               "constituencies", "customers"))
  expect_true(all(sapply(report,nrow) > 0))
})

# process.unsubscribe_report ----------------------------------------------

test_that("process.unsubscribe_report identifies email unsubscribes and bounces", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)

  report$email_events[,`:=`(event_subtype = rep(c("Open","Click","Send"), length.out = .N),
                            listid = NA)]
  setorder(report$email_events, customer_no, timestamp)
  report$email_events[,`:=`(I=1:.N,N=.N), by = "customer_no"]
  report$email_events[I==N, `:=`(event_subtype = c("Hard Bounce","Soft Bounce","Unsubscribe"),
                                 listid = c(NA,NA,1))]
  report$email_events[I==N-1,`:=`(event_subtype = c("Hard Bounce","Soft Bounce","Unsubscribe"),
                                  listid = c(NA,NA,2))]

  report <- process(report)

  expect_equal(report$report[grepl("Unsubscribe", message),.N], 2) # list 1 and list 2
  expect_equal(report$report[grepl("Bounce", message),.N], 2)
})

test_that("process.unsubscribe_report identifies missing emails", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)
  report$emails <- report$emails[customer_no != 8992918]
  report <- process(report)

  expect_equal(report$report[grepl("No primary email", message),.N], 1)
})

test_that("process.unsubscribe_report identifies bad addresses", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)
  report$addresses[,last_updated_by:=c("me","you","NCOA$DNM")]
  report <- process(report)

  expect_equal(report$report[grepl("Bad primary mailing address", message),.N], 1)
})

test_that("process.unsubscribe_report identifies missing addresses", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)
  report$addresses <- report$addresses[customer_no != 8992918]
  report <- process(report)

  expect_equal(report$report[grepl("No primary mailing address", message),.N], 1)
})

test_that("process.unsubscribe_report identifies non-matching logins", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)
  report$logins[,primary_ind:="Y"]
  report <- process(report)

  expect_equal(report$report[grepl("Email does not match login", message),.N], 2)
})

test_that("process.unsubscribe_report identifies inactive customers", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)
  report$customers[1,inactive_desc := "Inactive"]
  report <- process(report)

  expect_equal(report$report[grepl("Customer is inactive", message),.N], 1)
})

test_that("process.unsubscribe_report has one row per issue", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)


  report$email_events[,`:=`(event_subtype = rep(c("Open","Click","Send"), length.out = .N),
                            listid = NA)]
  setorder(report$email_events, customer_no, timestamp)
  report$email_events[,`:=`(I=1:.N,N=.N), by = "customer_no"]
  report$email_events[I==N, `:=`(event_subtype = c("Hard Bounce","Soft Bounce","Unsubscribe"),
                                 listid = c(NA,NA,1))]
  report$email_events[I==N-1,`:=`(event_subtype = c("Hard Bounce","Soft Bounce","Unsubscribe"),
                                  listid = c(NA,NA,2))]
  report$emails <- report$emails[customer_no != 8992918]
  report$addresses[,last_updated_by:=c("me","you","NCOA$DNM")]
  report$addresses <- report$addresses[customer_no != 8992917]
  report$logins[,primary_ind:="Y"]
  report$customers[1,inactive_desc := "Inactive"]

  report <- process(report)

  expect_equal(report$report[,.N],9) # 4 bad emails, 1 missing, 1 bad mailing address, 1 missing, 1 bad login, 1 inactive.
})

test_that("process.unsubscribe_report includes timestamp, membership, MGO, name, and constituency data", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)
  report <- process(report)

  expect_names(colnames(report$report), permutation.of = c("message", "timestamp", "memb_level", "expr_dt", "customer_no", "MGOs",
                                                           "name", "constituencies"))
  expect_false(any(sapply(report$report,\(x) all(is.na(x)))))
})

# output.unsubscribe_report -----------------------------------------------

test_that("output.unsubscribe_report filters based on since and until", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(unsubscribe_report(), customers)
  report <- process(report)

  expect_names(colnames(report$report), permutation.of = c("message", "timestamp", "memb_level", "expr_dt", "customer_no", "MGOs",
                                                           "name", "constituencies"))
  expect_false(any(sapply(report$report,\(x) all(is.na(x)))))
})

test_that("output.unsubscribe_report routes based on MGO and constituency", {
})
