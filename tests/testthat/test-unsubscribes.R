withr::local_package("checkmate")
withr::local_package("mockery")

customers <- c(8993321,8992917,8992918)
fixture <- readRDS(rprojroot::find_testthat_root_file("unsubscribe_report.Rds"))

test_that("read.unsubscribe_report loads email, address, MGO, membership, and login data", {

  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)

  expect_names(names(report), must.include = c("memberships", "MGOs", "emails", "email_events", "addresses", "logins",
                                               "constituencies", "customers"))
  expect_true(all(sapply(report,nrow) > 0))
})

test_that("process.unsubscribe_report identifies email unsubscribes and bounces", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)

  report$email_events[,event_subtype := rep(c("Open","Click","Send"), length.out = .N)]
  setorder(report$email_events, customer_no, timestamp)
  report$email_events[,`:=`(I=1:.N,N=.N), by = "customer_no"]
  report$email_events[I==N,event_subtype := c("Hard Bounce","Soft Bounce","Unsubscribe")]

  report <- process(report)

  expect_equal(report$report[grepl("Unsubscribe", message),.N], 1)
  expect_equal(report$report[grepl("Bounce", message),.N], 2)
})

test_that("process.unsubscribe_report identifies missing emails", {
  fixture$emails <- fixture$emails[customer_no != 8992918]

  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)
  report <- process(report)

  expect_equal(report$report[grepl("No primary email", message),.N], 1)
})

test_that("process.unsubscribe_report identifies bad addresses", {
  fixture$addresses[,last_updated_by:=c("me","you","NCOA$DNM")]

  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)
  report <- process(report)

  expect_equal(report$report[grepl("Bad primary mailing address", message),.N], 1)
})

test_that("process.unsubscribe_report identifies missing addresses", {
  fixture$addresses <- fixture$addresses[customer_no != 8992918]

  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)
  report <- process(report)

  expect_equal(report$report[grepl("No primary mailing address", message),.N], 1)
})

test_that("process.unsubscribe_report identifies non-matching logins", {
  setDT(fixture$logins)[,primary_ind:="Y"]

  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)
  report <- process(report)

  expect_equal(report$report[grepl("Email does not match login", message),.N], 2)
})

test_that("process.unsubscribe_report has one row per issue", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)
  report <- process(report)

  expect_equal(report$report[,.N],6) # three bad emails, 1 bad mailing address, 2 bad logins.
})

test_that("process.unsubscribe_report includes timestamp, membership, MGO, name, and constituency data", {
  stub(read.unsubscribe_report, "read_cache", fixture[[1]])
  stub(read.unsubscribe_report, "read_tessi", do.call(mock,fixture[-1]))

  report <- read(new_unsubscribe_report(), customers)
  report <- process(report)

  expect_names(colnames(report$report), permutation.of = c("message", "timestamp", "memb_level", "expr_dt", "customer_no", "MGOs",
                                                           "name", "constituencies"))
  expect_false(any(sapply(report$report,\(x) all(is.na(x)))))
})

