withr::local_package("checkmate")
withr::local_package("mockery")
withr::local_package("dplyr")

# run.p2_orphans_report -------------------------------------------------------

png <- tempfile(fileext=".png")
html <- tempfile()
tempfile <- mock(png,html,cycle=T)

p2_resolve_orphan <- mock(T,F,cycle=T)
send_xlsx <- mock()

stub(p2_orphans, "tessi_customer_no_map", data.table(customer_no = seq(100) + 1000,
                                                     group_customer_no = seq(100) %% 10))

stub(run.p2_orphans_report, "p2_orphans", p2_orphans <- data.table(address=c(letters,seq(26)),
                                                               customer_no=seq(52),
                                                               id=seq(52)))
stub(run.p2_orphans_report, "tessi_changed_emails", tessi_changed_emails <- data.table(from=letters,
                                                                                   to=LETTERS,
                                                                                   last_updated_by=rep(c("popmulti","sqladmin","me"),length.out=26),
                                                                                   timestamp = lubridate::ymd_hms("2023-01-01 00:00:00") + ddays(seq(26)),
                                                                                   customer_no=seq(26),
                                                                                   group_customer_no=100+seq(26)))
stub(run.p2_orphans_report, "read_tessi", data.table(customer_no=1,
                                                 group_customer_no=c(101,152),
                                                 memb_level="L01",
                                                 expr_dt=lubridate::ymd_hms("2023-01-01 00:00:00")))
stub(run.p2_orphans_report, "tempfile", tempfile)
stub(run.p2_orphans_report, "p2_resolve_orphan", p2_resolve_orphan)
stub(run.p2_orphans_report, "send_xlsx", send_xlsx)
stub(run.p2_orphans_report, "tessi_customer_no_map", data.table(customer_no=seq(100),
                                                            group_customer_no=seq(100)+100))
run.p2_orphans_report()

test_that("run.p2_orphans_report creates a chart of orphans analysis",{

  expect_length(png::readPNG(png),480*480*3)
  # test ink converage > 50%
  expect_gte(length(which(png::readPNG(png)<1)),480*480*3/2)

})

test_that("run.p2_orphans_report sends an email with the spreadsheet and an inline image",{

  expect_match(readLines(html),gsub("\\","\\\\",png,fixed=T))
  expect_class(mock_args(send_xlsx)[[1]][["table"]],"data.table")

})

test_that("run.p2_orphans_report creates a spreadsheet with one row per orphan and all relevant info",{

  report <- mock_args(send_xlsx)[[1]][["table"]]

  expect_equal(nrow(report),nrow(p2_orphans))
  expect_equal(report[1:26,timestamp], as.Date(tessi_changed_emails$timestamp))
  expect_equal(report[,`customer_#`], p2_orphans$customer_no)
  expect_equal(report[,p2_id], p2_orphans$id)
  expect_equal(report[,from_email], p2_orphans$address)
  expect_equal(report[1:26,to_email], tessi_changed_emails$to)
  expect_equal(report[,member_level], c("L01",rep(NA,nrow(p2_orphans)-2),"L01"))
  expect_equal(report[,expiration_date], c(lubridate::ymd("2023-01-01"),rep(NA,nrow(p2_orphans)-2),lubridate::ymd("2023-01-01")))
  expect_equal(report[1:26,change_type], rep(c("web","merge","client"),length.out = 26))
  expect_equal(report[1:26,last_updated_by], tessi_changed_emails$last_updated_by)

})


test_that("run.p2_orphans_report attempts to resolve each row and saves the result",{

  expect_equal(purrr::map_chr(mock_args(p2_resolve_orphan),1) %>% sort,p2_orphans$address %>% sort)
  expect_equal(purrr::map_chr(mock_args(p2_resolve_orphan),2) %>% sort,tessi_changed_emails$to %>% sort)
  expect_equal(purrr::map_lgl(mock_args(p2_resolve_orphan),"dry_run"),rep(F,nrow(p2_orphans)))

  report <- mock_args(send_xlsx)[[1]][["table"]]
  expect_equal(report[,has_been_updated], as.list(rep(c(T,F),26)))

})

test_that("run.p2_orphans_report runs gracefully when there are no orphans",{
  stub(run.p2_orphans_report, "p2_orphans", data.table())

  run.p2_orphans_report()
  expect_length(mock_args(send_xlsx),2)
  expect_equal(nrow(mock_args(send_xlsx)[[2]][["table"]]),0)

  stub(run.p2_orphans_report, "p2_orphans", p2_orphans)
  stub(run.p2_orphans_report, "tessi_changed_emails", data.table())

  run.p2_orphans_report()
  expect_length(mock_args(send_xlsx),3)
  expect_equal(nrow(mock_args(send_xlsx)[[3]][["table"]]),0)

})
