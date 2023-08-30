withr::local_package("checkmate")
withr::local_package("mockery")
withr::local_package("dplyr")

# run.p2_orphans_report -------------------------------------------------------

png <- tempfile(fileext=".png")
xlsx <- tempfile(fileext=".xlsx")
html <- tempfile()
tempfile <- mock(png,xlsx,html)

p2_resolve_orphan <- mock(T,F,cycle=T)
send_email <- mock()

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
stub(run.p2_orphans_report, "send_email", send_email)
stub(run.p2_orphans_report, "tessi_customer_no_map", data.table(customer_no=seq(100),
                                                            group_customer_no=seq(100)+100))
debugonce(run.p2_orphans_report)
run.p2_orphans_report()

test_that("run.p2_orphans_report creates a chart of orphans analysis",{

  expect_length(png::readPNG(png),480*480*3)
  # test ink converage > 50%
  expect_gte(length(which(png::readPNG(png)<1)),480*480*3/2)

})

test_that("run.p2_orphans_report creates a spreadsheet with one row per orphan and all relevant info",{

  report <- openxlsx::read.xlsx(xlsx, colNames = T, detectDates = TRUE) %>% setDT %>% setkeyv("P2.Id")

  expect_equal(nrow(report),nrow(p2_orphans))
  expect_equal(report[1:26,Timestamp], as.Date(tessi_changed_emails$timestamp))
  expect_equal(report[,`Customer.#`], p2_orphans$customer_no)
  expect_equal(report[,`P2.Id`], p2_orphans$id)
  expect_equal(report[,`From.Email`], p2_orphans$address)
  expect_equal(report[1:26,`To.Email`], tessi_changed_emails$to)
  expect_equal(report[,`Member.Level`], c("L01",rep(NA,nrow(p2_orphans)-2),"L01"))
  expect_equal(report[,`Expiration.Date`], c(lubridate::ymd("2023-01-01"),rep(NA,nrow(p2_orphans)-2),lubridate::ymd("2023-01-01")))
  expect_equal(report[1:26,`Change.Type`], rep(c("web","merge","client"),length.out = 26))
  expect_equal(report[1:26,`Last.Updated.by`], tessi_changed_emails$last_updated_by)

})

test_that("run.p2_orphans_report sends an email with the spreadsheet and an inline image",{

  expect_match(readLines(html),gsub("\\","\\\\",png,fixed=T))
  expect_equal(mock_args(send_email)[[1]][["attach.files"]],xlsx)

})

test_that("run.p2_orphans_report determines the updatability of each row",{

  expect_equal(purrr::map_chr(mock_args(p2_resolve_orphan),1) %>% sort,p2_orphans$address %>% sort)
  expect_equal(purrr::map_chr(mock_args(p2_resolve_orphan),2) %>% sort,tessi_changed_emails$to %>% sort)
  expect_equal(purrr::map_lgl(mock_args(p2_resolve_orphan),"dry_run"),rep(T,nrow(p2_orphans)))
  expect_equal(openxlsx::read.xlsx(xlsx)$can.be.Updated, rep(c(T,F),26))

})
