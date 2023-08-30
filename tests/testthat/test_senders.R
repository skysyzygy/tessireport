withr::local_package("checkmate")
withr::local_package("mockery")


# send_email --------------------------------------------------------------

test_that("send_email complains if subject, body, emails, or smtp are not set correctly", {
  send.mail <- mock()
  stub(send_email, "send.mail", send.mail)

  expect_error(send_email(),"subject.+missing")
  expect_error(send_email("subject"),"body.+missing")

  stub(send_email, "config::get", mock(NULL,
                                       "test@test.com", NULL,
                                       "test@test.com", list(host.name = "blah")))

  expect_error(send_email("subject", "body"),"email.+sender.+recipients")
  expect_error(send_email("subject", "body"),"smtp server")
})

test_that("send_email passes on parameters to send.mail", {
  send.mail <- mock()
  stub(send_email, "send.mail", send.mail)
  stub(send_email, "config::get", mock("test@test.com", list(host.name = "blah")))

  expect_silent(send_email("subject","body"))
  expect_length(mock_args(send.mail),1)
  expect_equal(mock_args(send.mail)[[1]][["subject"]],"subject")
  expect_equal(mock_args(send.mail)[[1]][["body"]],"body")
  expect_equal(mock_args(send.mail)[[1]][["from"]],"test@test.com")
  expect_equal(mock_args(send.mail)[[1]][["to"]],"test@test.com")
  expect_equal(mock_args(send.mail)[[1]][["smtp"]],list(host.name = "blah"))
})

test_that("send_email sends an email", {
  stub(send.mail, ".jTryCatch", function(...) {
    rlang::warn(class = "sent!")
  })
  stub(send_email, "send.mail", send.mail)
  stub(send_email, "config::get", mock("test@test.com", list(host.name = "blah"), cycle = TRUE))
  expect_warning(send_email("subject", "body"), class = "sent!")
})



# send_xlsx ---------------------------------------------------------------

test_that("send_xlsx calls send_email with the specified args", {
  send_email <- mock()
  stub(send_xlsx, "send_email", send_email)

  a_table <- data.table(x = seq(1000))

  send_xlsx(a_table)
  expect_equal(mock_args(send_email)[[1]][["subject"]],paste("a_table",Sys.Date()))
  expect_equal(mock_args(send_email)[[1]][["body"]],paste("Sent by",Sys.info()["nodename"]))
  expect_equal(mock_args(send_email)[[1]][["emails"]],"ssyzygy@bam.org")
  expect_equal(mock_args(send_email)[[1]][["html"]],TRUE)
  expect_match(mock_args(send_email)[[1]][["attach.files"]],"\\.xlsx$")
  expect_match(mock_args(send_email)[[1]][["file.names"]],paste0("a_table_",Sys.Date(),".xlsx"))


  send_xlsx(a_table, subject = "subject", body = "body", emails = "me@me.com",
            file.names = "a_table.xlsx")
  expect_equal(mock_args(send_email)[[2]][["subject"]],"subject")
  expect_equal(mock_args(send_email)[[2]][["body"]],"body")
  expect_equal(mock_args(send_email)[[2]][["emails"]],"me@me.com")
  expect_equal(mock_args(send_email)[[2]][["html"]],TRUE)
  expect_match(mock_args(send_email)[[2]][["attach.files"]],"\\.xlsx$")
  expect_match(mock_args(send_email)[[2]][["file.names"]],paste0("a_table.xlsx"))

})
