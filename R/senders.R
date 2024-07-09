#' send_email
#'
#' @param emails email addresses (first will be sender)
#' @inheritParams mailR::send.mail
#' @param body Body of the email as text. If the parameter body refers to an existing file location,
#' the text of the file is parsed as body of the email. Default is `Sent by <computer name>`.
#' @inheritDotParams mailR::send.mail html attach.files
#' @importFrom checkmate assert_character test_character test_list
#' @importFrom mailR send.mail
#' @export
send_email <- function(subject, body = paste("Sent by", Sys.info()["nodename"]),
                       emails = config::get("tessiflow.email"),
                       smtp = config::get("tessiflow.smtp"),...
) {
  assert_character(subject, len = 1)
  assert_character(body, len = 1)

  if (!test_character(emails, min.len = 1)) {
    stop("Set tessiflow.email to the sender (first email) and list of recipients for messages")
  }
  if (!test_list(smtp)) {
    stop("Set tessiflow.smtp to the smtp server used to send messages")
  }

  send.mail(
    from = emails[[1]],
    to = emails,
    subject = subject,
    body = body,
    smtp = smtp,
    encoding = "utf-8",
    ...,
    send = TRUE
  )
}

#' send_xlsx
#'
#' Simple wrapper for [send_email] and [write_xlsx]
#'
#' @inheritParams send_email
#' @inheritParams write_xlsx
#' @param table data.table to send
#' @param basename name of the file to use in the email. Defaults to the name of the table.
#' A timestamp and extension will be appended and passed on to `mailR::send.mail` as `file.names`
#' @inheritDotParams mailR::send.mail html inline
#' @inheritDotParams write_xlsx group currency
#' @importFrom checkmate assert_data_table assert_character
#' @export
send_xlsx <- function(table,
                      subject = paste(format(substitute(table)), Sys.Date()),
                      body = paste("Sent by",Sys.info()["nodename"]),
                      emails = config::get("tessiflow.email"),
                      basename = format(substitute(table)), ...) {
  assert_data_table(table)
  assert_character(emails, min.len = 1)

  filename <- write_xlsx(table, ...)

  send_email_args <- modifyList(list(subject = subject, body = body, emails = emails,
                          html = TRUE, attach.files = filename,
                          file.names = paste0(basename,"_",Sys.Date(),".xlsx")),
                     rlang::list2(...))

  do.call(send_email,send_email_args)

}
