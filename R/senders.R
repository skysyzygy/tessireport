#' send_email
#'
#' @param subject email subject
#' @param body email body
#' @param emails email addresses (first will be sender)
#' @param smtp smtp configuration
#' @param ... additional parameters sent on to [mailR::send.mail]
#' @importFrom checkmate assert_character test_character test_list
#' @importFrom mailR send.mail
#' @export
send_email <- function(subject, body,
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
#' @param table data.table to send
#' @param emails character email addresses to send the email to (first will be sender as well)
#' @param subject character subject of the email, default is the name of the `table` and the current date.
#' @param body character body of the email, default is a human readable message indicating the computer name.
#' If the parameter body refers to an existing file location, the text of the file is parsed as body of the email.
#' @param ... additional parameters to send on to [send_email] and then to [mailR::send.mail]
#' @note Useful additional parameters include:
#' * `html`: boolean indicating whether the body of the email should be parsed as HTML. Default is `TRUE`.
#' * `inline`: boolean indicating whether images in the HTML file should be embedded inline.
#' * `file.names`: character name of the filename to show in the email. The default is `<table_name>_<today's date>.xlsx`
#' @importFrom checkmate assert_data_table assert_character
#' @export
send_xlsx <- function(table,
                      subject = paste(format(substitute(table)), Sys.Date()),
                      body = paste("Sent by",Sys.info()["nodename"]),
                      emails = config::get("tessiflow.email"), ...) {
  assert_data_table(table)
  assert_character(emails, min.len = 1)

  filename <- write_xlsx(table)

  args <- rlang::list2(...)
  args <- modifyList(list(subject = subject, body = body, emails = emails,
                          html = TRUE, attach.files = filename,
                          file.names = paste0(format(substitute(table)),"_",Sys.Date(),".xlsx")),
                     args)

  do.call(send_email,args)

}
