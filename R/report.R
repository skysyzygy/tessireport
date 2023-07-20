#' report
#'
#' @description Base S3 class for report.
#'
#' @param x a list
#' @param class an optional additional subclass
#'
#' @rdname report
#' @name report
#' @export
#'
new_report = function(x = list(),class=character()) {
  stopifnot(is.list(x))
  structure(x,class=c(class,"report",class(x)))
}

#' @export
#' @describeIn report tests if x is an report object
is_report = function(x) inherits(x,"report")

#' @export
#' @describeIn report generic function to dispatch for data reading
read = function(x,...) UseMethod("read")

#' @export
#' @describeIn report generic function to dispatch for data processing
process = function(x,...) UseMethod("process")

#' @export
#' @describeIn report prints the names and contents of the report object
print.report = function(x,...) {
  cli::cli_h1(paste(class(x)[1],"with contents:"))
  cli::cli_ul()
  purrr::imap(x,~{
    cli::cli_li(cli::col_cyan(.y))
    cli::cli_bullets(c(" " = paste0(names(.x),collapse=", ")))
    })
}

#' @export
#' @describeIn report generic fucntion to dispatch for data output
output = function(x,...) UseMethod("output")

#' @export
#' @describeIn report generic function to dispatch for data saving
write = function(x,...) UseMethod("write")

