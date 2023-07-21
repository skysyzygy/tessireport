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

default_function <- function(fun_name) {
  function(x, ...) {
    rlang::warn(paste0("No ",fun_name," function defined for object of class (",paste(class(x),collapse = ", "),"), doing nothing"))
  }
}

#' @export
#' @describeIn report tests if x is an report object
is_report = function(x) inherits(x,"report")

#' @export
#' @describeIn report generic function to dispatch for data reading
read = function(x,...) UseMethod("read")
#' @export
#' @describeIn report does thing but print a warning, intended to be overloaded by subclass
read.report = default_function("read")

#' @export
#' @describeIn report generic function to dispatch for data processing
process = function(x,...) UseMethod("process")
#' @export
#' @describeIn report does thing but print a warning, intended to be overloaded by subclass
process.report = default_function("process")

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
#' @describeIn report does thing but print a warning, intended to be overloaded by subclass
output.report = default_function("output")

#' @export
#' @describeIn report generic function to dispatch for data saving
write = function(x,...) UseMethod("write")
#' @export
#' @describeIn report does thing but print a warning, intended to be overloaded by subclass
write.report = default_function("write")

#' @export
#' @describeIn report generic function to dispatch for report running
run = function(x,...) UseMethod("run")
#' @export
#' @describeIn report run all methods in order: read -> process -> output -> write
run.report = function(x,...) {
  x %>%
    read(...) %>%
    process(...) %>%
    output(...) %>%
    write(...)
}
