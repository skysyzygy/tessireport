#' auditR
#'
#' @description Base S3 class for auditR.
#'
#' @param x an environment
#' @param class an optional additional subclass
#'
#' @rdname auditR
#' @export
#'
new_auditR = function(x = new.env(),class=character()) {
  stopifnot(is.environment(x))
  structure(x,class=c(class,"auditR",class(x)))
}

#' @export
is.auditR = function(x) inherits(x,"auditR")


#' @export
read = function(x,...) UseMethod("read")

#' @export
process = function(x,...) UseMethod("process")


#' @export
print.auditR = function(x,...) {
  print(paste(class(x)[1],"with contents:"))
  purrr::map2(names(x),as.list(x),~{print(.x);print(.y)})
}

#' @export
output = function(x,...) UseMethod("output")

#' @export
write.auditR = function(x,...) NextMethod()

