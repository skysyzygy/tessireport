#' Base auditR class
#' @description Basic class for all of the auditR modules, with several functions
#' - auditR$new(data = NULL) that creates a new auditR object
#' - auditR$load() a stub that does nothing (for loading data)
#' - auditR$process() a stub that does nothing (for processing data)
#' - auditR$report() that prints the data (for reporting data)
#' @field data
#' @param data optional data used during initialization to more easily copy an object without having to
#' reload data
#'
#' @return all functions return the object invisibly
#' @export
#'
#' @examples
auditR = R6::R6Class("auditR",
  public = list(
    data = NULL,

    initialize = function(data = NULL) {
      self$data = data
      invisible(self)
    },
    load = function() {
      invisible(self)
    },
    process = function() {
      invisible(self)
    },
    report = function() {
      print(self$data)
      invisible(self)
    }
  )
)
