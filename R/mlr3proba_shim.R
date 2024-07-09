#' .__TaskSurv__initialize
#'
#' Shim for .__TaskSurv__initialize to work with duckdb tables
#'
#' @importFrom dplyr slice_head select collect
#' @param id (`character(1)`)\cr
#'   Identifier for the new instance.
#' @param backend ([DataBackend])\cr
#'   Either a [DataBackend], or any object which is convertible to a [DataBackend] with `as_data_backend()`.
#'   E.g., a `data.frame()` will be converted to a [DataBackendDataTable].#' @template param_time
#' @param event (`character(1)`)\cr
#' Name of the column giving the event indicator.
#' If data is right censored then "0"/`FALSE` means alive (no event), "1"/`TRUE` means dead
#' (event). If `type` is `"interval"` then "0" means right censored, "1" means dead (event),
#' "2" means left censored, and "3" means interval censored. If `type` is `"interval2"` then
#' `event` is ignored.
#' @param time (`character(1)`)\cr
#' Name of the column for event time if data is right censored, otherwise starting time if
#' interval censored.
#' @param time2 (`character(1)`)\cr
#' Name of the column for ending time of the interval for interval censored or
#' counting process data, otherwise ignored.
#' @param type (`character(1)`)\cr
#' Name of the column giving the type of censoring. Default is 'right' censoring.
#' @param label (`character(1)`)\cr
#'    Label for the new instance.
.__TaskSurv__initialize <- function(id, backend, time = "time", event = "event", time2,
                                    type = c("right", "left", "interval", "counting", "interval2", "mstate"),
                                    label = NA_character_)
{
  type = match.arg(type)
  backend = as_data_backend(backend)
  if (type != "interval2") {
    c_ev = backend$data(1,"event")[[1]]
    if (type == "mstate") {
      assert_factor(c_ev)
    }
    else if (type == "interval") {
      assert_integerish(c_ev, lower = 0, upper = 3)
    }
    else if (!is.logical(c_ev)) {
      assert_integerish(c_ev, lower = 0, upper = 2)
    }
  }
  private$.censtype = type
  if (type %in% c("right", "left", "mstate")) {
    super$initialize(id = id, task_type = "surv", backend = backend,
                     target = c(time, event), label = label)
  }
  else if (type %in% c("interval", "counting")) {
    super$initialize(id = id, task_type = "surv", backend = backend,
                     target = c(time, time2, event), label = label)
  }
  else {
    super$initialize(id = id, task_type = "surv", backend = backend,
                     target = c(time, time2), label = label)
  }
}
