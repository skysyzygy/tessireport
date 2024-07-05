#' .__TaskSurv__initialize
#'
#' Shim for .__TaskSurv__initialize to work with duckdb tables
#'
#' @importFrom dplyr slice_head select collect
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
