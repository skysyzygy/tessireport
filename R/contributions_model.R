#' @title contributions_model
#' @description mlr3 model for predicting contributions
#' @export
#' @name contributions_model
contributions_model <- report(class=c("contributions_model","mlr_report"))


#' @export
read.contributions_model <- function(contributions_model) {
  stream_path <- file.path(tessilake::cache_path("","deep",".."),"stream","addressStream.gz")
  ffbase::unpack.ffdf(stream_path)

  positive_rows <- which(ff::as.ram(stream$event_type) == "Contribution")
  negative_rows <- setdiff(seq(nrow(stream)),positive_rows) %>% sample(length(.)/10)
  recent_rows <- which(ff::as.ram(stream$timestamp) >= (Sys.Date() - 365))

  rows <- unique(sort(c(positive_rows, negative_rows, recent_rows)))

  dataset <- stream[rows,]

  write_cache(dataset,"acquisition_stream","stream")

  NextMethod()
}
