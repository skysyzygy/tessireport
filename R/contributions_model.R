#' @title contributions_model
#' @description mlr3 model for predicting contributions
#' @export
#' @name contributions_model
contributions_model <- report(class=c("contributions_model","mlr_report"))


#' @export
read.contributions_model <- function(model) {
  stream_path <- file.path(tessilake::cache_path("","deep",".."),"stream","stream-20240619.gz")
  ffbase::unpack.ffdf(stream_path)

  positive_rows <- which(ff::as.ram(stream$event_type) == "Contribution")
  negative_rows <- setdiff(seq(nrow(stream)),positive_rows) %>% sample(length(.)/10)
  recent_rows <- which(ff::as.ram(stream$timestamp) >= (Sys.Date() - 365))
  # --- censor

  all_rows <- data.table::data.table(I = unique(sort(c(positive_rows, negative_rows, recent_rows))))
  all_rows[,partition := sort(rep(1:10,length.out=.N))]

  for (rows in split(all_rows,by="partition")) {
    dataset <- stream[rows$I,]
    dataset <- cbind(dataset,rows)
    # -- add event signal + rollback
    tessilake::write_cache(dataset, "contributions", "model", partition = "partition",
                overwrite = TRUE, sync = FALSE)
    # ---

  }

  ff::delete(stream)
  dataset <- tessilake::read_cache("contributions","model")

  # --- refactor as function
  db <- DBI::dbConnect(duckdb::duckdb())
  duckdb::duckdb_register_arrow(db, "dataset", dataset)
  table <- dplyr::tbl(db, "dataset")
  backend <- mlr3db::DataBackendDplyr$new(table, "I", strings_as_factors = FALSE)
  # ---

  model$task <- mlr3proba::as_task_surv(backend, time = "timestamp", event = "event",
                          type = "right", id = "contributions")

  # --- label rows + columns

  NextMethod()
}

