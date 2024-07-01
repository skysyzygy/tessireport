#' @title contributions_model
#' @description mlr3 model for predicting contributions
#' @export
#' @name contributions_model
contributions_model <- report(class=c("contributions_model","mlr_report"))


#' @export
read.contributions_model <- function(model, n_partitions = 10) {
  stream_path <- file.path(tessilake::cache_path("","deep",".."),"stream","stream-20240619.gz")
  ffbase::unpack.ffdf(stream_path)

  stream_key <- stream[,c("group_customer_no","timestamp","event_type","contributionAmt")] %>% setDT
  stream_key[,I:=.I]
  setkey(stream_key,group_customer_no,timestamp)

  # add event
  stream_key[,event := event_type == "Contribution" & contributionAmt>=50]
  stream_key[,n_event := cumsum(event), by="group_customer_no"]
  # censor
  stream_key <- stream_key[n_event == 0 | event & n_event==1]
  stream_key[,n_event := NULL]
  # subset
  stream_key[,weight := 1/pmax(timestamp >= Sys.Date()-365 | event, .1)]
  stream_key <- stream_key[runif(.N) < 1/weight]


  # partition by group
  groups <- stream_key[,.(group_customer_no = unique(group_customer_no))]
  groups[,partition := rep(seq_len(n_partitions), length.out = .N)]
  stream_key <- merge(stream_key,groups, by="group_customer_no")

  for (rows in split(stream_key,by="partition")) {
    dataset <- stream[rows$I,]
    dataset <- cbind(dataset,rows[,.(I,event,partition,weight)])
    setDT(dataset)

    # normalize names for mlr3
    setnames(dataset, names(dataset), \(.) gsub("\\W","_",.))

    stream_rollback_event(dataset, columns = grep("^contribution",names(dataset),value = T))
    stream_normalize_timestamps(dataset)
    tessilake::write_cache(dataset, "contributions", "model", partition = "partition",
                overwrite = TRUE, sync = FALSE)

  }

  #ff::delete(stream)
  dataset <- tessilake::read_cache("contributions","model")

  model$task <- mlr3::Task$new(id = "contributions",
                               task_type = "surv",
                               backend = arrow_to_mlr3(dataset))

  # label columns + rows
  model$task$col_roles$target <- c("timestamp","event")
  model$task$col_roles$feature <- grep("^(email|contribution|address|ticket).+(amt|count|level|max|min)",
                                       names(dataset),value=T,ignore.case=T,perl=T)
  model$task$col_roles$stratum <- "group_customer_no"
  model$task$col_roles$weight <- "weight"
  model$task$col_roles$order <- "timestamp"

  model$task$row_roles$test <- stream_key[timestamp >= Sys.Date()-365, I]

  NextMethod()
}

#' arrow_to_mlr3
#'
#' Converts arrow Table/Dataset to mlr3 Backend
#'
#' @param dataset [arrow::Table] or [arrow::Dataset]
#' @param primary_key character name of the column to use as a primary key
#'
#' @return [mlr3db::DataBackendDplyr]
#' @importFrom duckdb duckdb duckdb_register_arrow
#' @importFrom dplyr tbl
#' @importFrom mlr3db DataBackendDplyr
#' @importFrom checkmate assert_multi_class
#' @importFrom DBI dbConnect
arrow_to_mlr3 <- function(dataset, primary_key = "I") {
  assert_multi_class(dataset,c("Table","Dataset","arrow_dplyr_query"))
  assert_names(names(dataset), must.include = primary_key)

  db <- dbConnect(duckdb())
  duckdb_register_arrow(db, "dataset", dataset)
  table <- tbl(db, "dataset")
  backend <- DataBackendDplyr$new(table, primary_key, strings_as_factors = FALSE)
}

#' stream_rollback_event
#'
#' Rolls back the data in `columns` for rows flagged by `event` to prevent data leaks during training.
#'
#' @param dataset data.table of data to roll back
#' @param columns character vector of columns to roll back
#' @param event character column name containing a logical feature that indicates events to rollback
#' @param by character column name to group the table by
#'
#' @return rolled back data.table
#' @importFrom checkmate assert_data_table assert_names assert_logical
#' @importFrom dplyr lead lag
stream_rollback_event <- function(dataset, event = "event", columns = NULL, by = "group_customer_no") {

  assert_data_table(dataset)
  assert_names(names(dataset),must.include = c(event,columns,by))
  assert_logical(dataset[,event,with=F][[1]])

  dataset[,i := seq_len(.N)]
  dataset[,by_i := seq_len(.N), by = by]
  if (event %in% names(dataset)) {
    event_ <- event
    rm(event)
  }

  setkeyv(dataset,by)

  rollback <- dataset[lead(get(event_)) == T, c(columns,by,"by_i"),with=F] %>% .[,by_i := by_i+1] %>%
    rbind(dataset[get(event_) == T & by_i == 1, c(by, "by_i"), with = F], fill = T)
  dataset[rollback, (columns) := mget(paste0("i.",columns)), on = c(by, "by_i")]

  setkey(dataset,i)

  dataset[,`:=`(by_i = NULL, i = NULL)]

}

#' stream_normalize_timestamps
#'
#' Replaces the date-times in `columns` with integer offsets from the first `timestamp` per group identified by `by`.
#'
#' @param dataset data.table of data to normalize
#' @param columns character vector of columns to normalize; defaults to all columns with a name containing the word `timestamp`
#' @param by character column name to group the table by
#' @importFrom checkmate assert_data_table assert_names
#' @return normalized data.table
stream_normalize_timestamps <- function(dataset,
                                        columns = grep("timestamp", colnames(dataset), value=T, ignore.case = T),
                                        by = "group_customer_no") {
  assert_data_table(dataset)
  assert_names(names(dataset), must.include = c("timestamp",columns,by))
  assert_data_table(dataset[,c("timestamp",columns), with = F],types=c("Date","POSIXct"))

  dataset[,(columns) := lapply(.SD, \(c) c-min(timestamp, na.rm = T)), by = by, .SDcols = columns]
  dataset[,(columns) := lapply(.SD, \(c) as.numeric(c)), .SDcols = columns]
}
