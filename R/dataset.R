
dataset_chunk_write <- function(dataset, partition,
                                dataset_name,
                                rows = data.table(I=seq_len(nrow(dataset))),
                                cols = colnames(dataset),
                                rollback = NULL) {

  assert_names(colnames(dataset), must.include = c("timestamp",cols,rollback))
  assert_data_table(rows)
  assert_character(cols)
  assert_character(rollback)

  dataset <- dataset[rows$I, ] %>% setDT
  dataset <- cbind(dataset,rows[,setdiff(colnames(rows),
                                         colnames(dataset)), with = F])

  # normalize names for mlr3
  setnames(dataset, names(dataset), \(.) gsub("\\W","_",.))

  if (!is.null(rollback))
    dataset_rollback_event(dataset, columns = rollback)

  dataset[,date := timestamp]
  dataset_normalize_timestamps(dataset)

  dataset[,partition := partition]

  tessilake::write_cache(dataset, "dataset", dataset_name, partition = "partition",
                         incremental = TRUE, date_column = "date", sync = FALSE)

}


#' dataset_rollback_event
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
dataset_rollback_event <- function(dataset, event = "event", columns = NULL, by = "group_customer_no") {

  i <- by_i <- . <- NULL

  assert_data_table(dataset)
  assert_names(names(dataset),must.include = c(event,columns,by))
  assert_logical(dataset[,event,with=F][[1]])

  dataset[,i := seq_len(.N)]
  dataset[,by_i := seq_len(.N), by = by]
  event_ <- event
  rm(event)

  setkeyv(dataset,by)

  rollback <- dataset[lead(get(event_)) == T, c(columns,by,"by_i"),with=F] %>% .[,by_i := by_i+1] %>%
    rbind(dataset[get(event_) == T & by_i == 1, c(by, "by_i"), with = F], fill = T)
  dataset[rollback, (columns) := mget(paste0("i.",columns)), on = c(by, "by_i")]

  setkey(dataset,i)

  dataset[,`:=`(by_i = NULL, i = NULL)]

}

#' dataset_normalize_timestamps
#'
#' Replaces the date-times in `columns` with integer offsets from the first `timestamp` per group identified by `by`.
#'
#' @param dataset data.table of data to normalize
#' @param columns character vector of columns to normalize; defaults to all columns with a name containing the word `timestamp`
#' @param by character column name to group the table by
#' @importFrom checkmate assert_data_table assert_names
#' @return normalized data.table
dataset_normalize_timestamps <- function(dataset,
                                        columns = grep("timestamp", colnames(dataset), value=T, ignore.case = T),
                                        by = "group_customer_no") {
  timestamp <- NULL

  assert_data_table(dataset)
  assert_names(names(dataset), must.include = c("timestamp",columns,by))
  assert_data_table(dataset[,c("timestamp",columns), with = F],types=c("Date","POSIXct"))

  dataset[,min_timestamp := min(timestamp, na.rm = T), by = by]
  dataset[,(columns) := lapply(.SD, \(c) as.numeric(as.POSIXct(c)-as.POSIXct(min_timestamp))), .SDcols = columns]
  dataset[,min_timestamp := NULL]
  dataset
}
