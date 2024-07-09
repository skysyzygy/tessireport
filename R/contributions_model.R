#' @title contributions_model
#' @description mlr3 model for predicting a customer's first contribution
#' @export
#' @name contributions_model
contributions_model <- report(class=c("contributions_model","mlr_report"))

#' @describeIn contributions_model Build the contributions dataset from the overall stream.
#' * events are the first contribution per household > $50
#' * data after an event are censored
#' * contribution indicators are rolled back and timestamps are normalized to the start of customer activity
#' * only data since `since` are loaded
#' Data is written to the primary cache partitioned by year and then synced across storages
#' @param since Date/POSIXct data on or after this date will be loaded and possibly used for training
#' @param ... not used
contributions_dataset <- function(since = Sys.Date()-365*5, until = Sys.Date(), ...) {
  stream <- group_customer_no <- timestamp <- event_type <- event <- contributionAdjAmt <- n_event <-
    N <- partition <- NULL

  stream_path <- file.path(tessilake::cache_path("","deep",".."),"stream","stream.gz")
  ffbase::unpack.ffdf(stream_path)

  stream_key <- stream[,c("group_customer_no","timestamp","event_type","contributionAdjAmt")] %>% setDT
  stream_key[,I:=.I]
  setkey(stream_key,group_customer_no,timestamp)

  # add event
  stream_key[,event := event_type == "Contribution" & contributionAdjAmt>=50]
  stream_key[,`:=`(n_event = cumsum(event),
                   N = .N), by="group_customer_no"]
  # censor
  stream_key <- stream_key[n_event == 0 | event & n_event==1 & N>1]
  stream_key[,`:=`(n_event = NULL, N = NULL)]
  # subsample
  month_subset <- stream_key[,last(I), by=list(group_customer_no,lubridate::floor_date(timestamp,"months"))]$V1
  stream_key <- stream_key[event | I %in% month_subset]
  stream_key <- stream_key[timestamp >= since & timestamp < until]

  # partition by year
  stream_key[,partition := year(timestamp)]

  stream_chunk_write <- \(rows, partition) {
    dataset <- stream[rows$I,]
    setDT(dataset)

    dataset <- cbind(dataset,rows[,setdiff(colnames(rows),
                                           colnames(dataset)), with = F])

    # normalize names for mlr3
    setnames(dataset, names(dataset), \(.) gsub("\\W","_",.))

    stream_rollback_event(dataset, columns = grep("^contribution",names(dataset),value = T))

    dataset[,date := timestamp]
    stream_normalize_timestamps(dataset)

    dataset[,partition := partition]

    tessilake::write_cache(dataset, "contributions_dataset", "model", partition = "partition",
                           incremental = TRUE, date_column = "date", sync = FALSE)

  }

  stream_key[, stream_chunk_write(.SD,partition), by = "partition"]

  ff::delete(stream)
}

#' @export
#' @importFrom tessilake read_cache cache_exists_any
#' @importFrom dplyr filter select collect mutate summarise
#' @importFrom mlr3 TaskClassif
#' @describeIn contributions_model Read in contribution data and prepare a mlr3 training task and a prediction/validation task
#' @param model `contributions_model` object
#' @param predict_since Date/POSIXct data on/after this date will be used to make predictions and not for training
#' @param until Date/POSIXct data after this date will not be used for training or predictions, defaults to the beginning of today
#' @param rebuild_dataset boolean rebuild the dataset by calling `contributions_dataset(since=since,until=until)` (TRUE), just read the existing one (FALSE),
#' or append new rows by calling `contributions_dataset(since=max_existing_date,until=until)` (NULL, default)
#' @note Data will be loaded in-memory, because *\[inaudible\]* mlr3 doesn't work well with factors encoded as dictionaries in arrow tables.
read.contributions_model <- function(model, rebuild_dataset = NULL,
                                     since = Sys.Date()-365*5,
                                     until = Sys.Date(),
                                     predict_since = Sys.Date() - 30, ...) {

  . <- event <- TRUE

  if(rebuild_dataset %||% F || !cache_exists_any("contributions_dataset","model")) {
    contributions_dataset(since = since, until = Sys.Date())
  }

  dataset <- read_cache("contributions_dataset","model")
  dataset_max_date <- summarise(dataset,max(date,na.rm = T)) %>% collect %>% .[[1]]

  if (rebuild_dataset %||% T && dataset_max_date < until && until <= Sys.Date()) {
    contributions_dataset(since = dataset_max_date, until = Sys.Date())
  }

  dataset <- dataset %>%
    filter(date >= since & date < until) %>%
    collect %>%
    mutate(date = as.POSIXct(date),
           event = as.factor(event))

  model$task <- TaskClassif$new(id = "contributions",
                                target = "event",
                                backend = dataset)

  # label columns + rows
  model$task$col_roles$feature <- grep("^(email|contribution|address|ticket).+(amt|count|level|max|min)|timestamp",
                                       names(dataset),value=T,ignore.case=T,perl=T)

  model$task$col_roles$order <- "timestamp"
  model$task$col_roles$group <- "group_customer_no"
  model$task$positive <- "TRUE"

  model$task$divide(ids = dataset[date >= predict_since,which=T])

  NextMethod()
}

#' @export
#' @importFrom tessilake cache_primary_path
#' @importFrom mlr3verse po to_tune flt lts ppl tune `%>>%` p_int tnr selector_invert selector_grep
#' @importFrom mlr3 msr rsmp as_learner lrn
#' @importFrom bbotk mlr_optimizers
#' @importFrom bestNormalize yeojohnson
#' @importFrom ranger ranger
#' @describeIn contributions_model Tune and train a stacked log-reg/ranger model on the data
#' @details
#' # Preprocessing:
#' * ignore 1-day and email "send" features because they leak data
#' * remove constant features
#' * balance classes to a ratio of 1:10 T:F
#' * Yeo-Johnson with tuned boundaries
#' * impute missing values out-of-range and add missing data indicator features
#' * feature importance filter (for log-reg submodel only)
#' # Model:
#' * stacked log-reg + ranger > log-reg model
#' * tuned using a hyperband method on the AUC (sensitivity/specificity)
train.contributions_model <- function(model, ...) {
  subsample <- po("subsample", frac = .1)

  preprocess <- po("select",selector = selector_invert(selector_grep("__1|Send"))) %>>%
                po("yeojohnson", lower = to_tune(-2,0), upper = to_tune(0,2), eps = .1) %>>%
                po("classbalancing", reference = "minor",ratio = 10,adjust="downsample") %>>%
                ppl("robustify")

  importance_filter <- po("filter",
                   filter = flt("importance"),
                   filter.frac = to_tune(0,.67))

  logreg <- as_learner(importance_filter %>>% lrn("classif.log_reg", predict_type = "prob"), id = "logreg")
  ranger <- as_learner(lrn("classif.ranger", predict_type = "prob",
                           mtry.ratio = to_tune(0,1),
                           sample.fraction = to_tune(.1,1),
                           num.trees = to_tune(p_int(16,128,tags="budget"))), id = "ranger")

  stacked <- as_learner(subsample %>>% preprocess %>>% ppl("stacking", c(logreg,ranger),
                                                           lrn("classif.log_reg", predict_type = "prob"),
                                                           use_features = FALSE), id = "stacked")
  stacked_tuned <- tune(
      tuner = tnr("hyperband",eta=2),
      task = model$task,
      learner = stacked,
      resampling = rsmp("holdout"),
      measure = msr("classif.auc"))

  stacked$param_set$values <- stacked_tuned$result_learner_param_vals

  model$model <- stacked$train(model$task)

  # save state
  write(model, sync = FALSE)

  NextMethod()
}

#' @export
#' @importFrom tessilake cache_primary_path read_cache
#' @importFrom dplyr filter
#' @describeIn contributions_model Predict using the trained model
predict.contributions_model <- function(model, ...) {
  if(is.null(model$model))
    model$model <- readRDS(cache_primary_path("contributions.model","model"))

  model$predictions <-
    cbind(as.data.table(model$model$predict(model$task$internal_valid_task)),
          model$task$internal_valid_task$data(cols = c("I","group_customer_no","date")))

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

  i <- by_i <- . <- NULL

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
  timestamp <- NULL

  assert_data_table(dataset)
  assert_names(names(dataset), must.include = c("timestamp",columns,by))
  assert_data_table(dataset[,c("timestamp",columns), with = F],types=c("Date","POSIXct"))

  dataset[,(columns) := lapply(.SD, \(c) c-min(timestamp, na.rm = T)), by = by, .SDcols = columns]
  dataset[,(columns) := lapply(.SD, \(c) as.numeric(c)), .SDcols = columns]
}
