#' @title mlr_report
#' @description Base class for training/running a machine learning model
#' @name mlr_report
#' @param mlr_report The report object
#' @export
mlr_report <- report(class="mlr_report")

#' @export
#' @describeIn mlr_report Should load the dataset into mlr_report$task
#' and is in charge of labeling column roles (i.e. feature, target, group, weight)
read.mlr_report <- function(mlr_report) {
  NextMethod()
}


#' @param train boolean whether to run the training
#' @param predict boolean whether to run the predictions
#' @export
#' @describeIn mlr_report Do the training and/or run the model. Subclasses
#' should define a `train.mlr_model` and `predict.mlr_model` function, which should load from
#' `task` and store their reults in entries of the same name
process.mlr_report <- function(mlr_report, train = TRUE, predict = TRUE) {
  if (train) train(mlr_report)
  if (predict) predict(mlr_report)
  NextMethod()
}

#' @export
#' @describeIn mlr_report Analyze the input set and the sensitivity of the training using
#' some standard heuristics. TBD
output.mlr_report <- function() {
  # TBD
  NextMethod()
}


#' @param subdir character name of subdir to store data in
#' @param sync boolean whether to sync data across storages
#' @describeIn mlr_report Save the trained model and model output for future use. Writes the model to the
#' [tessilake::cache_primary_path] under the subdirectory `subdir` and then syncs them across storages.
#' @export
#' @importFrom tessilake write_cache sync_cache
write.mlr_report <- function(mlr_report, subdir = "models", sync = TRUE) {

  report_name <- class(mlr_report)[1]
  path_name = tessilake::cache_primary_path(report_name, subdir)

  if (!is.null(mlr_report$train)) {
    train_filename = paste0(report_name,".Rds")
    saveRDS(mlr_report$train, train_filename)
    if (sync) sync_cache(report_name, subdir, whole_file = TRUE)
  }

  if (!is.null(mlr_report$predict)) {
    write_cache(mlr_report$predict, report_name, "models", sync = sync)
  }

  NextMethod()
}
