#' @title mlr_report
#' @description Base class for training/running a machine learning model
#' @name mlr_report
#' @param mlr_report The report object
#' @export
mlr_report <- report(class="mlr_report")

#' @export
#' @describeIn mlr_report Should load the dataset into `mlr_report$task`
#' and is in charge of labeling column roles (i.e. feature, target, group, weight)
read.mlr_report <- function(mlr_report, ...) {
  NextMethod()
}


#' @param train boolean whether to run the training
#' @param predict boolean whether to run the predictions
#' @export
#' @describeIn mlr_report Do the training and/or run the model. Subclasses
#' should define a `train.mlr_model` and `predict.mlr_model` function, which should load from
#' `mlr_report$task` and store their results in `mlr_report$model` and `mlr_report$predictions`
process.mlr_report <- function(mlr_report, train = TRUE, predict = TRUE, ...) {
  if (train) mlr_report <- train(mlr_report, ...)
  if (predict) mlr_report <- predict(mlr_report, ...)
  NextMethod()
}

#' @export
train <- function(...) UseMethod("train")
#' @export
train.mlr_report <- function(x, ...) x

#' @export
predict <- function(...) UseMethod("predict")
#' @export
predict.mlr_report <- function(x, ...) x


#' @export
#' @describeIn mlr_report Analyze the input set and the sensitivity of the training using
#' some standard heuristics. TBD
output.mlr_report <- function(...) {
  # TBD
  NextMethod()
}


#' @param subdir character name of subdirectory to store data in, defaults to "model"
#' @param sync boolean whether to sync data across storages
#' @describeIn mlr_report Save the trained model and model output for future use. Writes the model to the
#' [tessilake::cache_primary_path] under the subdirectory `subdir` and then syncs them across storages.
#' @export
#' @importFrom tessilake write_cache sync_cache
write.mlr_report <- function(mlr_report, subdir = "model", sync = TRUE, ...) {

  report_name <- class(mlr_report)[1]
  path_name = tessilake::cache_primary_path(report_name, subdir)

  if (!is.null(mlr_report$model)) {
    model_filename = gsub("_",".",report_name)
    saveRDS(mlr_report$model, file.path(path_name,model_filename))
    if (sync) sync_cache(report_name, subdir, whole_file = TRUE)
  }

  if (!is.null(mlr_report$predictions)) {
    write_cache(mlr_report$predictions, report_name, subdir, sync = sync)
  }

  NextMethod()
}
