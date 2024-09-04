#' @name iml
#' @title interpretable machine learning for mlr_report
#' @description
#' interpretable machine learning for mlr_report using [iml]
NULL

#' @return [iml::Predictor]
#' @importFrom iml Predictor
#' @importFrom purrr reduce
#'
#' @describeIn iml Wrapper around [iml::Predictor] to subset the features of data and
#' provide a `predict.function` and `y` when the predictor can't identify
#' them.
#'
#' @param model mlr3 model, pre-trained
#' @param data data.table of a test dataset
#' @param predict.function function the function to predict newdata. The first argument is the model, the second the newdata.
#' @param y character/numeric/factor The target vector or (preferably) the name of the target column in the data argument. Predictor tries to infer the target automatically from the model.
#'
iml_predictor <- function(model, data, predict.function = NULL, y = NULL) {

  # patch for prob models
  if (model$predict_type == "prob") {
    predict.function <- function(model,newdata) {
      model$predict_newdata(newdata)$prob[,"TRUE"]
    }
    y <- "prob.TRUE"
  } else {
    y <- "response"
  }

  # features used in the model
  features <- reduce(model$model,\(f,m) c(f,m$intasklayout$id)) %>% unlist %>% unique

  Predictor$new(model, data[,intersect(c(features,y),
                                       colnames(data)),
                            with=F],
                predict.function = predict.function,
                y = y)

}

iml_featureimp <- function(model, data, loss = "logLoss", compare = "difference",
                           n.repetitions = 5, features = NULL) {
  future::plan("multisession")
  predictor <- iml_predictor(model, data)
  fi <- FeatureImp$new(predictor, loss = loss, compare = compare,
                       n.repetitions = n.repetitions, features = features)
}

iml_featureeffects <- function(model, data, features = NULL, method = "ale",
                               center.at = NULL, grid.size = 20) {
  future::plan("sequential")
  # filter out rows with missing data
  data <- data[ data[,apply(.SD,1,\(.) !any(is.na(.))), .SDcols = features] ]
  predictor <- iml_predictor(model, data)
  fe <- FeatureEffects$new(predictor, features = features, method = method,
                           center.at = center.at, grid.size = grid.size)
}

iml_shapley <- function(model, data, x.interest = NULL, sample.size = 100) {
  future::plan("multisession")
  predictor <- iml_predictor(model$model, data)
  s <- Shapley$new(predictor, sample.size = sample.size, x.interest = x.interest)
  explanations <- data %>% split(seq_len(nrow(.))) %>%
    future_map(as.data.frame %>% s$explain())
}

#' @describeIn contributions_model create IML reports for contributions_model
#' @importFrom iml FeatureImp FeatureEffects Shapley
#' @importFrom dplyr inner_join
#' @export
output.contributions_model <- function(model) {

  model <- NextMethod()

  model$dataset <- mutate(model$dataset, date = as.Date(date))
  model$predictions <- mutate(model$predictions, date = as.Date(date))

  dataset_predictions <- inner_join(model$dataset,model$predictions,
                                    by = c("group_customer_no","date")) %>% collect %>% setDT

  fi <- iml_featureimp_prob(model$model, dataset_predictions)
  top_features <- fi$results[1:25,"feature"]

  fe <- iml_featureeffects(model$model, dataset_predictions, top_features)

  ex <- imp_shapley(model$model, dataset_predictions[prob.TRUE>.75], sample.size = 10)

}
