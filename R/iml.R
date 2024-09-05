#' @name iml
#' @title Interpretable machine learning for mlr_report
#' @description
#' Simple wrappers around [iml-package] classes to provide a more streamlined approach for generating
#' interpretable plots and explanatory data.
#' @return
#' * `iml_predictor()`: [iml::Predictor]
#' * `iml_featureimp()`: [iml::FeatureImp]
#' * `iml_featureeffects()`: [iml::FeatureEffects]
#' * `iml_shapley()`: [iml::Shapley]
#' @importFrom iml FeatureImp FeatureEffects Shapley
NULL

#' @importFrom iml Predictor
#' @importFrom purrr reduce
#'
#' @describeIn iml wrapper for [iml::Predictor] to subset the features of data and
#' provide a `predict.function` and `y` when the predictor can't identify
#' them.
#'
#' @param model [mlr3::Learner] model, pre-trained
#' @param data [data.table] of a test dataset
#' @param predict.function `function` function to predict newdata. The first argument is the model, the second the newdata.
#' @param y `character(1)`|[numeric]|[factor] The target vector or (preferably) the name of the target column in the data argument. Predictor tries to infer the target automatically from the model.
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

#' @describeIn iml wrapper for [iml::FeatureImp] that handles predictor creation and multiprocessing
#' @param loss `character(1)`|[function]. The loss function. Either the name of a loss (e.g. "ce" for classification or "mse") or a function.
#' @param compare `character(1)` Either "ratio" or "difference".
#' @param n.repetitions `numeric(1)` How many shufflings of the features should be done?
iml_featureimp <- function(model, data, loss = "logLoss", compare = "difference",
                           n.repetitions = 5, features = NULL) {
  future::plan("multisession")
  predictor <- iml_predictor(model, data)
  fi <- FeatureImp$new(predictor, loss = loss, compare = compare,
                       n.repetitions = n.repetitions, features = features)
}

#' @describeIn iml wrapper for [iml::FeatureEffects] that handles data filtering, predictor creation and multiprocessing
#' @param features `character` The names of the features for which to compute the feature effects/importance.
#' @param method `character(1)`
#' * 'ale' for accumulated local effects,
#' * 'pdp' for partial dependence plot,
#' * 'ice' for individual conditional expectation curves,
#' * 'pdp+ice' for partial dependence plot and ice curves within the same plot.
#' @param center.at `numeric(1)` Value at which the plot should be centered. Ignored in the case of two features.
#' @param grid.size `numeric(1)` The size of the grid for evaluating the predictions.
iml_featureeffects <- function(model, data, features = NULL, method = "ale",
                               center.at = NULL, grid.size = 20) {
  future::plan("sequential")
  # filter out rows with missing data
  data <- data[ data[,apply(.SD,1,\(.) !any(is.na(.))), .SDcols = features] ]
  predictor <- iml_predictor(model, data)
  fe <- FeatureEffects$new(predictor, features = features, method = method,
                           center.at = center.at, grid.size = grid.size)
}

#' @describeIn iml wrapper for [iml::Shapley] that handles predictor creation and multiprocessing
#' @param x.interest [data.frame] Single row with the instance to be explained.
#' @param sample.size `numeric(1)` The number of Monte Carlo samples for estimating the Shapley value.
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
                                    by = c("group_customer_no","date")) %>% collect %>% setDT %>%
    .[prob.TRUE>.75]

  withr::local_options(future.globals.maxSize = 16*1024^3)

  fi <- iml_featureimp(model$model, dataset_predictions)
  top_features <- fi$results[1:25,"feature"]

  fe <- iml_featureeffects(model$model, dataset_predictions, top_features)

  write_pdf({
    pdf_plot(fi, "Feature importance","first contributions model")
    pdf_plot(fe, "Feature effects","first contributions model")
  }, .title = "Contributions model", output_file = cache_primary_path("contributions_model.pdf","contributions_model"))

  ex <- imp_shapley(model$model, dataset_predictions[prob.TRUE>.75], sample.size = 10)
  saveRDS(ex, cache_primary_path("shapley.Rds", "contributions_model"))

}
