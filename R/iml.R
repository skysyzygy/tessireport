if (FALSE) {
  withr::local_envvar(R_CONFIG_FILE="")
  model <- load_model("contributions_model")
  dataset <- read_cache("dataset","contributions_model") %>% collect %>% setDT
  predictions <- read_cache("predictions","contributions_model") %>% collect %>% setDT

  dataset[, date := as.Date(date)]
  predictions[, date := as.Date(date)]

  dataset_predictions <- dataset[predictions,on=c("group_customer_no","date")]

  p <- iml::Predictor$new(model,dataset_predictions[sapply(dataset_predictions,\(c) {
    inherits(c,c("numeric","factor","integer","ordered","character")) })],
    y = "prob.TRUE")
}
