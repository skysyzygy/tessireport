if (FALSE) {
  withr::local_envvar(R_CONFIG_FILE="")
  model <- load_model("contributions_model")
  dataset <- read_cache("dataset","contributions_model") %>% collect %>% setDT
  predictions <- read_cache("predictions","contributions_model") %>% collect %>% setDT

  dataset[, date := as.Date(date)]
  predictions[, date := as.Date(date)]

  dataset_predictions <- dataset[predictions,on=c("group_customer_no","date")]

  future::plan("multisession")
  withr::local_options(future.globals.maxSize = 16*1024^3)

  p <- iml::Predictor$new(model,dataset_predictions[prob.TRUE>.5] %>% .[,
      sapply(.,\(c) {
        inherits(c,c("numeric","factor","integer","ordered","character")) &
        !all(is.na(c))}),
      with = F
    ],
    predict.function <- function(model,newdata) {
      model$predict_newdata(newdata)$prob[,"TRUE"]
    },
    y = "prob.TRUE")

  fi <- iml::FeatureImp$new(p,loss="logLoss")
  plot(fi)

  top_features <- fi$results[1:5,"feature"]
  future::plan("sequential")
  p$data$X <- p$data$X[p$data$X[,apply(.SD,1,\(.) !any(is.na(.))),.SDcols=top_features]]
  fe <- iml::FeatureEffects$new(p, top_features, method = "ale")
  plot(fe,fixed_y=F)

  s <- iml::Shapley$new(p,sample.size = 10)
  future::plan("multisession")
  dataset_predictions[prob.TRUE<.5][1,p$data$feature.names,with=F] %>%
    as.data.frame %>% s$explain()

  setDT(s$results)[order(-abs(s$results$phi)+s$results$phi.var*1.98),
            .(feature.value,phi/sum(phi))] %>% head

}
