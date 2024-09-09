withr::local_package("checkmate")
withr::local_package("mockery")


# contributions_dataset ------------------------------------------------

test_that("contributions_dataset reads from an ffdf and adds an event indicator", {
  tessilake::local_cache_dirs()

  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      group_customer_no = 1,
      event_type = c("Ticket","Contribution"),
      contributionAmt = 50,
      timestamp = Sys.Date()+c(-60,-.001)),
      envir = rlang::caller_env())
  })
  stub(contributions_dataset, "close", TRUE)

  contributions_dataset()

  dataset <- read_cache("dataset","contributions_model") %>% collect
  expect_equal(nrow(dataset),2)
  expect_equal(dataset[,"event"][[1]],c(F,T))

})

test_that("contributions_dataset rebuilds all data when rebuild_dataset = T", {
  tessilake::local_cache_dirs()

  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      group_customer_no = 1,
      event_type = c("Ticket","Contribution"),
      contributionAmt = 50,
      timestamp = Sys.Date()+c(-60,-.001)),
      envir = rlang::caller_env())
  })
  stub(contributions_dataset, "close", TRUE)

  contributions_dataset()

  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      group_customer_no = 1:4,
      event_type = c("Ticket","Contribution"),
      contributionAmt = 50,
      timestamp = Sys.Date()+c(-60,-.001,365,366)),
      envir = rlang::caller_env())
  })

  dataset_chunk_write <- mock(T)
  stub(contributions_dataset, "dataset_chunk_write", dataset_chunk_write)
  contributions_dataset(rebuild_dataset = T)

  expect_length(mock_args(dataset_chunk_write),1)
  expect_equal(mock_args(dataset_chunk_write)[[1]][["partition"]], year(Sys.Date()))

})

test_that("contributions_dataset only appends data when rebuild_dataset = TRUE", {
  tessilake::local_cache_dirs()

  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      group_customer_no = 1,
      event_type = c("Ticket","Contribution"),
      contributionAmt = 50,
      timestamp = Sys.Date()+c(-60,-.001)),
      envir = rlang::caller_env())
  })
  stub(contributions_dataset, "close", TRUE)

  contributions_dataset()

  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      group_customer_no = 1:4,
      event_type = c("Ticket","Contribution"),
      contributionAmt = 50,
      timestamp = Sys.Date()+c(-60,-.001,365,366)),
      envir = rlang::caller_env())
  })

  dataset_chunk_write <- mock(T,cycle = T)
  stub(contributions_dataset, "dataset_chunk_write", dataset_chunk_write)

  contributions_dataset()
  expect_length(mock_args(dataset_chunk_write),0)

  contributions_dataset(until = Inf)
  expect_length(mock_args(dataset_chunk_write),1)
  expect_equal(mock_args(dataset_chunk_write)[[1]][["partition"]], year(Sys.Date())+1)

})

test_that("contributions_dataset only reads data when rebuild_dataset = F", {
  tessilake::local_cache_dirs()

  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      group_customer_no = 1,
      event_type = c("Ticket","Contribution"),
      contributionAmt = 50,
      timestamp = Sys.Date()+c(-60,-.001)),
      envir = rlang::caller_env())
  })
  stub(contributions_dataset, "close", TRUE)

  contributions_dataset()

  dataset_chunk_write <- mock(T)
  stub(contributions_dataset, "dataset_chunk_write", dataset_chunk_write)
  contributions_dataset(rebuild_dataset = F)

  expect_length(mock_args(dataset_chunk_write),0)

})

# read.contributions_model ------------------------------------------------

test_that("read.contributions_model creates a valid mlr3 classification task", {
  stub(read.contributions_model, "contributions_dataset",
       \(...) {arrow::read_parquet(here::here("tests/testthat/test-contributions_model.parquet"), as_data_frame = F)})

  stub(read.contributions_model,"cache_exists_any",TRUE)

  model <<- read(contributions_model, predict_since = as.Date("2024-06-01"), rebuild_dataset = F)

  expect_class(model$task, "TaskClassif")

})

test_that("read.contributions_model creates a valid mlr3 validation task", {

  expect_class(model$task$internal_valid_task, "TaskClassif")
  data <- model$task$internal_valid_task$data(cols = "date")
  expect_true(all(data$date >= as.Date("2024-06-01")))

})


# train.contributions_model -----------------------------------------------
tessilake::local_cache_dirs()
test_that("train.contributions_model successfully trains a model", {
  future::plan("multisession")

  suppressWarnings(model <<- train(model))
  saveRDS(model$model, here::here("tests/testthat/test-contributions_model.Rds"))

  expect_class(model$model, "Learner")

})

# predict.contributions_model ---------------------------------------------

test_that("predict.contributions_model successfully predicts new data", {

  model <<- predict(model)

  expect_data_table(model$predictions)
  expect_names(names(model$predictions),must.include = c("group_customer_no","date","truth","prob.TRUE"))

})


# output.contributions_model ---------------------------------------------

test_that("output.contributions_model successfully interprets the model", {

  model$model <- readRDS(here::here("tests/testthat/test-contributions_model.RDs"))

  # predict the whole thing
  model$predictions <-
    cbind(as.data.table(model$model$predict(model$task)),
          model$task$data(cols = c("I","group_customer_no","date")))

  # downgrade some predictions
  model$predictions[,prob.TRUE := runif(.N)^2*prob.TRUE]

  # dataset
  d <- arrow::read_parquet(here::here("tests/testthat/test-contributions_model.parquet"), as_data_frame = F) %>% collect %>% setDT
  # fill in missing data because the test set is largely missing and add some random noise
  cols <- setdiff(colnames(d)[which(sapply(d,is.numeric))],c("group_customer_no","date","I"))
  d[,(cols) := lapply(.SD, \(.) coalesce(.,0) + runif(.N,min=-1,max=1)),.SDcols = cols]

  stub(output.mlr_report, "read_cache", d)

  output(model)

  pdf_filename <- cache_primary_path("contributions_model.pdf","contributions_model")
  exp_filename <- cache_primary_path("shapley.Rds","contributions_model")
  expect_file_exists(pdf_filename)
  expect_file_exists(exp_filename)

  explanations <- readRDS(exp_filename)
  expect_equal(nrow(explanations),model$predictions[prob.TRUE>.75,.N])
  setorder(explanations$explanation[[1]],-phi)
  expect_equal(explanations$explanation[[1]][1,feature],"ticketTimestampMax")

})



