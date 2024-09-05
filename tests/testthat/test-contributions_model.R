withr::local_package("checkmate")
withr::local_package("mockery")


# contributions_dataset ------------------------------------------------

test_that("contributions_dataset reads from an ffdf and adds an event indicator", {
  tessilake::local_cache_dirs()
  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      group_customer_no = 1,
      event_type = c("Ticket","Contribution"),
      contributionAdjAmt = 50,
      timestamp = Sys.Date()+c(-60,-.001)),
      envir = rlang::caller_env())
  })
  stub(contributions_dataset, "close", TRUE)

  contributions_dataset()

  dataset <- read_cache("contributions_dataset","model") %>% collect
  expect_equal(nrow(dataset),2)
  expect_equal(dataset[,"event"][[1]],c(F,T))

})

test_that("contributions_dataset censors and subsets", {
  tessilake::local_cache_dirs()
  stub(contributions_dataset, "ffbase::load.ffdf", function(...) {
    assign("stream",data.table(
      # additional contibutions will be censored
      group_customer_no = rep(1:2,each=3),
      event_type = c("Ticket","Contribution","Contribution"),
      contributionAdjAmt = 50,
      # early dates will be removed and only one item per month will be returned (i.e. row 5)
      timestamp = rep(c(Sys.Date()-10,Sys.Date()-.001),each=3)),
      envir = rlang::caller_env())
  })
  stub(contributions_dataset, "close", TRUE)

  contributions_dataset(since = Sys.Date()-1)

  dataset <- read_cache("contributions_dataset","model") %>% collect
  expect_equal(nrow(dataset),1)
  expect_equal(dataset[1,"event"][[1]],TRUE)
  expect_equal(dataset[1,"group_customer_no"][[1]],2)
  expect_equal(dataset[1,"I"][[1]],5)

})


# read.contributions_model ------------------------------------------------
test_that("read.contributions_model calls contributions_dataset if asked to or if necessary", {
  stub(read.contributions_model,"cache_exists_any",TRUE)
  contributions_dataset <- mock()
  stub(read.contributions_model,"contributions_dataset",contributions_dataset)
  stub(read.contributions_model,"read_cache",data.table(group_customer_no=1,timestamp=1,date=Sys.time(),event=factor(c(T,F))))

  read(contributions_model,rebuild_dataset = T)
  expect_length(mock_args(contributions_dataset),1)

  stub(read.contributions_model,"cache_exists_any",FALSE)

  read(contributions_model)
  expect_length(mock_args(contributions_dataset),2)

})

test_that("read.contributions_model creates a valid mlr3 classification task", {
  stub(read.contributions_model, "read_cache",
       \(...) {arrow::read_parquet("test-contributions_model.parquet", as_data_frame = F)})

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

  suppressWarnings(model <<- train(model))

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

  output(model)

  pdf_filename <- cache_primary_path("contributions_model.pdf","contributions_model")
  exp_filename <- cache_primary_path("explanations.Rds","contributions_model")
  expect_file_exists(pdf_filename)
  expect_file_exists(exp_filename)

  explanations <- readRDS(exp_filename)
  expect_equal(nrow(explanations),model$predictions[prob.TRUE>.75,.N])
  setorder(explanations$explanation[[1]],-phi)
  expect_equal(explanations$explanation[[1]][1,feature],"ticketTimestampMax")


})


# stream_rollback_event ---------------------------------------------------

test_that("stream_rollback_event rolls back data matching `columns`", {
  dataset <- data.table(event = runif(1000)<.1,
                        leaky_data = seq(1000),
                        other_data = seq(1000),
                        by = 1)
  dataset_e <- copy(dataset)

  stream_rollback_event(dataset, columns = "leaky_data", by = "by")

  expect_equal(dataset[event != T], dataset_e[event != T])
  expect_equal(dataset[event == T, leaky_data], dataset_e[event == T, ifelse(leaky_data-1 == 0, NA, leaky_data-1)])
})

test_that("stream_rollback_event respects group boundaries", {
  dataset <- data.table(event = runif(1000)<.1,
                        leaky_data = seq(1000),
                        other_data = seq(1000),
                        by = sample(10,1000,replace = T))
  dataset_e <- copy(dataset)

  stream_rollback_event(dataset, columns = "leaky_data", by = "by")

  expect_equal(dataset[event != T], dataset_e[event != T])
  expect_equal(dataset[event == T & leaky_data >= other_data],dataset[integer(0)])
  for (i in seq(10)) {
    expect_in(dataset[by == i, leaky_data], c(NA,dataset_e[by == i, leaky_data]))
  }
})


# stream_normalize_timestamp ----------------------------------------------

test_that("stream_normalize_timestamps replaces all data matching `columns` with offsets", {
  dataset <- data.table(timestamp = seq(as.POSIXct("2020-01-01"), Sys.time(), by = "day"),
                        timestamp_max = seq(as.POSIXct("2020-01-01"), Sys.time(), by = "day")+1,
                        by = 1)

  stream_normalize_timestamps(dataset, by = "by")

  expect_equal(dataset[,as.double(timestamp)], seq(0,nrow(dataset)-1)*86400)
  expect_equal(dataset[,as.double(timestamp_max)], seq(0,nrow(dataset)-1)*86400+1)

})

test_that("stream_normalize_timestamps respects group boundaries", {
  dataset <- data.table(timestamp = seq(as.POSIXct("2020-01-01"), Sys.time(), by = "day"),
                        timestamp_max = seq(as.POSIXct("2020-01-01"), Sys.time(), by = "day")+1)

  dataset[,by := sample(10,.N,replace = T)]
  dataset_e <- copy(dataset)

  timestamp_mins <- dataset[,min(timestamp), by = by]
  stream_normalize_timestamps(dataset, by = "by")

  expect_equal(dataset[timestamp_mins,timestamp,on="by"],
               dataset_e[timestamp_mins,as.double(timestamp-V1),on="by"])
  expect_equal(dataset[timestamp_mins,as.double(timestamp_max),on="by"],
               dataset_e[timestamp_mins,as.double(timestamp_max-V1),on="by"])

})

