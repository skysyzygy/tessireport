withr::local_package("checkmate")
withr::local_package("mockery")
tessilake::local_cache_dirs()

# save_model --------------------------------------------------------------

test_that("save_model serializes a model to a cache dir and syncs it", {
  model <- mlr3::lrn("classif.log_reg")
  withr::local_package("tessilake")

  save_model(model, "test_model", sync = F)

  expect_file_exists(cache_path("model.Rds","deep","test_model"))
  expect_failure(expect_file_exists(cache_path("test.model","shallow","test_model")))

  dir.create(cache_path("","shallow","test_model"))
  save_model(model, "test_model", sync = T)
  expect_file_exists(cache_path("model.Rds","shallow","test_model"))
  expect_file_exists(cache_path("model.Rds","deep","test_model"))

})

# load_model --------------------------------------------------------------

test_that("load_model deserilalizes a model from cache dir", {
  model <- mlr3::lrn("classif.log_reg")
  withr::local_package("tessilake")

  expect_equal(model,load_model("test_model"))
})


# arrow_to_mlr3 -----------------------------------------------------------

test_that("arrow_to_mlr3 converts an arrow table to an mlr3 backend", {
  table <-arrow::arrow_table(x = seq(26000), y = rep(letters,1000))

  expect_class(arrow_to_mlr3(table, primary_key = "x"), "DataBackendDuckDB")
})

test_that("arrow_to_mlr3 converts an arrow table to an mlr3 backend that can be queried", {
  table <- arrow::arrow_table(x = seq(26000), y = rep(letters,1000))
  future::plan("multisession")
  withr::defer(future::plan("sequential"))

  tbl <- arrow_to_mlr3(table, primary_key = "x")

  expect_equal(tbl$data(1:10,cols = c("x","y"), data_format = "data.table") %>%
               .[,y := as.character(y)],
               data.table(x = 1:10, y = letters[1:10]),
               ignore_attr = "sorted")

})


