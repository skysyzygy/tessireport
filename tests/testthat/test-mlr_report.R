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
