test_that("parse_shapley returns a human-redable version of a shapley analysis", {
  exp_filename <- here::here("tests/testthat/test-iml_shapley.Rds")
  explanations <- readRDS(exp_filename)

  expect_snapshot(lapply(explanations,parse_shapley))

  # formats dates
  expect_match(sapply(explanations,parse_shapley) %>% grep(pattern="timestamp",value=T), "timestamp.+days")
  # and big numbers
  expect_match(sapply(explanations,parse_shapley) %>% grep(pattern="income",value=T), "\\d+,\\d+{3}")

})
