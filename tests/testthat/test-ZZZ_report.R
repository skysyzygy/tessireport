withr::local_package("checkmate")
withr::local_package("mockery")

# run.report ---------------------------------------------------------------------

test_that("run.report returns useful debugging info", {
  bad_fun <- function(...){stop("Oops!")}
  stub(run.report, "write", function(...) bad_fun(...))
  e <- tryCatch(run.report(report()),error = force)

  expect_match(e$message, "Oops!")
  expect_match(as.character(e$call), "bad_fun", all = F)
  expect_match(as.character(e$trace), "bad_fun", all = F)
  expect_match(as.character(e$trace), "write", all = F)
})

test_that("run.report returns helpful error if a method isn't defined", {
  inconsiderate_fun <- function(...){TRUE}
  stub(run.report, "write", function(...) inconsiderate_fun(...))

  e <- tryCatch(run.report(report()),error = force)

  expect_match(e$message, "Did you forget to return a `report` object")
})
