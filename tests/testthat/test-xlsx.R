test_that("write_xlsx writes a readable file", {
  withr::local_package("openxlsx")
  withr::local_package("dplyr")

  bunch_of_data <- data.frame(A = letters,
                              B = runif(26),
                              C = rep(Sys.Date(),26),
                              D = rep(Sys.time(),26))


  filename <- write_xlsx(bunch_of_data)
  #system2("open",filename)
  expect_true(file.exists(filename))

  expect_equal(read.xlsx(filename, detectDates = TRUE) %>%
                 mutate(D = convertToDateTime(D)),
               bunch_of_data)
})

test_that("write_xlsx titlecases the column names", {
  withr::local_package("openxlsx")
  withr::local_package("dplyr")

  bunch_of_data <- data.frame(a_lot_of_letters = letters,
                              thisIsCamelCase = runif(26))

  filename <- write_xlsx(bunch_of_data)

  expect_equal(colnames(read.xlsx(filename, sep.names = " ")),c("A Lot of Letters","This is Camel Case"))
})
