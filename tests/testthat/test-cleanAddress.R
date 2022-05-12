test_that("cleanAddress shortens long directions", {
  expect_equal(cleanAddress("east north south southwest northfork"),"e n s sw northfork")
})

test_that("cleanAddress shortens long street names", {
  expect_equal(cleanAddress("street avenue boulevard placeholder"),"st ave blvd placeholder")
})

test_that("cleanAddress removes punctuation", {
  expect_equal(cleanAddress("25-14/34#3."),"2514 34 3")
})

test_that("cleanAddress works on vectors", {
  expect_equal(cleanAddress(c("east","street","-","3.14159")),c("e","st","","3 14159"))
})
