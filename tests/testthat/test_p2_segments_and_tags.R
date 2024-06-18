withr::local_package("mockery")
withr::local_package("checkmate")


test_that("read.p2_segments_and_tags gets data from P2", {
  p2_query_api <- mock(list(segments = data.table(name = "", updated_timestamp = Sys.time())),
                       list(tags = data.table(tag = "", updated_timestamp = Sys.time())))

  stub(read.p2_segments_and_tags,"p2_query_api",p2_query_api)

  data <- read(p2_segments_and_tags)

  expect_length(mock_args(p2_query_api),2)
  expect_length(data, 2)

})

test_that("process.p2_segments_and_tags filters data from P2", {
  data <- list(segments = data.table(name = c("Segment of something","Save me!"),
                                     created_timestamp = Sys.time()),
               tags = data.table(tag = c("Sent 010101","010101_RSVPs"),
                                 created_timestamp = Sys.time()))

  data_filtered <- process(report(data, "p2_segments_and_tags"))

  expect_equal(data_filtered$segments$name, "Segment of something")
  expect_equal(data_filtered$tags$tag, "Sent 010101")

})
