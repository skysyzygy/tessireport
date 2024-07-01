withr::local_package("checkmate")
withr::local_package("mockery")

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

