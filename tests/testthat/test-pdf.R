withr::local_package("checkmate")

test_that("write_pdf executes expr", {
  capture.output(filename <- write_pdf(cat(paste0("Hello I'm a pdf! 1+1=",1+1))))
  text <- pdftools::pdf_text(filename)
  expect_match(text,"Hello I.m a pdf! 1\\+1=2",all = F)
})

test_that("write_pdf includes title, author, and date", {
  capture.output(filename <- write_pdf(expr=NULL,.title = "Just a pdf",.author = "Me", .date = Sys.Date()))
  text <- pdftools::pdf_text(filename)
  expect_match(text,"Just a pdf",all = F)
  expect_match(text,"Me",all = F)
  expect_match(text,as.character(Sys.Date()),all = F)
})


# pdf_plot ----------------------------------------------------------------

test_that("pdf_plot generates a plot compatible with write_pdf",{
  plot <- ggplot(data.frame(x=rep(seq(100),seq(100)))) + geom_histogram(aes(x))
  capture.output(filename <- write_pdf(expr=pdf_plot(plot, "Histogram", "of random stuff")))

  text <- pdftools::pdf_text(filename)
  expect_match(text,"Histogram",all = F)
  expect_match(text,"of random stuff",all = F)

  image <- pdftools::pdf_convert(pdf = filename, format = "png", pages = 1, filenames = paste0(tempfile(),"_%d.%s"))
  expect_snapshot_file(image,"pdf_plot")
})
