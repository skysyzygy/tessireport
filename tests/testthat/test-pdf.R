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
  withr::local_package("ggplot2")
  plot <- ggplot(data.frame(x=rep(seq(100),seq(100)))) + geom_histogram(aes(x)) +
    scale_x_continuous(name="This is the x axis")
  capture.output(filename <- write_pdf(expr=pdf_plot(plot, "Histogram", "of random stuff")))

  text <- pdftools::pdf_text(filename)
  expect_match(text,"Histogram",all = F)
  expect_match(text,"of random stuff",all = F)
  expect_match(text,"This is the x axis", all = F)

  image <- as.integer(pdftools::pdf_render_page(pdf = filename, page = 1, dpi = 12))
  expect_snapshot_value(image, style="serialize", tolerance = 10)
})
