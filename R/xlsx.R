
#' write_xlsx
#'
#' Convenience wrapper around `openxlsx::write.xlsx` to handle some common formatting
#' - column headers are title-cased and bolded
#' - column widths are calculated based on the string representations of the data
#' - Date columns are formatted as DATE instead of LONGDATE
#'
#' @param data data to write to the spreadsheet
#' @param filename filename to write
#'
#' @return filename of written xlsx
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom openxlsx addStyle createStyle setColWidths
#' @importFrom purrr map_lgl map_int
#' @importFrom lubridate is.Date is.POSIXct
write_xlsx <- function(data, filename = tempfile("write_xlsx", fileext = ".xlsx")) {
  wb <- createWorkbook()
  addWorksheet(wb, "Sheet 1")

  colnames(data) <-
    # snakecase to spaces
    gsub("_"," ",colnames(data)) %>%
    # camelcase to spaces
    {gsub("([a-z])([A-Z])","\\1 \\2",.)} %>%
    # capitalize first letter
    {gsub("^(.)","\\U\\1",.,perl=T)} %>%
    tools::toTitleCase()

  # bold the first row
  writeData(wb,1,data,
            rowNames = FALSE,
            headerStyle = createStyle(textDecoration = "bold"))

  # format date columns
  addStyle(wb,1,createStyle(numFmt = "DATE"),
           rows = 1:nrow(data) + 1,
           cols = which(map_lgl(data, ~is.Date(.))),
           stack = TRUE)

  # calculate column widths
  col_widths <- map_int(data, ~max(c(nchar(format(.)),0),na.rm=T))
  colname_widths <- nchar(colnames(data))
  setColWidths(wb,1,seq_along(data),pmax(col_widths, colname_widths) + 3)

  openxlsx::saveWorkbook(wb, filename)

  filename
}
