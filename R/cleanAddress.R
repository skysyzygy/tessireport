
#' cleanAddress
#'
#' @param vec A character vector of addresses
#'
#' @return A character vector with directions and suffixes mapped to their USPS-approved abbreviated forms,
#' non-alphanumeric characters mapped to spaces and hyphens (-) removed.
#' @export
#'
#' @examples
#' vec <- c("25-13 east 25th street","43 1/2 southeast st james place")
#' cleanAddress(vec)
#'

cleanAddress = function(vec) {
  trimws(stringi::stri_replace_all(vec,
                                   regex=c(tolower(paste0("\\b",c(postmastr::dic_us_dir$dir.input,
                                                                  postmastr::dic_us_suffix$suf.input),"\\b")),
                                           "[^\\w\\-]+","-"),
                                   replacement=c(tolower(c(postmastr::dic_us_dir$dir.output,
                                                           postmastr::dic_us_suffix$suf.output)),
                                                 " ",""),
                                   vectorize_all=F))
}
