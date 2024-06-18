#' p2_segments_and_tags
#'
#' Report of Prospect2 segments and tags for review
#' @export
p2_segments_and_tags <- report(class="p2_segments_and_tags")
p2_query_api <- tessistream:::p2_query_api

#' @export
#' @describeIn p2_segments_and_tags load Prospect2 segments and tags
read.p2_segments_and_tags <- function(data) {

  keys <- c("segments","tags")
  lapply(keys,\(key) {

    value <- p2_query_api(file.path(tessistream:::api_url,"api/3/",key))[[key]]

    value$updated_timestamp <- unlist(value$updated_timestamp)
    setkey(value,updated_timestamp)

    data[[key]] <- value
  })

  NextMethod()
}

#' @export
#' @describeIn p2_segments_and_tags filter Prospect2 segments and tags
process.p2_segments_and_tags <- function(data,
                                         segment_regex = "^Segment of",
                                         tag_regex = "(?!.*RSVP)\\d{6,}") {

  data$segments <- data$segments[grepl(segment_regex,name, perl = T),
                                 .(name,created_timestamp)] %>% first(50)
  data$tags <- data$tags[grepl(tag_regex,tag, perl = T),
                         .(tag,created_timestamp)] %>% first(50)

  NextMethod()
}

#' @export
#' @describeIn p2_segments_and_tags send an email with spreadsheets of segments and tags
output.p2_segments_and_tags <- function(data, emails = config::get("tessiflow.email"), body = NULL) {

    filenames <- sapply(data,write_xlsx)

    send_email(subject = paste("P2 segments and tags",Sys.Date()),
               body = body %||% "<p>Hi!
               <p>Here are the first 50 segments and tags to review 😅
               <p>Please let me know if there are any that need to be saved.
               <p>Thanks!
               <p>Sky's computer",
               attach.files = filenames,
               html = TRUE,
               file.names = paste0(c("segments","tags"),"_",Sys.Date(),".xlsx"))

    NextMethod()
}
