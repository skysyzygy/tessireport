#' p2_orphans_report
#'
#' Sends an email containing a plot of recent orphans and a spreadsheet of all orphans.
#' @name p2_orphans_report
#' @export
p2_orphans_report <- new_report(class="p2_orphans_report")

#' @importFrom ggplot2 ggplot geom_histogram aes scale_fill_brewer theme_minimal
#' @importFrom dplyr case_when
#' @importFrom lubridate ddays
#' @importFrom grDevices dev.off png
#' @importFrom tessilake tessi_customer_no_map
#' @importFrom tessistream p2_orphans tessi_changed_emails
#' @importFrom purrr map
#' @describeIn p2_orphans_report runs the p2_orphans_report
#' @param freshness difftime,	the returned data will be at least this fresh
#' @export
run.p2_orphans_report <- function(freshness = 0) {
  . <- type <- timestamp <- id <- from <- to <- customer_no.x <- expr_dt <- memb_level <-
    last_updated_by <- customer_no <- i.customer_no <- NULL

  p2_orphans <- p2_orphans()
  tessi_emails <- tessi_changed_emails(since = 0, freshness = freshness)
  customer_no_map <- tessi_customer_no_map(freshness = freshness)

  # last change from `from`
  p2_orphan_events <- tessi_emails[p2_orphans,on=c("from"="address")] %>%
    .[,customer_no := coalesce(customer_no, i.customer_no)] %>%
    merge(customer_no_map, by = "customer_no", suffixes = c(".tessi_emails",""))

  p2_orphan_events[,type:=case_when(trimws(last_updated_by) %in% c("popmulti","addage") ~ "web",
                                    trimws(last_updated_by) %in% c("sqladmin","sa") ~ "merge",
                                    TRUE ~ "client") %>% forcats::fct_infreq()]

  png(image_file <- tempfile(fileext = ".png"))
  ggplot(p2_orphan_events[timestamp>'2022-08-01']) +
    geom_histogram(aes(timestamp,
                       fill=type),
                   binwidth=ddays(7)) +
    scale_fill_brewer(type="qual") +
    theme_minimal() -> p



  print(p)
  dev.off()

  memberships <- read_tessi("memberships", c("expr_dt","memb_level","customer_no")) %>%
    collect() %>% setDT() %>% .[,.SD[.N], by="group_customer_no"]

  p2_orphan_events <- merge(p2_orphan_events,memberships,all.x=T,by="group_customer_no",suffixes=c("",".memberships"))

  can_be_updated <- p2_orphan_events %>% split(1:nrow(.)) %>%
    map(~p2_resolve_orphan(.$from, .$to, customer_no = .$customer_no, dry_run = TRUE))


  xlsx_file <- write_xlsx(p2_orphan_events[,.(
    timestamp = as.Date(timestamp),
    "customer_#" = customer_no,
    p2_id = id,
    from_email = from,
    to_email = to,
    expiration_date = as.Date(expr_dt),
    member_level = memb_level,
    can_be_updated,
    change_type = type,
    last_updated_by
  )],
  xlsx_file <- tempfile(fileext = ".xlsx"))
  writeLines(paste0("<img src='",image_file,"'>"), html_file <- tempfile())

  send_email(
    subject = paste("P2 Orphan Report :",lubridate::today()),
    body = html_file,
    emails = "ssyzygy@bam.org",
    attach.files = xlsx_file,
    html = TRUE,
    file.names = paste0("p2_ophan_report_",lubridate::today(),".xlsx"),
    inline = TRUE
  )

}
