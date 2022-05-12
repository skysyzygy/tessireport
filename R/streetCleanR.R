#' streetCleanR
#'
#' @description Address auditing based on addressStream
#'
#' @param x an environment of data
#'
#' @rdname streetCleanR
#' @export
#'
new_streetCleanR = function(x=new.env()) {
  new_auditR(x,class="streetCleanR")
}

#' @export
streetCleanR = new_streetCleanR()

#' @export
#' @importFrom ffbase unpack.ffdf subset.ffdf
#' @importFrom ff delete
input.streetCleanR = function(x) {
  addressStreamFull <- timestamp <- primary_ind <- as.ram2 <- NULL
  streamDir = "//bam-fs-10/home$/jolson/My Documents/R/stream"
  source(file.path(Sys.getenv("R_USER"),"tools","as.ram.R"))
  unpack.ffdf(file.path(streamDir,"addressStreamFull.gz"))
  x$addressStream = subset.ffdf(addressStreamFull,timestamp>lubridate::today()-lubridate::ddays(14) &
                     primary_ind=='y') %>% as.ram2 %>% setDT
  delete(addressStreamFull)
  invisible(x)
}

#' process.streetCleanR
#'
#' @param x a streetCleanR object that has already been run through input
#'
#' @return a data.table with additional columns:
#' - suggestion.street1
#' - suggestion.street2
#' - suggestion.city
#' - suggestion.state
#' - suggestion.postal_code
#' - suggestion.source
#' - error (a list column of non-matching fields between the suggested and actual address)
#' suggestions are taken, in priority order, from Census (when matching exactly);
#' Google (when successful); with a fallback to Census (when matching partially).
#'
#'
#' @examples
#' \dontrun{
#' process(input(streetCleanR))
#' }
#'
#' @export
#' @import stringr
process.streetCleanR = function(x) {

  # Needed for R CMD check when working with NSE
  census.city <- census.postal_code <- census.state <-
  census.street1 <- census.street2 <- city <- country <- cxy_matched_address <-
  cxy_quality <- error <- google.administrative_area_level_1 <-
  google.administrative_area_level_2 <- google.administrative_area_level_3 <-
  google.city <- google.country <- google.establishment <- google.locality <-
  google.neighborhood <- google.political <- google.postal_code <-
  google.postal_town <- google.premise <- google.state <- google.status <-
  google.street1 <- google.street2 <- google.subpremise <- libpostal.city <-
  libpostal.country <- libpostal.house <- libpostal.po_box <-
  libpostal.postal_code <- libpostal.state <- libpostal.street1 <-
  libpostal.street2 <- libpostal.unit <- postal_code <- primary_ind <- state <-
  street1 <- street2 <- suggestion.city <- suggestion.country <- suggestion.source <-
  suggestion.state <- suggestion.street1 <- suggestion.street2 <- NULL

  addressStream = x$addressStream
  unit_regex = tolower("(Apartment(?!$)|APT(?!$)|Basement|BSMT|Building(?!$)|BLDG(?!$)|Department(?!$)|DEPT(?!$)|Floor|FL|Front|FRNT|Hanger(?!$)|HNGR(?!$)|Key(?!$)|KEY(?!$)|Lobby|LBBY|Lot(?!$)|LOT(?!$)|LOWR|Office|OFC|Penthouse|PH|Pier(?!$)|PIER(?!$)|Rear|REAR|Room(?!$)|RM(?!$)|Slip(?!$)|SLIP(?!$)|Space(?!$)|SPC(?!$)|Stop(?!$)|STOP(?!$)|Suite(?!$)|STE(?!$)|Trailer(?!$)|TRLR(?!$)|Unit(?!$)|UNIT(?!$)|UPPR|#(?!$))")
  unit_number_regex = "(\\d[\\d\\w]*|\\w\\d+|ph.{1,3})"


  addressStream[,colnames(addressStream):=lapply(.SD,function(.){if(is.factor(.)) {as.character(.)} else {.}})]
  addressStream[,grep("google",colnames(addressStream),value=T):=lapply(.SD,tolower),
                    .SDcols=grep("google",colnames(addressStream),value=T)]


  # build census address
  addressStream[,c("census.street","census.city","census.state","census.postal_code"):=
                      tstrsplit(tolower(cxy_matched_address),", ",fixed=T)]

  # NCOA rules are --
  # if there's a po box it goes in street1
  # ... unless it duplicates unit
  addressStream[str_detect(str_replace(libpostal.unit,"\\W+"," "),
                               fixed(str_replace(libpostal.po_box,"\\W+"," "))),
                    libpostal.po_box:=NA]
  addressStream[!is.na(libpostal.po_box),`:=`(libpostal.street1=libpostal.po_box,
                                                  census.street1=libpostal.po_box,
                                                  google.street1=libpostal.po_box)]

  # if the unit doesn't have a unit string in it then add a pound sign
  addressStream[!str_detect(libpostal.unit,unit_regex) & str_detect(libpostal.unit,unit_number_regex),
                    libpostal.unit:=paste("#",libpostal.unit)]
  addressStream[!str_detect(google.subpremise,unit_regex) & str_detect(google.subpremise,unit_number_regex),
                    google.subpremise:=paste("#",google.subpremise)]

  # combine libpostal pieces into unit
  addressStream[,libpostal.unit:=tidyr::unite(.SD,"libpostal.unit",na.rm=T,sep=" "),
                    .SDcols=c("libpostal.level","libpostal.unit","libpostal.entrance","libpostal.staircase")]
  addressStream[libpostal.unit=="",libpostal.unit:=NA]

  # google isn't so great at getting unit so use libpostal if it didn't get it right
  addressStream[is.na(google.subpremise),google.subpremise:=libpostal.unit]

  # and add to street1 if it's empty
  addressStream[ is.na(libpostal.street1),libpostal.street1:=tidyr::unite(.SD,"libpostal.street1",na.rm=T,sep=" "),
                     .SDcols=c("libpostal.house_number","libpostal.road","libpostal.unit")]
  addressStream[ is.na(census.street1),census.street1:=tidyr::unite(.SD,"census.street1",na.rm=T,sep=" "),
                     .SDcols=c("census.street","libpostal.unit")]
  addressStream[ is.na(google.street1),google.street1:=tidyr::unite(.SD,"google.street1",na.rm=T,sep=" "),
                     .SDcols=c("google.street_number","google.route","google.subpremise")]

  #if there's a name, it goes in street2
  addressStream[!is.na(libpostal.house),libpostal.street2:=libpostal.house]
  addressStream[!is.na(libpostal.house),census.street2:=libpostal.house]
  addressStream[!is.na(google.premise) | !is.na(google.establishment),google.street2:=
                      tidyr::unite(.SD,"google.street1",sep=", ",na.rm=T),.SDcols=c("google.establishment","google.premise")]

  # turn blanks into NA (unite doesn't do this)
  for(column in grep("\\.street\\d",colnames(addressStream),value=T)) {
    addressStream[get(column)=="",(column):=NA]
  }

  # city, state, zip, country
  # We aren't currently taking these from libpostal
  addressStream[,libpostal.city:=city]
  addressStream[,libpostal.state:=state]
  addressStream[,libpostal.postal_code:=postal_code]
  addressStream[,libpostal.country:=country]

  addressStream[,google.city:=dplyr::coalesce(google.postal_town,google.locality,google.political,google.neighborhood,
                                           google.administrative_area_level_3, #level_1 is state, level_2 is county, level_3 is city
                                           google.administrative_area_level_2,
                                           google.establishment)]
  addressStream[,google.state:=google.administrative_area_level_1]
  # google.postal_code and google.country already exist

  # Update match data based on google
  # addressStreamFull[(cleanAddress(google.street1)==cleanAddress(libpostal.street1) |
  #                   cleanAddress(google.street1)==cleanAddress(str_remove(libpostal.street1,fixed(paste0(" ",libpostal.unit)))))
  #                     & is.na(cxy_status),
  #                   `:=`(cxy_status="Match",cxy_quality="Exact")]

  # Build suggestions
  addressStream[cxy_quality=="Exact",`:=`(suggestion.street1=census.street1,
                                              suggestion.street2=census.street2,
                                              suggestion.city=census.city,
                                              suggestion.state=census.state,
                                              suggestion.postal_code=census.postal_code,
                                              suggestion.country="usa",
                                              suggestion.source="Census Exact")]

  addressStream[dplyr::coalesce(cxy_quality,"")!="Exact" &
                      google.status=="ok",`:=`(suggestion.street1=google.street1,
                                               suggestion.street2=google.street2,
                                               suggestion.city=google.city,
                                               suggestion.state=google.state,
                                               suggestion.postal_code=google.postal_code,
                                               suggestion.country=google.country,
                                               suggestion.source="Google")]


  addressStream[cxy_quality!="Exact" &
                      is.na(google.status),`:=`(suggestion.street1=census.street1,
                                                suggestion.street2=census.street2,
                                                suggestion.city=census.city,
                                                suggestion.state=census.state,
                                                suggestion.postal_code=census.postal_code,
                                                suggestion.country="usa",
                                                suggestion.source="Census Partial")]

  # no match
  addressStream[is.na(suggestion.source),error:=rep(list("No match"),.N)]

  #street1 mismatch
  addressStream[!cleanAddress(street1) %in%
                      cleanAddress(c(suggestion.street1,
                                     str_remove(suggestion.street1,fixed(paste0(" ",libpostal.unit))),
                                     str_remove(suggestion.street1,fixed(paste0(" ",google.subpremise))))),
                    error:=purrr::map(error,~append(.,"Street1"))]

  #street2 mismatch: doesn't match libpostal.street2 or libpostal.unit or libpostal.street2 minus unit
  addressStream[!cleanAddress(street2) %in%
                      cleanAddress(c(suggestion.street2,
                                     str_remove(suggestion.street1,fixed(paste0(" ",libpostal.unit))),
                                     str_remove(suggestion.street1,fixed(paste0(" ",google.subpremise))),
                                     libpostal.unit,
                                     google.subpremise)),
                    error:=purrr::map(error,~append(.,"Street2"))]


  #street3 mismatch
  # addressStreamFull[!cleanAddress(street3) %in%
  #                     cleanAddress(c(suggestion.street3,
  #                                    libpostal.unit,
  #                                    google.subpremise)),
  #                   error:=append(error,"Street3")]

  # city mismatch
  addressStream[!cleanAddress(city) %in%
                      cleanAddress(c(suggestion.city)),
                    error:=purrr::map(error,~append(.,"City"))]

  # state mismatch
  addressStream[!cleanAddress(state) %in%
                      cleanAddress(c(suggestion.state)),
                    error:=purrr::map(error,~append(.,"State"))]

  # country mismatch
  addressStream[!cleanAddress(country) %in%
                      cleanAddress(c(suggestion.country)),
                    error:=purrr::map(error,~append(.,"Country"))]

  invisible(x)
}

report.streetCleanR = function(x) {
  invisible(x)
}


