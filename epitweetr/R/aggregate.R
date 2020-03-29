
#' Get all tweets from json files of search api and json file from geolocated tweets obtained by calling (geotag_tweets)
#' @export
aggregate_tweets <- function() {
  #Getting a dataframe with dates and weeks to aggregate
  aggregate_dates <- get_aggregate_dates()
  
  #Calculating the aggregated regex to mach json files to aggregate
  agg_regex <-paste(sapply(aggregate_dates$date, function(d) strftime(as.Date(d, "1970-01-01"), ".*%Y\\.%m\\.%d.*")), collapse="|") 
  
  #deleting aggregated files on matching days
  sapply(unique(aggregate_dates$week), function(w) if(dir.exists(file.path(conf$dataDir, "series", w))) unlink(file.path(conf$dataDir, "series", w), recursive = TRUE)) 

  message(paste("Starting aggregation from week", as.Date(min(aggregate_dates$date), "1970-01-01")))
 
  
  get_geotagged_tweets(regexp = agg_regex
    , vars = list(
      "topic"
      ,"id"
    )) 
  

}



#' Get a dataframes with dates ans weeknames to aggregate
#' This corresponds to last three weeks before the first non aggregated weeks
#' All estimations are fone by analyzing and paring collected and aggregated file names
#' geo-tagging is assumed to be already dione
get_aggregate_dates <- function(last_week) {
  #getting collected dates based on search files
  collected_dates <- list.files(file.path(conf$dataDir, "tweets", "search"), recursive=TRUE)
  collected_dates <- collected_dates[grepl(".*\\.json.gz", collected_dates)]
  collected_dates <- unique(sapply(collected_dates, function(f) {as.Date(gsub("\\.", "-", substr(tail(strsplit(f, "/")[[1]], n=1), 1, 10)))}))
  collected_dates <- data.frame(date = collected_dates, week = sapply(collected_dates, function(d) strftime(as.Date(d, "1970-01-01"), format = "%Y.%V")))

  #aggregated weeks
  #creating the rDataFolder if it does not exists
  ifelse(!dir.exists(file.path(conf$dataDir, "series")), dir.create(file.path(conf$dataDir, "series")), FALSE)
  aggregated_weeks <- list.files(file.path(conf$dataDir, "series"), recursive=FALSE)
  aggregated_dates <- collected_dates[sapply(collected_dates$week, function(w) w %in% aggregated_weeks ), ]
  
  start_week <- ( 
    if(nrow(aggregated_dates) == 0) #If there are no aggregations already done, we start aggregatin from first collected week
      strftime(as.Date(min(collected_dates$date), "1970-01-01"), format = "%Y.%V")
    else #If the are already aggregated dates we start aggregating from week 10 days before las aggregation
      strftime(as.Date(max(aggregated_dates$date), "1970-01-01") - 10 , format = "%Y.%V")
  )
  #Returning all collected dates after the start week
  collected_dates[sapply(collected_dates$week, function(w) as.integer(gsub("\\.", "", w))) >= as.integer(gsub("\\.", "", start_week)), ]
}

