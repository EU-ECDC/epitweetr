
#' Get all tweets from json files of search api and json file from geolocated tweets obtained by calling (geotag_tweets)
#' Saving aggregated data as weekly RDS files
#' @export
aggregate_tweets <- function() {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`

  #Getting a dataframe with dates and weeks to aggregate
  aggregate_dates <- get_aggregate_dates()

  #Calculating the aggregated regex to mach json files to aggregate
  agg_regex <-lapply(aggregate_dates$date, function(d) strftime(as.Date(d, "1970-01-01"), ".*%Y\\.%m\\.%d.*")) 
 
  #deleting aggregated files on matching weeks to recalculate
  sapply(unique(aggregate_dates$week), function(w) if(dir.exists(file.path(conf$dataDir, "series", w))) unlink(file.path(conf$dataDir, "series", w), recursive = TRUE)) 

  message(paste("Starting aggregation from", as.Date(min(aggregate_dates$date), "1970-01-01")))
 
  #Aggregating tweets by tpoic and country
  agg_df <- get_geotagged_tweets(regexp = agg_regex
    , vars = list(
      "topic"
      , "created_at"
      , get_tweet_location_var("longitude") 
      , get_tweet_location_var("latitude") 
      , get_tweet_location_var("geo_country_code") 
      , get_user_location_var("longitude") 
      , get_user_location_var("latitude") 
      , get_user_location_var("geo_country_code") 
    ))
  #Calculating the created week
  agg_df$created_weeknum <- as.integer(strftime(as.POSIXct(agg_df$created_at, origin = "1970-01-01"), format = "%Y%V"))
  agg_df$created_date <- as.Date(as.POSIXct(agg_df$created_at, origin = "1970-01-01"), format = "%Y%V")

  #Getting the first week that will be aggregated 
  first_weeknum <- as.integer(strftime(as.Date(min(aggregate_dates$date), "1970-01-01"), format = "%Y%V"))
  #Aggregating tweets by user and tweet location
  agg_df  <- (
    agg_df  
    %>% dplyr::filter(created_weeknum >= first_weeknum)
    %>% dplyr::group_by(created_weeknum, created_date, topic, user_geo_country_code, tweet_geo_country_code, user_longitude, user_latitude, tweet_longitude, tweet_latitude) 
    %>% dplyr::summarise(tweets = dplyr::n())
  )

  #saving geolocated files by week
  sapply(unique(agg_df$created_weeknum),  function(week) {
    #taking only variables for one week
    week_df <- agg_df %>% dplyr::filter(created_weeknum == week)
    #Transforming week number to YYYY.MM formay
    weekname <- paste(as.integer(week/100), paste(if(week %% 100 < 10) "0" else "", week %% 100, sep =""), sep=".")
    #creating folder if deos not exists
    if(!dir.exists(file.path(conf$dataDir, "series", weekname))) dir.create(file.path(conf$dataDir, "series", weekname)) 
    #saving geolocated week data as RDS file
    message(paste("saving geolocated data for week", weekname))
    saveRDS(week_df, file = file.path(conf$dataDir, "series",weekname, "geolocated.Rds")) 
  })
  message("aggregation done!")
}

get_aggregates <- function(dataset = "geolocated") {
  files <- list.files(path = file.path(conf$dataDir, "series"), recursive=TRUE, pattern = paste(dataset, ".Rds", sep=""))
  dfs <- lapply(files, function (file) readRDS(file.path(conf$dataDir, "series", file)))
  return(jsonlite::rbind_pages(dfs))
} 

#' Get a dataframes with dates and weeknames to aggregate
#' This corresponds to last three weeks before the first non aggregated weeks
#' All estimations are fone by analyzing and paring collected and aggregated file names
#' geo-tagging is assumed to be already dione
get_aggregate_dates <- function() {
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
  
  #setting the date to start reading for aggregating data. Tweets from a week can still be retrieves up to 10 days due tu API limitations
  #this means that aggregation should be recalculated for as two weeks already aggregated
  start_week <- ( 
    if(nrow(aggregated_dates) <= 2) #If there are no aggregations already done, we start reading from first collected week
      strftime(as.Date(min(collected_dates$date), "1970-01-01"), format = "%Y.%V")
    else {#If the are already aggregated dates we start reading from last 2 aggregations
      strftime(as.Date(max(aggregated_dates$date), "1970-01-01") - 7 , format = "%Y.%V")
    }
  )

  #Returning all collected dates after the start week
  collected_dates[sapply(collected_dates$week, function(w) as.integer(gsub("\\.", "", w))) >= as.integer(gsub("\\.", "", start_week)), ]
}



