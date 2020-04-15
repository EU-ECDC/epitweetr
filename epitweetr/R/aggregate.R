
#' Get all tweets from json files of search api and json file from geolocated tweets obtained by calling (geotag_tweets)
#' Saving aggregated data as weekly RDS files
#' @export
aggregate_tweets <- function(series = list("geolocated", "topwords")) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`

  # Getting a dataframe with dates and weeks to aggregate
  aggregate_weeks <- get_aggregate_weeks()

  # Creating series folder if does not exists
  if(!dir.exists(file.path(conf$dataDir, "series"))) {
    dir.create(file.path(conf$dataDir, "series")) 
  }

  if(nrow(aggregate_weeks)>0) {
    for(i in 1:nrow(aggregate_weeks)) {
      week <- aggregate_weeks$week[[i]]
      fromDate <- as.Date(aggregate_weeks$fromDate[[i]], "1970-01-01") 
      toDate <- as.Date(aggregate_weeks$toDate[[i]], "1970-01-01") 
      weekStart <- as.Date(aggregate_weeks$weekStart[[i]], "1970-01-01") 
      
      # Creating week folder if does not exists
      if(!dir.exists(file.path(conf$dataDir, "series", week))) {
        dir.create(file.path(conf$dataDir, "series", week)) 
      }
      for(i in 1:length(series)) { 
        # Aggregating tweets for the given serie and week
        agg_df <- get_aggregated_serie(series[[i]], fromDate, toDate, weekStart)
        
        # If result is not empty proceed to filter out weeks out of scope and save aggregated week serie 
        if(nrow(agg_df) > 0) {
          # Calculating the created week
          agg_df$created_week <- strftime(as.POSIXct(agg_df$created_date, origin = "1970-01-01"), format = "%G.%V")
          # Filtering only tweets for current week
          agg_df <- agg_df %>% dplyr::filter(created_week == week)
          agg_df$created_week <- NULL
          # Casting week and date to numeric values
          agg_df$created_weeknum <- as.integer(strftime(as.POSIXct(agg_df$created_date, origin = "1970-01-01"), format = "%G%V"))
          agg_df$created_date <- as.Date(as.POSIXct(agg_df$created_date, origin = "1970-01-01"))
          
          # saving the dataset to disk (with overwrite)
          message(paste("saving ", series[[i]]," data for week", week))
          saveRDS(agg_df, file = file.path(conf$dataDir, "series",week, paste(series[[i]], "Rds", sep = "."))) 
        } else {
          warning(paste("No rows found for", series[[i]]," on week", week ))  
        }
      }
    }  
  }
 
  #saving geolocated files by week
  sapply(unique(agg_df$created_weeknum),  function(week) {
    #taking only variables for one week
  })
  message("aggregation done!")
}

#' Getting already aggregated datasets
#' @export
get_aggregates <- function(dataset = "geolocated", cache = TRUE) {
  
  #If dataset is already on cache return it
  if(exists(dataset, where = cached)) {
    return (cached[[dataset]])
  }
  else {
    files <- list.files(path = file.path(conf$dataDir, "series"), recursive=TRUE, pattern = paste(dataset, ".Rds", sep=""))
    dfs <- lapply(files, function (file) readRDS(file.path(conf$dataDir, "series", file)))
    ret <- jsonlite::rbind_pages(dfs)
    if(cache) cached[[dataset]] <- ret
    return(ret)
  }
} 

#' Get a dataframe weeknames to aggregate having dates to search for each week
#' This corresponds to last three weeks before the first non aggregated weeks
#' All estimations are done by analyzing and parsing collected and aggregated file names
#' geo-tagging is assumed to be already dione
get_aggregate_weeks <- function() {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  
  #getting collected dates based on search files
  collected_dates <- list.files(file.path(conf$dataDir, "tweets", "search"), recursive=TRUE)
  collected_dates <- collected_dates[grepl(".*\\.json.gz", collected_dates)]
  collected_dates <- unique(sapply(collected_dates, function(f) {as.Date(gsub("\\.", "-", substr(tail(strsplit(f, "/")[[1]], n=1), 1, 10)))}))
  collected_dates <- data.frame(date = collected_dates, week = sapply(collected_dates, function(d) strftime(as.Date(d, "1970-01-01"), format = "%G.%V")))

  #aggregated weeks
  #creating the rDataFolder if it does not exists
  ifelse(!dir.exists(file.path(conf$dataDir, "series")), dir.create(file.path(conf$dataDir, "series")), FALSE)
  aggregated_weeks <- list.files(file.path(conf$dataDir, "series"), recursive=FALSE)
  aggregated_dates <- collected_dates[sapply(collected_dates$week, function(w) w %in% aggregated_weeks ), ]
  
  #setting the date to start reading for aggregating data. Tweets from a week can still be retrieves up to 10 days due tu API limitations
  #this means that aggregation should be recalculated for as two weeks already aggregated
  start_week <- ( 
    if(nrow(aggregated_dates) <= 2) #If there are no aggregations already done, we start reading from first collected week
      strftime(as.Date(min(collected_dates$date), "1970-01-01"), format = "%G.%V")
    else {#If the are already aggregated dates we start reading from last 2 aggregations
      strftime(as.Date(max(aggregated_dates$date), "1970-01-01") - 7 , format = "%G.%V")
    }
  )

  #Returning all collected dates after the start week
  weeks_to_collect <- collected_dates[sapply(collected_dates$week, function(w) as.integer(gsub("\\.", "", w))) >= as.integer(gsub("\\.", "", start_week)), ]
  #Grouping by week to collect and calculating the possible input dates for each week
  (weeks_to_collect 
    %>% dplyr::group_by(week) 
    %>% dplyr::summarise(fromDate = min(date)-10, toDate = max(date), weekStart = min(date)) 
    %>% dplyr::ungroup())
}


get_aggregated_serie <- function(serie_name, fromDate, toDate, created_start) {
  `%>%` <- magrittr::`%>%`
  message(paste("Aggregation serie", serie_name, "(", fromDate, "to", toDate, ")"))
 
  # Calculating the aggregated regex to mach json files to aggregate
  agg_regex <-lapply(fromDate:toDate, function(d) strftime(as.Date(d, "1970-01-01"), ".*%Y\\.%m\\.%d.*")) 

  if(serie_name == "geolocated") {
    get_geotagged_tweets(regexp = agg_regex
       , groupBy = list(
         "topic"
         , "date_format(created_at, 'yyyy-MM-dd') as created_date" 
         , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
         , paste(get_user_location_var("geo_country_code"), "as user_geo_country_code") 
         , paste(get_tweet_location_var("geo_code"), "as tweet_geo_code") 
         , paste(get_user_location_var("geo_code"), "as user_geo_code") 
       )
       , vars = list(
         paste("avg(", get_tweet_location_var("longitude"),") as tweet_logitude") 
         , paste("avg(", get_tweet_location_var("latitude"), ") as tweet_latitude")
         , paste("avg(", get_user_location_var("longitude"),") as user_longitude") 
         , paste("avg(", get_user_location_var("latitude"), ") as user_latitude")
         , "cast(count(*) as Integer) as tweets"
       )
       ,filterBy = list(
         paste("created_at >= '", strftime(created_start, "%Y-%m-%d"), "'", sep = "")
       )  
     )
  } else if(serie_name == "topwords") {
    # Getting top word aggregation for each
    top_chunk <- get_geotagged_tweets(regexp = agg_regex
      , vars = list(
        "topic"
        , "date_format(created_at, 'yyyy-MM-dd') as created_date" 
        , "date_format(created_at, 'yyyy-MM-dd HH:mm:ss') as created_at" 
        , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
        , paste(get_tweet_location_var("geo_code"), "as tweet_geo_code") 
        , "lang"
        , "text"
      )
      , sortBy = list(
        "topic"
        ,"created_at" 
      )
      ,filterBy = list(
         paste("created_at >= '", strftime(created_start, "%Y-%m-%d"), "'", sep = "")
      )  
      , handler = function(df, con_tmp) {
          pipe_top_words(df = df, text_col = "text", lang_col = "lang", group_by = c("topic", "created_date"), max_words = 1000, con_out = con_tmp, page_size = 1000)
      }
    )
    top_chunk %>% 
      dplyr::group_by(tokens, topic, created_date)  %>%
      dplyr::summarize(frequency = sum(count))  %>%
      dplyr::ungroup()  %>%
      dplyr::group_by(topic, created_date)  %>%
      dplyr::top_n(n = 200, wt = frequency) %>%
      dplyr::ungroup() 

  }
}
