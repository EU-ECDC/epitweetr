
#' Get all tweets from json files of search api and json file from geolocated tweets obtained by calling (geotag_tweets)
#' Saving aggregated data as weekly RDS files
#' @export
aggregate_tweets <- function(series = list("geolocated", "topwords", "country_counts"), tasks = get_tasks()) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`

  tasks <- tryCatch({
    # Setting task status to running
    tasks <- update_aggregate_task(tasks, "running", "processing", start = TRUE)
    # Creating series folder if does not exists
    if(!dir.exists(file.path(conf$data_dir, "series"))) {
      dir.create(file.path(conf$data_dir, "series")) 
    }

    for(i in 1:length(series)) { 
      # Getting a dataframe with dates and weeks to aggregate
      aggregate_weeks <- get_aggregate_weeks(series[[i]])
    
      if(nrow(aggregate_weeks)>0) {
        for(j in 1:nrow(aggregate_weeks)) {
          week <- aggregate_weeks$week[[j]]
          read_from <- as.Date(aggregate_weeks$read_from[[j]], "1970-01-01") 
          read_to <- as.Date(aggregate_weeks$read_to[[j]], "1970-01-01") 
          created_from <- as.Date(aggregate_weeks$created_from[[j]], "1970-01-01") 
          created_before <- as.Date(aggregate_weeks$created_before[[j]], "1970-01-01") 
          
          # Creating week folder if does not exists
          if(!dir.exists(file.path(conf$data_dir, "series", week))) {
            dir.create(file.path(conf$data_dir, "series", week)) 
          }

          # Aggregating tweets for the given serie and week
          tasks <- update_aggregate_task(tasks, "running", paste("serie",  series[[i]], "from", read_from, "to", read_to))
          agg_df <- get_aggregated_serie(series[[i]], read_from, read_to, created_from, created_before)
          
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
            saveRDS(agg_df, file = file.path(conf$data_dir, "series",week, paste(series[[i]], "Rds", sep = "."))) 
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
    # Setting status to succès
    tasks <- update_aggregate_task(tasks, "success", "", end = TRUE)
    tasks

  }, error = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$aggregate$message," languages", error_condition))
    tasks <- update_aggregate_task(tasks, "failed", paste("failed while", tasks$aggregate$message," languages", error_condition))
    tasks
  }, warning = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$aggregate$message," languages", error_condition))
    tasks <- update_aggregate_task(tasks, "failed", paste("failed while", tasks$aggregate$message," languages", error_condition))
    tasks
  })
  return(tasks)
}

#' Getting already aggregated datasets
#' @export
get_aggregates <- function(dataset = "geolocated", cache = TRUE, filter = list()) {
  `%>%` <- magrittr::`%>%`
  
  filter$last_aggregate <- get_tasks()$aggregate$end_on  
  same_filter <- exists("last_filter", where = cached) && all(names(cached$last_filter) == names(filter)) && all(unlist(cached$last_filter) == unlist(filter))
  cached$last_filter <- filter
  #If dataset is already on cache return it
  if(cache && exists(dataset, where = cached) && same_filter) {
    return (cached[[dataset]])
  }
  else {
    files <- list.files(path = file.path(conf$data_dir, "series"), recursive=TRUE, pattern = paste(dataset, ".Rds", sep=""))
    if(length(files) == 0) {
      warning(paste("Dataset ", dataset, " not found in any wek folder inside", conf$data_dir, "/series. Please make sure the data/series folder is not empty and run aggregate process", sep = ""))  
      return (data.frame(created_date=as.Date(character()),topic=character()))
    }
    else {
      dfs <- lapply(files, function (file) {
        if(exists("topic", where = filter) && exists("period", where = filter)) 
	  readRDS(file.path(conf$data_dir, "series", file)) %>% dplyr::filter( topic == filter$topic & created_date >= filter$period[[1]] & created_date <= filter$period[[2]])
        else if(exists("period", where = filter)) 
	  readRDS(file.path(conf$data_dir, "series", file)) %>% dplyr::filter(created_date >= filter$period[[1]] & created_date <= filter$period[[2]])
        else if(exists("topic", where = filter)) 
	  readRDS(file.path(conf$data_dir, "series", file)) %>% dplyr::filter( topic == filter$topic)
        else 
	  readRDS(file.path(conf$data_dir, "series", file))
      })
      ret <- jsonlite::rbind_pages(dfs) 
      if(cache) cached[[dataset]] <- ret
      return(ret)
    }
  }
} 

#' Get a dataframe weeknames to aggregate having dates to search for each week
#' This corresponds to last three weeks before the first non aggregated weeks
#' All estimations are done by analyzing and parsing collected and aggregated file names
#' geo-tagging is assumed to be already dione
get_aggregate_weeks <- function(dataset) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  
  #getting collected dates based on search files
  collected_dates <- list.files(file.path(conf$data_dir, "tweets", "search"), recursive=TRUE)
  collected_dates <- collected_dates[grepl(".*\\.json.gz", collected_dates)]
  collected_dates <- unique(sapply(collected_dates, function(f) {as.Date(gsub("\\.", "-", substr(tail(strsplit(f, "/")[[1]], n=1), 1, 10)))}))
  collected_dates <- data.frame(date = collected_dates, week = sapply(collected_dates, function(d) strftime(as.Date(d, "1970-01-01"), format = "%G.%V")))

  #creating the series folder if it does not exists
  ifelse(!dir.exists(file.path(conf$data_dir, "series")), dir.create(file.path(conf$data_dir, "series")), FALSE)
  
  #aggregated weeks
  aggregated_weeks <- list.files(file.path(conf$data_dir, "series"), recursive=TRUE)
  aggregated_weeks <- aggregated_weeks[grepl(paste(".*", dataset, "\\.Rds", sep = ""), aggregated_weeks)] 
  aggregated_weeks <- sapply(aggregated_weeks, function(w) gsub(paste("/", dataset, "\\.Rds", sep = ""), "",w))
  aggregated_dates <- collected_dates[sapply(collected_dates$week, function(w) w %in% aggregated_weeks ), ]
  
  #setting the date to start reading for aggregating data. Tweets from a week can still be retrieves up to 10 days due tu API limitations
  #this means that aggregation should be recalculated for as two weeks already aggregated
  start_week <- ( 
    if(nrow(aggregated_dates) <= 2) #If there are no aggregations already done, we start reading from first collected week
      strftime(as.Date(min(collected_dates$date), "1970-01-01"), format = "%G.%V")
    else {#If the are already aggregated dates we start reading one week before las aggregared week
      strftime(as.Date(max(aggregated_dates$date), "1970-01-01") - 7 , format = "%G.%V")
    }
  )

  #Returning all collected dates after the start week
  weeks_to_collect <- collected_dates[sapply(collected_dates$week, function(w) as.integer(gsub("\\.", "", w))) >= as.integer(gsub("\\.", "", start_week)), ]
  #Grouping by week to collect and calculating the possible input dates for each week
  (weeks_to_collect 
    %>% dplyr::group_by(week) 
    %>% dplyr::summarise(read_from = min(date), read_to = max(date) + 10, created_from = min(date), created_before = max(date) + 1) 
    %>% dplyr::ungroup())
}


get_aggregated_serie <- function(serie_name, read_from, read_to, created_from, created_before) {
  `%>%` <- magrittr::`%>%`
  message(paste("Aggregating series", serie_name, "(", created_from, "to before", created_before, ") by looking on tweets collected between (", read_from, "until", read_to, ")"))
 
  # Calculating the aggregated regex to mach json files to aggregate
  agg_regex <-lapply(read_from:read_to, function(d) strftime(as.Date(d, "1970-01-01"), ".*%Y\\.%m\\.%d.*")) 

  if(serie_name == "geolocated") {
    get_geotagged_tweets(regexp = agg_regex
       , sources_exp = list(
           tweet = c(
             list("date_format(created_at, 'yyyy-MM-dd') as created_date", "is_retweet")
             , get_user_location_columns("tweet")
           )
           ,geo = c(
             get_tweet_location_columns("geo") 
             , get_user_location_columns("geo") 
             ) 
       )
       , vars = list(
           paste("avg(", get_tweet_location_var("longitude"), ") as tweet_longitude") 
           , paste("avg(", get_tweet_location_var("latitude"), ") as tweet_latitude")
           , paste("avg(", get_user_location_var("longitude"), ") as user_longitude") 
           , paste("avg(", get_user_location_var("latitude"), ") as user_latitude")
           , "cast(sum(case when is_retweet then 1 else 0 end) as Integer) as retweets"
           , "cast(sum(case when is_retweet then 0 else 1 end) as Integer) as tweets"
       )
       , filter_by = list(
         paste("created_at >= '", strftime(created_from, "%Y-%m-%d"), "'", sep = "")
         , paste("created_at < '", strftime(created_before, "%Y-%m-%d"), "'", sep = "")
       )  
       , group_by = list(
         "topic"
         , "created_date" 
         , paste(get_user_location_var("geo_country_code"), "as user_geo_country_code") 
         , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
         , paste(get_user_location_var("geo_code"), "as user_geo_code")
         , paste(get_tweet_location_var("geo_code"), "as tweet_geo_code")
       )
     )
  } else if(serie_name == "topwords") {
    # Getting top word aggregation for each
    top_chunk <- get_geotagged_tweets(regexp = agg_regex
      , sources_exp = list(
          tweet = list(
            "date_format(created_at, 'yyyy-MM-dd') as created_date"
	    , "is_retweet"
            , "date_format(created_at, 'yyyy-MM-dd HH:mm:ss') as created_at" 
            , "lang"
            , "text"
           )
          ,geo = get_user_location_columns("geo") 
        )
      , sort_by = list(
        "topic"
        , "tweet_geo_country_code" 
        , "created_at" 
      )
      , filter_by = list(
         paste("created_at >= '", strftime(created_from, "%Y-%m-%d"), "'", sep = "")
         , paste("created_at < '", strftime(created_before, "%Y-%m-%d"), "'", sep = "")
      )  
      , vars = list(
        "topic"
        , "created_date" 
        , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
        , paste(get_tweet_location_var("geo_code"), "as tweet_geo_code") 
        , "lang"
        , "text"
	, "is_retweet"
      )
      , handler = function(df, con_tmp) {
          pipe_top_words(df = df, text_col = "text", lang_col = "lang", max_words = 500, con_out = con_tmp, page_size = 500)
      }
    )
    top_chunk %>% 
      dplyr::group_by(tokens, topic, created_date, tweet_geo_country_code)  %>%
      dplyr::summarize(frequency = sum(count), original = sum(original), retweets = sum(retweets))  %>%
      dplyr::ungroup()  %>%
      dplyr::group_by(topic, created_date, tweet_geo_country_code)  %>%
      dplyr::top_n(n = 200, wt = frequency) %>%
      dplyr::ungroup() 

  } else if(serie_name == "country_counts") {
    # Getting the expression for known users
    known_user <- 
      paste("screen_name in ('"
        , paste(get_known_users(), collapse="','")
	, "') or linked_screen_name in ('"
        , paste(get_known_users(), collapse="','")
	, "') ",
	sep = ""
      )
    replacementfile <- tempfile(pattern = "repl", fileext = ".txt")
    f <-file(replacementfile)
    writeLines(c(
      paste("@known_retweets:cast(sum(case when is_retweet and ", known_user, "then 1 else 0 end) as Integer) as known_retweets")
      , paste("@known_original:cast(sum(case when not is_retweet and ", known_user, "then 1 else 0 end) as Integer) as known_original")
      ), f)
    close(f) 
    # Aggregation by country level
    get_geotagged_tweets(regexp = agg_regex
       , sources_exp = list(
           tweet = c(
             list("created_at", "is_retweet", "screen_name", "linked_screen_name")
             , get_user_location_columns("tweet")
           )
           ,geo = c(
             get_tweet_location_columns("geo") 
             , get_user_location_columns("geo") 
             ) 
       )
      , group_by = list(
        "topic"
        , "date_format(created_at, 'yyyy-MM-dd') as created_date" 
        , "date_format(created_at, 'HH') as created_hour" 
        , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
        , paste(get_user_location_var("geo_country_code"), "as user_geo_country_code") 
      )
      , vars = list(
        "cast(sum(case when is_retweet then 1 else 0 end) as Integer) as retweets"
        , "cast(sum(case when is_retweet then 0 else 1 end) as Integer) as tweets"
        , "@known_retweets"
        , "@known_original"
       )
      , filter_by = list(
         paste("created_at >= '", strftime(created_from, "%Y-%m-%d"), "'", sep = "")
         , paste("created_at < '", strftime(created_before, "%Y-%m-%d"), "'", sep = "")
      )
      , params = replacementfile
    )
  }
}
