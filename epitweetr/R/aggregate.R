
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
      aggregate_days <- get_aggregate_dates(series[[i]])
    
      if(length(aggregate_days)>0) {
        for(j in 1:length(aggregate_days)) {
          created_date <- aggregate_days[[j]]$created_date  
          files <- aggregate_days[[j]]$files
          week <- strftime(created_date, format = "%G.%V")
           
          # Creating week folder if does not exists
          if(!dir.exists(file.path(conf$data_dir, "series", week))) {
            dir.create(file.path(conf$data_dir, "series", week)) 
          }

          # Aggregating tweets for the given serie and week
          tasks <- update_aggregate_task(tasks, "running", paste("serie",  series[[i]], "from", created_date))
          agg_df <- get_aggregated_serie(series[[i]], created_date, files)
          
          # If result is not empty proceed to filter out weeks out of scope and save aggregated week serie 
          if(nrow(agg_df) > 0) {
            # Calculating the created week
            agg_df$created_week <- strftime(as.Date(agg_df$created_date, format = "%Y-%m-%d"), format = "%G.%V")
            # Filtering only tweets for current week
            agg_df <- agg_df %>% dplyr::filter(created_week == week)
            agg_df$created_week <- NULL
            # Casting week and date to numeric values
            agg_df$created_weeknum <- as.integer(strftime(as.Date(agg_df$created_date, format = "%Y-%m-%d"), format = "%G%V"))
            agg_df$created_date <- as.Date(agg_df$created_date, format = "%Y-%m-%d")
            
            # saving the dataset to disk (with overwrite)
            message(paste("saving ", series[[i]]," data for week", week, ", day", created_date))
            saveRDS(agg_df, file = file.path(conf$data_dir, "series",week, paste(series[[i]], "_", strftime(created_date, format = "%Y.%m.%d."), "Rds", sep = ""))) 
          } else {
            message(paste("No rows found for", series[[i]]," on week", week, ", day", created_date ))  
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
get_aggregates <- function(dataset = "country_counts", cache = TRUE, filter = list()) {
  `%>%` <- magrittr::`%>%`
  last_filter_name <- paste("last_filter", dataset, sep = "_")
  filter$last_aggregate <- get_tasks()$aggregate$end_on  
  reuse_filter <- 
    exists(last_filter_name, where = cached) && 
    (!exists("topic", cached[[last_filter_name]]) ||
      (
        exists("topic", cached[[last_filter_name]]) && 
        exists("topic", filter) &&  
        all(filter$topic %in% cached[[last_filter_name]]$topic)
      )
    ) &&
    (!exists("period", cached[[last_filter_name]]) ||
      (
        exists("period", cached[[last_filter_name]]) && 
        exists("period", filter) &&
        cached[[last_filter_name]]$period[[1]] <= filter$period[[1]] &&
        cached[[last_filter_name]]$period[[2]] >= filter$period[[2]]
      )
    )

  # Overriding current filter when no cache hit
  if(!reuse_filter) cached[[last_filter_name]] <- filter
  # On cache hit returning from cache
  if(cache && exists(dataset, where = cached) && reuse_filter) {
    return (cached[[dataset]] %>% 
      dplyr::filter(
        (if(exists("topic", where = filter)) topic %in% filter$topic else TRUE) & 
        (if(exists("period", where = filter)) created_date >= filter$period[[1]] & created_date <= filter$period[[2]] else TRUE)
      )
    )
  }
  else {
    # No cache hit getting from aggregated files
    files <- list.files(path = file.path(conf$data_dir, "series"), recursive=TRUE, pattern = paste(dataset, ".*\\.Rds", sep=""))
    if(length(files) == 0) {
      warning(paste("Dataset ", dataset, " not found in any week folder inside", conf$data_dir, "/series. Please make sure the data/series folder is not empty and run aggregate process", sep = ""))  
      return (data.frame(created_date=as.Date(character()),topic=character()))
    }
    else {
      # Limiting files by the selected period based on week name and file names if filtering for date
      if(exists("period", where = filter)) {
        from <- filter$period[[1]]
        until <- filter$period[[2]]
        files <- files[
          sapply(files, function(f) {
            weekstr <- strsplit(f, "/")[[1]][[1]]
            day <- as.Date(tail(strsplit(gsub(".Rds", "", f), "_")[[1]], n = 1), "%Y.%m.%d")
            strftime(from, "%G.%V")<=weekstr && 
              strftime(until, "%G.%V") >= weekstr && 
              (is.na(day) || from <= day && until >= day)
          })
        ]
      }

      # Exracting data from aggregated files
      dfs <- lapply(files, function (file) {
	      readRDS(file.path(conf$data_dir, "series", file)) %>% 
          dplyr::filter(
            (if(exists("topic", where = filter)) topic %in% filter$topic else TRUE) & 
            (if(exists("period", where = filter)) created_date >= filter$period[[1]] & created_date <= filter$period[[2]] else TRUE)
          )
      })
      
      #Joining data exctractes if any or retunning empty dataser otherwise
      ret <- 
        if(length(files) > 0)
          jsonlite::rbind_pages(dfs)
        else 
          readRDS(file.path(conf$data_dir, "series", tail(file, n = 1))) %>% dplyr::filter(1 == 0)
      if(cache) cached[[dataset]] <- ret
      return(ret)
    }
  }
} 

#' getting last geolocated date
get_geolocated_period <- function(dataset) {
  last_geolocate <- list.dirs(file.path(conf$data_dir, "tweets", "geolocated"), recursive=TRUE)
  last_geolocate <- last_geolocate[grepl(".*\\.json.gz", last_geolocate)]
  if(length(last_geolocate)==0) stop("To aggregate, or calculate alerts geolocation must have been succesfully executed, but no geolocation files where found")
  first_geolocate <- as.Date(min(sapply(last_geolocate, function(f) {as.Date(gsub("\\.", "-", substr(tail(strsplit(f, "/")[[1]], n=1), 1, 10)))})), origin = '1970-01-01')
  last_geolocate <- as.Date(max(sapply(last_geolocate, function(f) {as.Date(gsub("\\.", "-", substr(tail(strsplit(f, "/")[[1]], n=1), 1, 10)))})), origin = '1970-01-01')
  list(first = first_geolocate, last = last_geolocate) 
}

#' getting last aggregation date or NA if first
get_aggregated_period <- function(dataset) {
  agg_files <- list.files(file.path(conf$data_dir, "series"), recursive=TRUE, full.names =TRUE)
  agg_files <- agg_files[grepl(paste(".*", dataset, ".*\\.Rds", sep = ""), agg_files)] 
  agg_files <- sort(agg_files)
  if(length(agg_files) > 0) { 
   first_file <- agg_files[[1]]
   first_df <- readRDS(first_file)
   last_file <- agg_files[[length(agg_files)]]
   last_df <- readRDS(last_file)
   last_hour <- 
     if(dataset == "country_counts") 
       max(strptime(paste(strftime(last_df$created_date, format = "%Y-%m-%d"), " ", last_df$created_hour, ":00:00", sep=""), format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))
     else
       max(strptime(paste(strftime(last_df$created_date, format = "%Y-%m-%d"), " ", "00:00:00", sep=""), format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))
   list(
     first = min(first_df$created_date), 
     last = as.Date(last_hour),
     last_hour= as.integer(strftime(last_hour, format = "%H"))
   )   
  } else 
   list(first = NA, last = NA, last_hour = NA)
}

#' Get a list of dates that should be aggregated for the given serie
#' This is calculated based on statistic files generated by tweet collection and existing series and successfull geolocation
#' @export
get_aggregate_dates <- function(dataset) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  
  #creating the series folder if it does not exists
  ifelse(!dir.exists(file.path(conf$data_dir, "series")), dir.create(file.path(conf$data_dir, "series")), FALSE)
  
  #getting last geolocated date
  geo_period <- get_geolocated_period(dataset)
  first_geolocate <- geo_period$first
  last_geolocate <- geo_period$last
        
  #Getting the last day aggregated for this serie
  #if no aggregation exists, aggregation will start on first available geolocation
  last_aggregate <- get_aggregated_period(dataset)$last
  last_aggregate <- if(!is.na(last_aggregate)) last_aggregate else first_geolocate

  #We know that all tweets collected before last_aggregate date have been already aggregated
  #Twets collected after the last aggregation are ignored
  #Aggregation imcumbes all tweets collected between last_aggregate and last_geolocate
  #We have to find the oldest tweet creation date collected during that period aggregation will start on that day
  stat_files <-  list.files(file.path(conf$data_dir, "stats"), recursive=TRUE, full.names =TRUE)
  created_from <-
    as.Date(
      min(
        sapply(stat_files, function(f) {
          stats <- jsonlite::read_json(f, simplifyVector = FALSE, auto_unbox = TRUE)
          min(sapply(stats, function(t) {if(as.Date(t$collected_from) >= last_aggregate && as.Date(t$created_from) < last_aggregate) as.Date(t$created_from) else last_aggregate}))  
        })
      ),
      origin='1970-01-01'
    )
  
  created_to <- last_geolocate
  ret <- list()
  if(last_geolocate >= created_to) {
    for(i in 1:length(created_from:created_to)) {
      ret[[i]] <- list(created_date = created_from + (i-1), files = list())
      for(f in stat_files) {
        stats <- jsonlite::read_json(f, simplifyVector = FALSE, auto_unbox = TRUE)
        for(t in stats) {
           if(as.Date(t$created_from) <= ret[[i]]$created_date && as.Date(t$created_to) >= ret[[i]]$created_date) {
             ret[[i]]$files <- unique(c(ret[[i]]$files, substr(tail(strsplit(f, "/")[[1]], n = 1), 1, 10))) 
           }
        }
      }  
    }
  }
  ret[sapply(ret, function(d) length(d$files)>0)]  
}


get_aggregated_serie <- function(serie_name, created_date, files) {
  `%>%` <- magrittr::`%>%`
  message(paste("Aggregating series", serie_name, "(", created_date, ") by looking on tweets collected between (", paste(files ,collapse=", "), ")"))
 
  # Calculating the aggregated regex to mach json files to aggregate
  agg_regex <-lapply(files, function(s) paste(".*", gsub("\\.", "\\\\\\.", s),".*", sep= "")) 

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
         paste("date_format(created_at, 'yyyy-MM-dd') = '", strftime(created_date, "%Y-%m-%d"), "'", sep = "")
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
         paste("date_format(created_at, 'yyyy-MM-dd') = '", strftime(created_date, "%Y-%m-%d"), "'", sep = "")
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
   if(nrow(top_chunk)==0) {
     data.frame(topic = character(), created_date=character(), tweet_geo_country_code=character(), tokens=character(), frequency=numeric(), original=numeric(), retwets=numeric())
    } else {
      if(!("tweet_geo_country_code" %in% colnames(top_chunk))) top_chunk$tweet_geo_country_code <- NA

      top_chunk %>% 
        dplyr::group_by(tokens, topic, created_date, tweet_geo_country_code)  %>%
        dplyr::summarize(frequency = sum(count), original = sum(original), retweets = sum(retweets))  %>%
        dplyr::ungroup()  %>%
        dplyr::group_by(topic, created_date, tweet_geo_country_code)  %>%
        dplyr::top_n(n = 200, wt = frequency) %>%
        dplyr::ungroup() 
    }
  } else if(serie_name == "country_counts") {
    # Getting the expression for known users
    known_user <- 
      paste("screen_name in ('"
        , paste(get_known_users(), collapse="','")
	      , "') or linked_screen_name in ('"
        , paste(get_known_users(), collapse="','")
	      , "') "
	      , sep = ""
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
         paste("date_format(created_at, 'yyyy-MM-dd') = '", strftime(created_date, "%Y-%m-%d"), "'", sep = "")
      )
      , params = replacementfile
    )
  }
}
