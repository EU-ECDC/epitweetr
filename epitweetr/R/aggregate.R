#Environment for storing cached data
cached <- new.env()


#' @title Getting already aggregated time series produced by \code{\link{detect_loop}}
#' @description Read and returns the required aggregated dataset for the selected period and topics defined by the filter.
#' @param dataset A character(1) vector with the name of the series to request, it must be one of 'country_counts', 'geolocated' or 'topwords', default: 'country_counts'
#' @param cache Whether to use the cache for lookup and storing the returned dataframe, default: TRUE
#' @param filter A named list defining the filter to apply on the requested series, default: list()
#' @return A dataframe containing the requested series for the requested period
#' @details This function will look in the 'series' folder, which contains Rds files per weekday and type of series. It will parse the names of file and folders to limit the files to be read.
#' Then it will apply the filters on each dataset for finally joining the matching results in a single dataframe.
#' If no filter is provided all data series are returned, which can end up with millions of rows depending on the time series. 
#' To limit by period, the filter list must have an element 'period' containing a date vector or list with two dates representing the start and end of the request.
#'
#' To limit by topic, the filter list must have an element 'topic' containing a non-empty character vector or list with the names of the topics to return.
#' 
#' The available time series are: 
#' \itemize{
#'   \item{"country_counts" counting tweets and retweets by posted date, hour and country}
#'   
#'   \item{"geolocated" counting tweets and retweets by posted date and the smallest possible geolocated unit (city, adminitrative level or country)}
#'   
#'   \item{"topwords" counting tweets and retweets by posted date, country and the most popular words, (this excludes words used in the topic search)}
#' }
#' The returned dataset can be cached for further calls if requested. Only one dataset per series is cached.
#' @examples 
#' if(FALSE){
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    # Getting all country tweets between 2020-jan-10 and 2020-jan-31 for all topics
#'    df <- get_aggregates(
#'      dataset = "country_counts", 
#'      filter = list(period = c("2020-01-10", "2020-01-31"))
#'    )
#'
#'    # Getting all country tweets for the topic dengue
#'    df <- get_aggregates(dataset = "country_counts", filter = list(topic = "dengue"))
#'
#'    # Getting all country tweets between 2020-jan-10 and 2020-jan-31 for the topic dengue
#'     df <- get_aggregates(
#'         dataset = "country_counts",
#'          filter = list(topic = "dengue", period = c("2020-01-10", "2020-01-31"))
#'     )
#' }
#' @seealso 
#'  \code{\link{detect_loop}}
#'  \code{\link{geotag_tweets}}
#' @rdname get_aggregates
#' @export 
#' @importFrom magrittr `%>%`
#' @importFrom dplyr filter
#' @importFrom jsonlite rbind_pages
#' @importFrom utils tail
get_aggregates <- function(dataset = "country_counts", cache = TRUE, filter = list()) {
  `%>%` <- magrittr::`%>%`
  # getting the name for cache lookup dataset dependant
  last_filter_name <- paste("last_filter", dataset, sep = "_")

  # Setting a time out of 1 hour
  if(is.null(cached[[last_filter_name]]$last_aggregate) ||  as.numeric(Sys.time() - cached[[last_filter_name]]$last_aggregate, unit = "secs") > 3600) { 
    filter$last_aggregate <- Sys.time()
  } else 
    filter$last_aggregate <- cached[[last_filter_name]]$last_aggregate 


  # checking wether we can reuse the cache
  reuse_filter <- 
    exists(last_filter_name, where = cached) && #cache entry exists for that dataset
    (exists("last_aggregate", where = cached[[last_filter_name]]) && cached[[last_filter_name]]$last_aggregate == filter$last_aggregate) && # No new aggregation has been finished
    (!exists("topic", cached[[last_filter_name]]) || # all topics are cached or 
      (
        exists("topic", cached[[last_filter_name]]) && # there are some topic cached
        exists("topic", filter) &&  # there is a topic on the current applied filter
        all(filter$topic %in% cached[[last_filter_name]]$topic) # all filtered topic is cached
      )
    ) && # AND
    (!exists("period", cached[[last_filter_name]]) || # all periods are cached or
      (
        exists("period", cached[[last_filter_name]]) &&  # there are some period cached
        exists("period", filter) && # there is a period on the filter
        cached[[last_filter_name]]$period[[1]] <= filter$period[[1]] && # and filetered period is contained on cached period
        cached[[last_filter_name]]$period[[2]] >= filter$period[[2]]
      )
    )

  # Overriding current filter when no cache hit
  if(!reuse_filter) cached[[last_filter_name]] <- filter
  # On cache hit returning from cache
  if(cache && exists(dataset, where = cached) && reuse_filter) {
    return (cached[[dataset]] %>% 
      dplyr::filter(
        (if(exists("topic", where = filter)) .data$topic %in% filter$topic else TRUE) & 
        (if(exists("period", where = filter)) .data$created_date >= filter$period[[1]] & .data$created_date <= filter$period[[2]] else TRUE)
      )
    )
  }
  else {
    # getting the aggregated dataframe from the storage system
    q_url <- paste0(get_scala_aggregate_url(), "?jsonnl=true&serie=", URLencode(dataset, reserved = T))
    for(field in names(filter)) {
      if(field == "topic") q_url <- paste0(q_url, "&topic=", URLencode(filter$topic, reserved = T)) 
      else if(field == "period") 
        q_url <- paste0(
          q_url, 
          "&from=", 
          URLencode(strftime(filter$period[[1]], format = "%Y-%m-%d"), reserved = T), 
          "&to=", 
          URLencode(strftime(filter$period[[2]], format = "%Y-%m-%d"), reserved = T) 
        ) 
      else if(field != "last_aggregate")
        q_url <- paste0(q_url, "&filter=", URLencode(field, reserved = T), ":", URLencode(filter[[field]], reserved = T))
    }

    #measure_time <- function(f) {start.time <- Sys.time();ret <- f();end.time <- Sys.time();time.taken <- end.time - start.time;message(time.taken); ret}
    message(q_url)
    agg_df = jsonlite::stream_in(url(q_url))
    # Calculating the created week
    if(nrow(agg_df) > 0) {
      agg_df$created_week <- strftime(as.Date(agg_df$created_date, format = "%Y-%m-%d"), format = "%G.%V")
      agg_df$created_weeknum <- as.integer(strftime(as.Date(agg_df$created_date, format = "%Y-%m-%d"), format = "%G%V"))
      agg_df$created_date <- as.Date(agg_df$created_date, format = "%Y-%m-%d")
    } else {
      agg_df$created_week <- NULL 
      agg_df$created_weeknum <- NULL 
      agg_df$created_date <- NULL
    }
    ret <- rbind(get_aggregates_rds(dataset, cache = cache, filter = filter), agg_df)
    if(cache) {
      cached[[dataset]] <- ret
    }
    ret
  }
}

get_aggregates_rds <- function(dataset = "country_counts", cache = TRUE, filter = list()) {
  # No cache hit getting from aggregated files
  # starting by listing all series files
  `%>%` <- magrittr::`%>%`
  # getting the name for cache lookup dataset dependant
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

    # Extracting data from aggregated files
    dfs <- lapply(files, function (file) {
      message(paste("reading", file))
      readRDS(file.path(conf$data_dir, "series", file)) %>% 
        dplyr::mutate(topic = stringr::str_replace_all(.data$topic, "%20", " ")) %>% #putting back espaces from %20 to " "
        dplyr::filter(
          (if(exists("topic", where = filter)) .data$topic %in% filter$topic else TRUE) & 
          (if(exists("period", where = filter)) .data$created_date >= filter$period[[1]] & .data$created_date <= filter$period[[2]] else TRUE)
        )
    })
    
    #Joining data extracts if any or returning empty dataset otherwise
    ret <- 
      if(length(files) > 0)
        jsonlite::rbind_pages(dfs)
      else 
        readRDS(tail(list.files(file.path(conf$data_dir, "series"), full.names=TRUE, recursive=TRUE, pattern="*.Rds"), 1)) %>% dplyr::filter(1 == 0)
    if(dataset == "topwords" && "tokens" %in% colnames(ret)) {
      ret <- ret %>% dplyr::rename("token" = .data$tokens) 
    }
    return(ret)
  }

} 

register_series <- function() {
  `%>%` <- magrittr::`%>%`
  #geolocated"
  set_aggregated_tweets(
    name = "geolocated"
    , dateCol = "created_date"
    , pks = list("created_date", "topic", "user_geo_country_code", "tweet_geo_country_code", "user_geo_code", "tweet_geo_code", "user_geo_name", "tweet_geo_name")
    , aggr = list(tweet_longitude = "avg", tweet_latitude = "avg", user_longitude = "avg", user_latitude = "avg", retweets = "sum", tweets = "sum") 
    , sources_exp = c(
        "topic"
         , list("date_format(created_at, 'yyyy-MM-dd') as created_date", "is_retweet")
         , get_user_location_columns("tweet")
         , get_tweet_location_columns("geo") 
         , get_user_location_columns("geo") 
    )
    , vars = list(
        paste("avg(", get_tweet_location_var("longitude"), ") as tweet_longitude") 
        , paste("avg(", get_tweet_location_var("latitude"), ") as tweet_latitude")
        , paste("avg(", get_user_location_var("longitude"), ") as user_longitude") 
        , paste("avg(", get_user_location_var("latitude"), ") as user_latitude")
        , "cast(sum(case when is_retweet then 1 else 0 end) as Integer) as retweets"
        , "cast(sum(case when is_retweet then 0 else 1 end) as Integer) as tweets"
    )
    , group_by = list(
      "topic"
      , "created_date" 
      , paste(get_user_location_var("geo_country_code"), "as user_geo_country_code") 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
      , paste(get_user_location_var("geo_code"), "as user_geo_code")
      , paste(get_tweet_location_var("geo_code"), "as tweet_geo_code")
      , paste(get_user_location_var("geo_name"), "as user_geo_name")
      , paste(get_tweet_location_var("geo_name"), "as tweet_geo_name")
    )
  )
  #topwords
  # Getting topic words to exclude 
  topic_word_to_exclude <- unlist(sapply(1:length(conf$topics), 
    function(i) {
      terms <- strsplit(conf$topics[[i]]$query, " |OR|\"|AND|,|\\.| |'|-|_")[[1]]
      terms <- terms[terms != ""]
      paste(conf$topics[[i]]$topic, "_", terms, sep = "")
    })) 
  lang_stop_words <- paste("'", unlist(lapply(conf$languages, function(l) lapply(get_stop_words(l$code), function(t) paste(l$code, t, sep = "_")))), "'", sep = "", collapse = ",")
  # Getting top word aggregation
  set_aggregated_tweets(
    name = "topwords"
    , dateCol = "created_date"
    , pks = list("created_date", "topic", "tweet_geo_country_code", "token")
    , aggr = list(frequency = "sum", original = "sum", retweets = "sum") 
    , sources_exp = c(
        list(
          "topic"
          , "date_format(created_at, 'yyyy-MM-dd') as created_date"
          , "is_retweet"
          , "lang"
          , "explode(split(text, '\\\\W')) as token"
         )
        , get_tweet_location_columns("geo") 
      )
    #, sort_by = list(
    #  "topic"
    #  , "tweet_geo_country_code" 
    #  , "created_at" 
    #)
    , filter_by = list(
       "length(token) > 1",
       "lower(token) not in ('via', 'rt', 'http', 'www', 'https', 'co', 't')",
       paste("lower(concat(topic, '_', token)) not in (", paste("'", sapply(topic_word_to_exclude, function(t) tolower(t)), "'", collapse = ",", sep = '') , ")", sep = ""),
       paste("concat(lang, '_', token) not in (", lang_stop_words, ")")
    )  
    , vars = list(
      "topic"
      ,"token"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
      , "count(1) as frequency"
      , "sum(case when is_retweet then 0 else 1 end) as original"
      , "sum(case when is_retweet then 1 else 0 end) as retweets"
    )
    , group_by = list(
      "topic"
      ,"token"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
    )
 )
 #country_counts
 # Getting the expression for known users and writing it as a file so it can be read and applied by spark on query
 known_user <- 
   paste("screen_name in ('"
     , paste(get_known_users(), collapse="','")
     , "') or linked_screen_name in ('"
     , paste(get_known_users(), collapse="','")
     , "') "
     , sep = ""
   )
 params <- list(
   known_retweets = paste("cast(sum(case when is_retweet and ", known_user, "then 1 else 0 end) as Integer) as known_retweets")
   , known_original = paste("cast(sum(case when not is_retweet and ", known_user, "then 1 else 0 end) as Integer) as known_original")
 )
 
 # Aggregation by country level
  set_aggregated_tweets(
    name = "country_counts"
    , dateCol = "created_date"
    , pks = list("created_date", "topic", "created_hour", "tweet_geo_country_code", "user_geo_country_code")
    , aggr = list(retweets = "sum", tweets = "sum", know_retweets = "sum", know_original = "sum") 
    , sources_exp = c(
        list("topic", "created_at", "is_retweet", "screen_name", "linked_screen_name")
        , get_user_location_columns("tweet")
        , get_tweet_location_columns("geo") 
        , get_user_location_columns("geo") 
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
   , params = params
 )
 
}

# getting last aggregation date or NA if first
# date is obtained by sorting and reading first and last file
get_aggregated_period_rds <- function(dataset) {
  # listing all aggregated files for given dataset 
  agg_files <- list.files(file.path(conf$data_dir, "series"), recursive=TRUE, full.names =TRUE)
  agg_files <- agg_files[grepl(paste(".*", dataset, ".*\\.Rds", sep = ""), agg_files)] 
  # sorting them alphabetically. This makes them sorted by date too because of namind convention
  agg_files <- sort(agg_files)
  if(length(agg_files) > 0) { 
   # getting date information from firt and last aggregated file
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

get_aggregated_period <- function(dataset) {
  #TODO: add last hour!!
  rds_period <- get_aggregated_period_rds(dataset)
  fs_period <- tryCatch({
     ret <- jsonlite::fromJSON(url(paste0(get_scala_period_url(),"?serie=country_counts")), simplifyVector = T)
     ret$first <- if(exists("first", where = ret)) {
         as.Date(strptime(ret$first, format = "%Y-%m-%d"))
       } else {
         NA
       }
     ret$last <-  if(exists("last", where = ret)) {
         as.Date(strptime(ret$last, format = "%Y-%m-%d"))
       } else {
         NA
       }
     ret
  }, warning = function(w) {
     list(first= NA, last = NA)
  }, error = function(e) {
     list(first = NA, last = NA)
  })

  if(is.na(rds_period$first) && is.na(fs_period$first))
    list(first = NA, last = NA, last_hour = NA)
  else 
    list(
      first = min(c(rds_period$first, fs_period$first), na.rm = T), 
      last = max(c(rds_period$last, fs_period$last), na.rm = T),
      last_hour = 23
    )
}

recalculate_hash <- function() {
  
  message("recalculating hashes")
  post_result <- httr::POST(url=get_scala_recalc_hash_url(), httr::content_type_json(), body="", encode = "raw", encoding = "UTF-8")
  if(httr::status_code(post_result) != 200) {
    stop(paste("recalc hash web service failed with the following output: ", substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100), sep  = "\n"))
  } else {
    message("hashes recalculated")
  }

}
