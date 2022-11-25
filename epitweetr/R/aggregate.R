#Environment for storing cached data
cached <- new.env()


#' @title Getting already aggregated time series produced by \code{\link{detect_loop}}
#' @description Read and returns the required aggregated dataset for the selected period and topics defined by the filter.
#' @param dataset A character(1) vector with the name of the series to request, it must be one of 'country_counts', 'geolocated', 'topwords', 'hashtags', 'entities', 'urls', 'contexts', default: 'country_counts'
#' @param cache Whether to use the cache for lookup and storing the returned dataframe, default: TRUE
#' @param filter A named list defining the filter to apply on the requested series, it should be on the shape of a named list e.g. list(tweet_geo_country_code=list('FR', 'DE')) default: list()
#' @param top_field Name of the top field used with top_frequency to enable optimisation for getting only most frequent elements. It will only keep top 500 items after first 50k lines on reverse index order
#' @param top_freq character, Name of the frequency fields used with top_field to enable optimisation for getting only most frequent elements. 
#' It will only keep top 500 items after first 50k rows on reverse index order
#' @return A data frame containing the requested series for the requested period
#' @details This function returns data aggregated by epitweetr. The data is found on the 'series' folder, which contains Rds files per weekday and type of series. 
#' starting on v 1.0.x it will also look on Lucene indexes situated on fs folder. Names of files and folders are parsed to limit the files to be read.
#' When using Lucene indexes, filters are directly applied on read. This is an improvement compared 'series' folder where filters are applied 
#' after read. All returned rows are joined in a single dataframe.
#' If no filter is provided all data series is returned, which can end up with millions of rows depending on the time series. 
#' To limit by period, the filter list must have an element 'period' containing a date vector or list with two dates representing the start and end of the request.
#'
#' To limit by topic, the filter list must have an element 'topic' containing a non-empty character vector or list with the names of the topics to return.
#' 
#' The available time series are: 
#' \itemize{
#'   \item{"country_counts" counting tweets and retweets by posted date, hour and country}
#'   
#'   \item{"geolocated" counting tweets and retweets by posted date and the smallest possible geolocated unit (city, administrative level or country)}
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
#'  \code{\link{fs_loop}}
#' @rdname get_aggregates
#' @export 
#' @importFrom magrittr `%>%`
#' @importFrom dplyr filter
#' @importFrom jsonlite rbind_pages
#' @importFrom utils tail
#' @importFrom utils URLencode
get_aggregates <- function(dataset = "country_counts", cache = TRUE, filter = list(), top_field = NULL, top_freq = NULL) {
  `%>%` <- magrittr::`%>%`
  # getting the name for cache lookup dataset dependency
  last_filter_name <- paste("last_filter", dataset, sep = "_")

  # Setting the last aggregate filter
  last_agg <- get_aggregated_period()
  last_agg <- paste(last_agg$last,last_agg$last_hour)
  filter$last_aggregate <- last_agg


  # checking whether we can reuse the cache
  reuse_filter <- (
    cache &&
    exists(dataset, where = cached ) &&
    exists(last_filter_name, where = cached) && #cache entry exists for that dataset
    (exists("last_aggregate", where = cached[[last_filter_name]]) && cached[[last_filter_name]]$last_aggregate == filter$last_aggregate) && # No new aggregation has been finished
    (length(setdiff(names(cached[[last_filter_name]]), names(filter))) == 0) && # All filters in cache are alse present in the query 
    (!exists("period", cached[[last_filter_name]]) || # all periods are cached or
      (
        exists("period", cached[[last_filter_name]]) &&  # there are some period cached
        exists("period", filter) && # there is a period on the filter
        cached[[last_filter_name]]$period[[1]] <= filter$period[[1]] && # and filtered period is contained on cached period
        cached[[last_filter_name]]$period[[2]] >= filter$period[[2]]
      )
    )
  )
  for(field in names(filter)) {
    if(!(field %in% c("last_aggregate", "period"))) {
      reuse_filter <- reuse_filter && 
        (!exists(field, cached[[last_filter_name]]) || # all values are cached or 
          (
            exists(field, cached[[last_filter_name]]) && # there are some values cached
            all(filter[[field]] %in% cached[[last_filter_name]][[field]]) # all filtered values are cached
          )
        )

    }
  }
  if(reuse_filter) {
    # On cache hit returning from cache
    ret <- (cached[[dataset]] %>% 
      dplyr::filter(
        (if(exists("period", where = filter)) .data$created_date >= filter$period[[1]] & .data$created_date <= filter$period[[2]] else TRUE)
      )
    )
    for(field in names(filter)) {
      if(exists(field, where = filter) && !(field %in% c("last_aggregate", "period"))) {
        filt = filter[[field]]
        ret <- ret %>% 
          dplyr::filter(!!as.symbol(field) %in% filt)
      }
    }
    ret
  }
  else {
    # Overriding current filter when no cache hit
    cached[[last_filter_name]] <- filter
    # getting the aggregated data frame from the storage system
    q_url <- paste0(get_scala_aggregate_url(), "?jsonnl=true&serie=", URLencode(dataset, reserved = T))
    if(!is.null(top_field) && !is.null(top_freq))
      q_url <- paste0(q_url, "&topField=", top_field, "&topFrequency=", top_freq) 
    for(field in names(filter)) {
      if(field == "topic") q_url <- paste0(q_url, "&topic=", URLencode(paste0(filter$topic, collapse=";"), reserved = T)) 
      else if(field == "period") 
        q_url <- paste0(
          q_url, 
          "&from=", 
          URLencode(strftime(filter$period[[1]], format = "%Y-%m-%d"), reserved = T), 
          "&to=", 
          URLencode(strftime(filter$period[[2]], format = "%Y-%m-%d"), reserved = T) 
        ) 
      else if(field != "last_aggregate")
        q_url <- paste0(q_url, "&filter=", URLencode(field, reserved = T), ":", URLencode(paste0(filter[[field]], collapse=";"), reserved = T))
    }

    #measure_time <- function(f) {start.time <- Sys.time();ret <- f();end.time <- Sys.time();time.taken <- end.time - start.time;message(time.taken); ret}
    #message(q_url)
    agg_df = (
      if(conf$onthefly_api)
        jsonlite::stream_in(url(q_url), verbose = FALSE)
      else {
        if(!file.exists( file.path(conf$data_dir, "tmp")))
          dir.create(file.path(conf$data_dir, "tmp"))
        dest = file.path(conf$data_dir, "tmp", paste0("aggregate_", as.integer(stats::runif(1, 1, 99999)), ".json"))
        download.file(url = q_url, destfile = dest, quiet = TRUE)
        on.exit(if(file.exists(dest)) file.remove(dest))
        df <- jsonlite::stream_in(file(dest), verbose = FALSE)
        df
      }
    )
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
    #add possible missing columns removed by json null management
    agg_df <- add_missing(agg_df, dataset)
    
    ret <- rbind(agg_df, get_aggregates_rds(dataset, cache = cache, filter = filter))
    cached[[dataset]] <- ret
    ret
  }
}

get_aggregates_rds <- function(dataset = "country_counts", cache = TRUE, filter = list()) {
  # No cache hit getting from aggregated files
  # starting by listing all series files
  `%>%` <- magrittr::`%>%`
  # getting the name for cache lookup dataset dependancy
  files <- list.files(path = file.path(conf$data_dir, "series"), recursive=TRUE, pattern = paste(dataset, ".*\\.Rds", sep=""))
  if(length(files) == 0) {
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
    
    #Joining data extracts if any or otherwise returning empty dataset
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

# This function registers the aggregated series that are computed by epitweetr.
# Each series is defined by a name a date column, primary keys columns, variables columns, group by columns and sources expressions
# Each registered series uses the set_aggregated_tweets function
# This function is periodically called bt the search loop.
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
  # Getting top words aggregation
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
          , "explode(split(text,'[^a-zA-Z\\'\\\\p{L}]')) as token"
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
  # Getting entities aggregation
  set_aggregated_tweets(
    name = "entities"
    , dateCol = "created_date"
    , pks = list("created_date", "topic", "tweet_geo_country_code", "entity")
    , aggr = list(frequency = "sum", original = "sum", retweets = "sum") 
    , sources_exp = c(
        list(
          "topic"
          , "date_format(created_at, 'yyyy-MM-dd') as created_date"
          , "is_retweet"
          , "explode(entities) as entity"
         )
        , get_tweet_location_columns("geo") 
      )
    , vars = list(
      "topic"
      ,"entity"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
      , "count(1) as frequency"
      , "sum(case when is_retweet then 0 else 1 end) as original"
      , "sum(case when is_retweet then 1 else 0 end) as retweets"
    )
    , group_by = list(
      "topic"
      ,"entity"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
    )
  )
 
  # Getting URLs aggregation
  set_aggregated_tweets(
    name = "urls"
    , dateCol = "created_date"
    , pks = list("created_date", "topic", "tweet_geo_country_code", "url")
    , aggr = list(frequency = "sum", original = "sum", retweets = "sum") 
    , sources_exp = c(
        list(
          "topic"
          , "date_format(created_at, 'yyyy-MM-dd') as created_date"
          , "is_retweet"
          , "explode(urls) as url"
         )
        , get_tweet_location_columns("geo") 
      )
    , vars = list(
      "topic"
      ,"url"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
      , "count(1) as frequency"
      , "sum(case when is_retweet then 0 else 1 end) as original"
      , "sum(case when is_retweet then 1 else 0 end) as retweets"
    )
    , group_by = list(
      "topic"
      ,"url"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
    )
  )
  # Getting context aggregation
  set_aggregated_tweets(
    name = "contexts"
    , dateCol = "created_date"
    , pks = list("created_date", "topic", "tweet_geo_country_code", "context")
    , aggr = list(frequency = "sum", original = "sum", retweets = "sum") 
    , sources_exp = c(
        list(
          "topic"
          , "date_format(created_at, 'yyyy-MM-dd') as created_date"
          , "is_retweet"
          , "explode(contexts) as context"
         )
        , get_tweet_location_columns("geo") 
      )
    , vars = list(
      "topic"
      ,"context"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
      , "count(1) as frequency"
      , "sum(case when is_retweet then 0 else 1 end) as original"
      , "sum(case when is_retweet then 1 else 0 end) as retweets"
    )
    , group_by = list(
      "topic"
      ,"context"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
    )
  )
  # Getting hashtag aggregation
  set_aggregated_tweets(
    name = "hashtags"
    , dateCol = "created_date"
    , pks = list("created_date", "topic", "tweet_geo_country_code", "hashtag")
    , aggr = list(frequency = "sum", original = "sum", retweets = "sum") 
    , sources_exp = c(
        list(
          "topic"
          , "date_format(created_at, 'yyyy-MM-dd') as created_date"
          , "is_retweet"
          , "explode(hashtags) as hashtag"
         )
        , get_tweet_location_columns("geo") 
      )
    , vars = list(
      "topic"
      ,"hashtag"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
      , "count(1) as frequency"
      , "sum(case when is_retweet then 0 else 1 end) as original"
      , "sum(case when is_retweet then 1 else 0 end) as retweets"
    )
    , group_by = list(
      "topic"
      ,"hashtag"
      , "created_date" 
      , paste(get_tweet_location_var("geo_country_code"), "as tweet_geo_country_code") 
    )
  )
}

# Getting last aggregation date or NA if first
# Date is obtained by sorting and reading first and last file on the series folder
# This is used for data collected with epitweetr v0.x.x
get_aggregated_period_rds <- function() {
  # listing all aggregated files for given dataset 
  agg_files <- list.files(file.path(conf$data_dir, "series"), recursive=TRUE, full.names =TRUE)
  agg_files <- agg_files[grepl(paste(".*", "country_counts", ".*\\.Rds", sep = ""), agg_files)] 
  # sorting them alphabetically. This makes them sorted by date too because of naming convention
  agg_files <- sort(agg_files)
  if(length(agg_files) > 0) { 
   # getting date information from first and last aggregated file
   first_file <- agg_files[[1]]
   first_df <- readRDS(first_file)
   last_file <- agg_files[[length(agg_files)]]
   last_df <- readRDS(last_file)
   last_hour <- max(strptime(paste(strftime(last_df$created_date, format = "%Y-%m-%d"), " ", last_df$created_hour, ":00:00", sep=""), format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))
   list(
     first = min(first_df$created_date), 
     last = as.Date(last_hour),
     last_hour= as.integer(strftime(last_hour, format = "%H"))
   )   
  } else 
   list(first = NA, last = NA, last_hour = NA)
}

# getting last aggregation date or NA if first
# date is obtained by sorting and reading first and last file on the series folder and in the fs folder containing Lucene indexes

get_aggregated_period <- function() {
  if(!exists("last_agg_request", cached) || as.numeric(Sys.time() - cached$last_agg_request, units="secs") > 60) {
    cached$last_agg_request_value <- {
      rds_period <- get_aggregated_period_rds()
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
         ret$last_hour <-  if(exists("last_hour", where = ret)) {
             as.integer(ret$last_hour)
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
          first = min(c(as.Date(rds_period$first), as.Date(fs_period$first)), na.rm = T), 
          last = max(c(as.Date(rds_period$last), as.Date(fs_period$last)), na.rm = T),
          last_hour = (
            if(is.na(fs_period$last))
              rds_period$last_hour
            else 
              fs_period$last_hour
          )
        )
    }
    cached$last_agg_request <- Sys.time()
    if(is.na(cached$last_agg_request_value$last))
      rm("last_agg_request", envir = cached)
  }
  cached$last_agg_request_value
}

# Utility function to ask epitweetr to recalculate hashes of stored tweets in Lucene indexes. 
# This function is experimental for testing parallel scan of indexes
# This function is deprecated since no significant performance gain was observed
recalculate_hash <- function() {
  message("recalculating hashes")
  post_result <- httr::POST(url=get_scala_recalc_hash_url(), httr::content_type_json(), body="", encode = "raw", encoding = "UTF-8")
  if(httr::status_code(post_result) != 200) {
    stop(paste("recalc hash web service failed with the following output: ", substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100), sep  = "\n"))
  } else {
    message("hashes recalculated")
  }

}

# Adds possible missing columns on a dataset produced by an aggregated series
# This is necessary to ensure that all expected columns are present for data produced
# in old epitweetr versions
add_missing <- function(df, dataset) {
  cols <- colnames(df)
  defaults <- ( 
    if(dataset == "geolocated") {
      list(
        topic = "char",
        created_date = "date",
        user_geo_country_code = "char",
        tweet_geo_country_code = "char",
        user_geo_code = "char",
        tweet_geo_code = "char",
        tweet_longitude = "num",
        tweet_latitude = "num",
        user_longitude = "num",
        user_latitude = "num",
        retweets = "int",
        tweets= "int",
        created_weeknum = "int" 
      )
    } else if(dataset == "country_counts") {
      list(
        topic  = "char",
        created_date = "date",
        created_hour = "char",
        tweet_geo_country_code = "char",
        user_geo_country_code  = "char",
        retweets = "int",
        tweets = "int",
        known_retweets = "int",
        known_original = "int",
        created_weeknum = "int"
      )
    } else if(dataset == "topwords") {
      list(
        token = "char",
        topic = "char",
        created_date = "date",
        tweet_geo_country_code = "char",
        frequency = "int",
        original = "int",
        retweets = "int",
        created_weeknum = "int"
      )
    } else if(dataset == "hashtags") {
      list(
        hashtag = "char",
        topic = "char",
        created_date = "date",
        tweet_geo_country_code = "char",
        frequency = "int",
        original = "int",
        retweets = "int",
        created_weeknum = "int"
      )
    } else if(dataset == "urls") {
      list(
        url = "char",
        topic = "char",
        created_date = "date",
        tweet_geo_country_code = "char",
        frequency = "int",
        original = "int",
        retweets = "int",
        created_weeknum = "int"
      )
    } else if(dataset == "entities") {
      list(
        entity = "char",
        topic = "char",
        created_date = "date",
        tweet_geo_country_code = "char",
        frequency = "int",
        original = "int",
        retweets = "int",
        created_weeknum = "int"
      )
    } else if(dataset == "contexts") {
      list(
        context = "char",
        topic = "char",
        created_date = "date",
        tweet_geo_country_code = "char",
        frequency = "int",
        original = "int",
        retweets = "int",
        created_weeknum = "int"
      )
    }
  )
  for(attr in names(defaults)) {
    type <- defaults[attr]
    if(!(attr %in% cols)) {
      if(nrow(df) == 0 && type == "char") df[attr] <- character()
      else if(nrow(df) == 0 && type == "date") df[attr] <- as.Date(character())
      else if(nrow(df) == 0 && type == "int") df[attr] <- integer()
      else if(nrow(df) == 0 && type == "num") df[attr] <- numeric()
      else if(nrow(df) > 0 && type == "char") df[attr] <- as.character(NA)
      else if(nrow(df) > 0 && type == "date") df[attr] <- as.Date(NA)
      else if(nrow(df) > 0 && type == "int") df[attr] <- as.integer(NA)
      else if(nrow(df) > 0 && type == "num") df[attr] <- as.numeric(NA)
      else stop(paste("unexpected default case with type", type, "for", attr, "with length", nrow(df)))
    }
  }
  return(df)
} 
