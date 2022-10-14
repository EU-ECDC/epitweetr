# This script includes all dedicated logic for interacting with the Spark/Scala API via HTTP REST requests

get_scala_api_endpoint <- function() {
  paste("http://localhost:", conf$fs_port, "/", sep = "")
}

get_scala_geotraining_url <- function() {
  paste(get_scala_api_endpoint(), "geotraining-set", sep ="")
}

get_scala_alert_training_url <- function() {
  paste(get_scala_api_endpoint(), "evaluate-alerts", sep ="")
}

get_scala_tweets_url <- function() {
  paste(get_scala_api_endpoint(), "tweets", sep ="")
}

get_scala_geolocated_tweets_url <- function() {
  paste(get_scala_api_endpoint(), "geolocated-tweets", sep = "")
}

get_scala_commit_url <- function() {
  paste(get_scala_api_endpoint(), "commit", sep = "")
}


get_scala_geolocate_text_url <- function() {
  paste(get_scala_api_endpoint(), "geolocate-text", sep = "")
}

get_scala_aggregate_url <- function() {
  paste(get_scala_api_endpoint(), "aggregate", sep = "")
}

get_scala_period_url <- function() {
  paste(get_scala_api_endpoint(), "period", sep = "")
}

get_scala_status_url <- function() {
  paste(get_scala_api_endpoint(), "period?serie=country_counts", sep = "")
}

get_scala_ping_url <- function() {
  paste(get_scala_api_endpoint(), "ping", sep = "")
}

get_scala_recalc_hash_url <- function() {
  paste(get_scala_api_endpoint(), "recalculate-hash", sep = "")
}


#' @title Runs the epitweetr embedded database loop
#' @description Infinite loop ensuring that the epitweetr embedded database is running (Lucene + akka-http)
#' @param data_dir Path to the 'data directory' containing application settings, models and collected tweets.
#' If not provided, the system will try to reuse the existing one from last session call of \code{\link{setup_config}} or use the EPI_HOME environment variable, default: NA
#' @return nothing
#' @details Launches the epitweetr embedded database which is accessed via a REST API located on \url{http://localhost:8080}. You can test that the database is running by accessing \url{http://localhost:8080/ping}
#' the REST API provide epitweetr a way to send and retrieve data related with tweets and time series and to trigger geolocation or aggregation
#' The database is implemented using Apache Lucene indexes allowing epitweetr to access its data as a search engine but also as a tabular database.
#' \code{\link{health_check}} called each 60 seconds on a background process to send alerts to the administrator if some epitweetr components fail.
#' @examples 
#' if(FALSE){
#'    #Running the detect loop
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    fs_loop()
#' }
#' @rdname fs_loop
#' @seealso
#'  \code{\link{detect_loop}}
#'  
#'  \code{\link{search_loop}}
#'
#'  \code{\link{health_check}}
#' @importFrom future multisession plan future
#' @importFrom rlang current_env
#' @export 
fs_loop <-  function(data_dir = NA) {
  if(is.na(data_dir) )
    setup_config_if_not_already()
  else
    setup_config(data_dir = data_dir)
  data_dir <- conf$data_dir

  # calculating alerts per topic
  future::plan(future::multisession)
  monitor <- future::future({
       message("Monitoring epitweetr") 
       setup_config(data_dir = data_dir)
       x = 60
       step = 2
       while(TRUE) {
         register_fs_monitor()
         if(x == 0) {
           health_check()
           x = 60
         }
         x = x - step
         Sys.sleep(step)
       }
       0
   }
   , globals = c(
        "data_dir", 
        "setup_config",
        "health_check",
        "register_fs_monitor"
      )
   ,envir=rlang::current_env()
  )
  
  message("Launching fs")
  # Registering the fs runner using current PID and ensuring no other instance of the search is actually running.
  register_fs_runner()
  
  # Infinite loop calling the fs runner
  while(TRUE) {
    tryCatch({
      spark_job(
        paste(
	        "fsService"
          , "epiHome" , conf$data_dir
        )
      )
    }, error = function(e) {
      message(paste("The epitweetr Scala API was stopped with the following error", e, "launching it again within 5 seconds", sep = "\n"))
      Sys.sleep(5)
    })
  }
}

#' @title perform full text search on tweets collected with epitweetr
#' @description  perform full text search on tweets collected with epitweetr (tweets migrated from epitweetr v<1.0.x are also included)
#' @param query character. The query to be used if a text it will match the tweet text. To see how to match particular fields please see details, default:NULL
#' @param topic character, Vector of topics to include on the search default:NULL
#' @param from an object which can be converted to ‘"POSIXlt"’ only tweets posted after or on this date will be included, default:NULL
#' @param to an object which can be converted to ‘"POSIXlt"’ only tweets posted before or on this date will be included, default:NULL
#' @param countries character or numeric, the position or name of epitweetr regions to be included on the query, default:NULL
#' @param mentioning character, limit the search to the tweets mentioning the given users, default:NULL
#' @param users character, limit the search to the tweets created by the provided users, default:NULL
#' @param hide_users logical, whether to hide user names on output replacing them by the USER keyword, default:FALSE
#' @param action character, an action to be performed on the search results respecting the max parameter. Possible values are 'delete' or 'anonymise' , default:NULL
#' @param max integer, maximum number of tweets to be included on the search, default:100
#' @param by_relevance logical, whether to sort the results by relevance of the matching query or by indexing order, default:FALSE
#' If not provided the system will try to reuse the existing one from last session call of \code{\link{setup_config}} or use the EPI_HOME environment variable, default: NA
#' @return a data frame containing the tweets matching the selected filters, the data frame contains the following columns: linked_user_location, linked_user_name, linked_user_description, 
#' screen_name, created_date, is_geo_located, user_location_loc, is_retweet, text, text_loc, user_id, hash, user_description, linked_lang, linked_screen_name, user_location, totalCount, 
#' created_at, topic_tweet_id, topic, lang, user_name, linked_text, tweet_id, linked_text_loc, hashtags, user_description_loc
#' @details 
#' epitweetr translate the query provided by all parameters into a single query that will be executed on tweet indexes which are weekly indexes.
#' The q parameter should respect the syntax of the Lucene classic parser \url{https://lucene.apache.org/core/8_5_0/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html} 
#' So other than the provided parameters, multi field queries are supported by using the syntax field_name:value1;value2 
#' AND, OR and -(for excluding terms) are supported on q parameter.
#' Order by week is always applied before relevance so even if you provide by_relevance = TRUE all of the matching tweets of the first week will be returned first 
#' @examples 
#' if(FALSE){
#'    #Running the detect loop
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    df <- search_tweets(
#'         q = "vaccination", 
#'         topic="COVID-19", 
#'         countries=c("Chile", "Australia", "France"), 
#'         from = Sys.Date(), 
#'         to = Sys.Date()
#'    )
#'    df$text
#' }
#' @rdname search_tweets
#' @seealso
#'  \code{\link{search_loop}}
#'  
#'  \code{\link{detect_loop}}
#' @importFrom utils URLencode
#' @importFrom jsonlite stream_in
#' @export 
search_tweets <- function(query = NULL, topic = NULL, from = NULL, to = NULL, countries = NULL, mentioning = NULL, users = NULL, hide_users = FALSE, action = NULL, max = 100, by_relevance = FALSE) {
  u <- paste(get_scala_tweets_url(), "?jsonnl=true&estimatecount=true&by_relevance=", tolower(by_relevance), sep = "")
  if(hide_users) {
    u <- paste(u, "&hide_users=true", sep = "") 
  }
  if(!is.null(action)) {
    u <- paste(u, "&action=", action, sep = "") 
  }
  if(!is.null(query)) {
    u <- paste(u, "&q=", utils::URLencode(query, reserved=T), sep = "") 
  }
  if(!is.null(topic)) {
    u <- paste(u, "&topic=", utils::URLencode(paste(topic, sep ="", collapse = ";"), reserved=T), sep = "", collapse = "") 
  }
  if(!is.null(from)) {
    u <- paste(u, "&from=",strftime(from, format="%Y-%m-%d") , sep = "") 
  }
  if(!is.null(to)) {
    u <- paste(u, "&to=",strftime(to, format="%Y-%m-%d") , sep = "") 
  }
  if(!is.null(countries)  && length(countries) > 0 && (is.character(countries) || any(countries != 1))) {
    # getting all regions
    regions <- get_country_items()
    # If countries are names they have to be changes to region indexes
    if(is.character(countries)) {
      countries = (1:length(regions))[sapply(1:length(regions), function(i) regions[[i]]$name %in% countries)]
    }
    country_codes <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(countries), function(i) unlist(regions[[i]]$codes)))
    u <- paste(u, "&country_code=", paste(country_codes, collapse = ";", sep = ""), sep = "", collapse = "") 
  }
  if(!is.null(mentioning)) {
    u <- paste(u, "&mentioning=",paste(mentioning, sep = "", collapse = ";") , sep = "") 
  }
  if(!is.null(users)) {
    u <- paste(u, "&user=",paste(users, sep = "", collapse = ";") , sep = "") 
  }

  if(!is.null(max)) {
    u <- paste(u, "&max=", max , sep = "") 
  }
  u <- url(u)
  tweets <- jsonlite::stream_in(u, verbose = FALSE)
  tweets
}



# Registers a query to define custom aggregations on tweets
# it is generic enough to choose variables, aggregation and filters
# deals with tweet deduplication at topic level
# regexp: regexp to limit the geolocation and search files to read
# vars: variable to read (evaluated after aggregation if required)
# sort_by: order to apply to the returned data frame
# filter_by: expressions to use for filtering tweets
# sources_exp: variables to limit the source files to read (setting this will improve reading performance)
# handler: function that to perform a custom R based transformation on data returned by SPARK
# params: definition of custom param files to enable big queries
set_aggregated_tweets <- function(name, dateCol, pks, aggr, vars = list("*"), group_by = list(), sort_by = list(), filter_by = list(), sources_exp = list(), params = list()) {
  stop_if_no_config(paste("Cannot get tweets without configuration setup")) 
  post_result <- httr::POST(
    url=get_scala_aggregate_url(), 
    httr::content_type_json(), 
    body= jsonlite::toJSON(
      list(
        name = name,
        dateCol = dateCol,
        pks = pks,
        aggr = aggr,
        aggregation = list(
          columns = vars,  
          groupBy = group_by, 
          sortBy = sort_by, 
          filterBy = filter_by, 
          sourceExpressions = sources_exp,
          params = if(length(params) == 0) list(fake_param_epi = "") else params
        )
      ), 
      simplify_vector = T ,
      auto_unbox = T
    ), 
    encode = "raw", 
    encoding = "UTF-8"
  )
  if(httr::status_code(post_result) != 200) {
    print(substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100))
    stop()
  }
}


# Utility function to interact with a POST endpoint and process data on a streaming way provided by handler
stream_post <- function(uri, body, handler = NULL) { 
  cum <- new.env()
  cum$tail <- c()
  cum$dfs <- list()

  
  if(!is.null(handler)) {
    # If a custom transformation is going to be done (a handler function has been set)
    # this function will be called on pages of 10k lines using the jsonlite stream_in function
    # the transformed results will be stored on a temporary file that will be read by stream_in
    tmp_file <- tempfile(pattern = "epitweetr", fileext = ".json")
    #message(tmp_file)
    con_tmp <- file(tmp_file, open = "w", encoding = "UTF-8") 
  }

  pipeconnection <- function(x, handler = handler) {
    cuts <- which(x == 0x0a)
	  if(length(cuts) == 0)
	    cum$tail <- c(cum$tail, x) 
	  else {
	    if( tail(cuts, n = 1) != length(x)) {
		    bytes <- c(cum$tail, x[1:tail(cuts, n = 1)])
	      cum$tail <- x[(tail(cuts, n = 1)+1):length(x)]
      } else {
		    bytes <- c(cum$tail, x)
		    cum$tail <- c()
		  }
	    con <- rawConnection(bytes, "rb")
      on.exit(close(con))
      if(is.null(handler)) {
	      cum$dfs[[length(cum$dfs) + 1]] <- jsonlite::stream_in(con, verbose = FALSE)
      }
	    else
  	    jsonlite::stream_in(con, function(df) handler(df, con_tmp), verbose = FALSE)
	  }
  }
  # Doing the post request
  parts = gregexpr("//[^/]+/", uri)
  split <- parts[[1]][[1]] + attr(parts[[1]], "match.length")
  #h <- httr::handle(substr(uri, 1, split-1))
  #path <- substr(uri, split, nchar(uri))
  #on.exit(httr::handle_reset(h))
  
  post_result <- tryCatch({
      httr::POST(
        url = uri , 
        httr::write_stream(function(x) {pipeconnection(x, handler)}), 
        body = body,
        httr::content_type_json(),
        encode = "raw", 
        encoding = "UTF-8"
      )
    }
    ,error = function(e) {
      closeAllConnections()
      stop(e)
    }
  )
  if(httr::status_code(post_result) != 200) {
    message(uri)
    message(httr::status_code(post_result))
    message(httr::content(post_result, "text", encoding = "UTF-8"))
    stop()
  }
	
  #Applying last line if necessary
  if(length(cum$tail)>0) {
    if(cum$tail[[length(cum$tail)]] != 0x0a)
      cum$tail[[length(cum$tail) + 1]] <- as.raw(0x0a)
    con <- rawConnection(cum$tail, "rb")
	  if(is.null(handler)) {
	    cum$dfs[[length(cum$dfs) + 1]] <- jsonlite::stream_in(con, verbose = FALSE)
    }
	  else
  	  cum$dfs[[length(cum$dfs) + 1]] <- jsonlite::stream_in(con, function(df) handler(df, con_tmp), verbose = F)
	  close(con)
	}
  #Transforming single lines responses in data frames
	#lapply(cum$dfs, function(l) if(typeof(l) == "list") data.frame(lapply(l, function(x) t(data.frame(x)))) else l)
  #joining response
  if(is.null(handler)) {
    jsonlite::rbind_pages(cum$dfs)
  } else {
    close(con_tmp)
    con_tmp <- file(tmp_file, open = "r", encoding = "UTF-8") 
    ret <- jsonlite::stream_in(con_tmp, pagesize = 10000, verbose = FALSE)
    close(con_tmp)
    unlink(tmp_file)
    ret
  }
}

# Get time difference since last request
# This function is used from the Shiny app to report when was the last time that a request saved tweets
# This is done by taking the last modified date of current year tweet files
last_fs_updates <- function(collections = c("tweets", "topwords", "country_counts", "geolocated")) {
  times <- lapply(collections,
    function(collection) {
      folders <- sort(list.files(path=paste(conf$data_dir, "fs", collection, sep="/"), full.names=T))
      if(length(folders)>0) {
        files <- list.files(tail(folders, 2), full.names = TRUE, recursive = TRUE)
        files <- files[!grepl("write.lock$", files)]
        max(file.mtime(files))
      } else {
        NA
      }
    }
  )
  setNames(times, collections)
}
