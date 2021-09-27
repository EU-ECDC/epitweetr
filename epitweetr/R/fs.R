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

get_scala_recalc_hash_url <- function() {
  paste(get_scala_api_endpoint(), "recalculate-hash", sep = "")
}

#' @export 
fs_loop <-  function(data_dir = NA) {
  # Setting or reusing the data directory
  if(is.na(data_dir) )
    setup_config_if_not_already()
  else
    setup_config(data_dir = data_dir)
  
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
      message(paste("The epitweetr scala API was stopped with the following error", e, "launching it again within 5 seconds", sep = "\n"))
      Sys.sleep(5)
    })
  }
}

#' @export
search_tweets <- function(query = NULL, topic = NULL, from = NULL, to = NULL, max = 100) {
  u <- paste(get_scala_tweets_url(), "?jsonnl=true", sep = "")
  if(!is.null(query)) {
    u <- paste(u, "&q=", URLencode(query, reserved=T), sep = "") 
  }
  if(!is.null(topic)) {
    u <- paste(u, "&topic=", URLencode(topic, reserved=T), sep = "") 
  }
  if(!is.null(from)) {
    u <- paste(u, "&from=",strftime(from, format="%Y-%m-%d") , sep = "") 
  }
  if(!is.null(to)) {
    u <- paste(u, "&to=",strftime(to, format="%Y-%m-%d") , sep = "") 
  }
  if(!is.null(max)) {
    u <- paste(u, "&max=", max , sep = "") 
  }
  u <- url(u)
  tweets <- jsonlite::stream_in(u, verbose = FALSE)
  tweets
}



# Gets a dataframe with geolocated tweets requested varables stored on json files of search api and json file from geolocated tweets produced by geotag_tweets
# this funtion is the entry point from gettin tweet information produced by SPARK
# it is generic enough to choose variables, aggregation and filters
# deals with tweet deduplication at topic level
# regexp: regexp to limit the geolocation and search files to read
# vars: variable to read (evaluated after aggregation if required)
# sort_by: order to apply to the returned dataframe
# filter_by: expressions to use for filtering tweets
# sources_exp: variables to limit the source files to read (setting this will improve reading performance)
# handler: function that to perform a custom R based transformation on data returned by SPARK
# perams: definition of custom param files to enable big queries
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
    migration_log(httr::content(post_result, "text", encoding = "UTF-8"))
    print(substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100))
    stop()
  }
}



stream_post <- function(uri, body, handler = NULL) { 
  cum <- new.env()
  cum$tail <- c()
  cum$dfs <- list()

  
  if(!is.null(handler)) {
    # If a custom transfotmation is going to be done (a handler function has been set)
    # this function will be calles on pages of 10k lines using the jsonlite stream_in function
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
	      cum$dfs[[length(cum$dfs) + 1]] <- jsonlite::stream_in(con, verbose = F)
      }
	    else
  	    jsonlite::stream_in(con, function(df) handler(df, con_tmp), verbose = F)
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
    message("HOLA§!!!!")
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
	    cum$dfs[[length(cum$dfs) + 1]] <- jsonlite::stream_in(con, verbose = F)
    }
	  else
  	  cum$dfs[[length(cum$dfs) + 1]] <- jsonlite::stream_in(con, function(df) handler(df, con_tmp), verbose = F)
	  close(con)
	}
  #Transforming single lines responses in dataframes
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

