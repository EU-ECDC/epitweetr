# Twitter base endpoint for direct API calls
t_endpoint <- list(
  `1.1` = "https://api.twitter.com/1.1/",
  `2`  = "https://api.twitter.com/2/"
)

# Twitter endpoint for getting rate limit status
ratelimit_endpoint <- paste(t_endpoint[["1.1"]], "application/rate_limit_status.json", sep = "")

# Twitter endpoint for searching tweets
search_endpoint <-  list(
  `1.1` = paste(t_endpoint[["1.1"]], "search/tweets.json", sep = ""),
  `2` =  paste(t_endpoint[["2"]], "tweets/search/recent", sep = "")
)

# Get twitter token bases on configuration based on configuration settings (app or user)
# This function is called when saving properties on shiny app and first time epitweetr performs a twitter search in a session
# request_new: Whether to force creating a new token
get_token <- function(request_new = TRUE) {
  token_path <- "~/.rtweet_token.rds"
  new_rtweet <- exists("rtweet_user", base::asNamespace("rtweet"))
  if(exists("access_token", where = conf$twitter_auth )) {
    if(new_rtweet && (conf$twitter_auth$access_token != "" || conf$twitter_auth$bearer == ""))
      stop("It seems you have updated the rtweet package. You need to reset the twitteer authentication using your app bearer token, fallback to user authentication or downgrade rtweet")
    else 
      conf$twitter_auth_mode == "app"
  }
  # An app token is requested
  if(conf$twitter_auth_mode == "app") {  
    # if new rtweet then we just provide the registered bearer
    if(new_rtweet)
      token <- conf$twitter_auth$bearer
    # if not requesting a new one and a non null token is already available on the configuration (because used) then we get it from conf
    else if(!request_new && exists("token", where = conf) && !is.null(conf$token)) 
      token <- conf$token
    # if not requesting new and an available token is written on disk
    else if(!request_new && file.exists(token_path)) {
      token <- readRDS(token_path)
      if(!("bearer" %in% class(conf$token)))
        token <- rtweet::bearer_token(token)
    }
    # In other cases we get a new token using rtweet
    else {
      if(file.exists(token_path)) 
        file.remove(token_path)
      token <- rtweet::create_token(
        app = conf$twitter_auth$app
        , consumer_key =conf$twitter_auth$api_key
        , consumer_secret =conf$twitter_auth$api_secret
        , access_token =conf$twitter_auth$access_token
        , access_secret = conf$twitter_auth$access_token_secret
      )
      # Removing user context
      token <- rtweet::bearer_token(token)
    } 
  } else {
    # Using delegated authentication otherwise, token will be reused if it exists and new one will be requested if it is not set
    token <- (
       if(interactive() && request_new)
         if(new_rtweet) {
           # new version of rtweet detected using new function
           t <- rtweet::rtweet_user()
           saveRDS(t, token_path)
           t
         } else
           rtweet::get_token()
       else if(file.exists(token_path)){
         readRDS(token_path)
       } else if(request_new){
         stop("Cannot get a token on an non interactive session. Create it from configuration page first")
       } else 
         stop("Cannot get a token. Please create it from the configuration page")
     )
  } 
  return(token)
}
last_get <- new.env()
# Execute a get request on twitter API using the provided URL
# It will internally deal with token generation and rate limits
# If request fails with an error or warning it will retry with a quadratic waiting time
# Twitter response codes are interpreted as per twitter documentation to slow down and wait as requested and to respect rate limits
# url: URL to try
# i: Number of tries done already
# This files to file.path(conf$data_dir, "lasterror.log") the last encountered error produced if any
twitter_get <- function(urls, i = 0, retries = 20, tryed = list()) {
  # Getting the right url to try based on version
  url <- if(length(urls) > 1) {
    toTry = order(names(urls)[!names(urls) %in% tryed], decreasing=T)
    if(exists("api_ver", last_get))
      toTry <- toTry[order(sapply(toTry, function(v) if(v == last_get$api_ver) paste0("zzz", v) else v), decreasing=T)]
    toTry <- toTry[[1]]
    last_get$api_ver = toTry
    urls[[toTry]]
  } else {
    last_get$api_ver = names(urls)[[1]]
    urls[[1]]
  }
  # message(url)
  # Getting token if not set for current session
  if(is.null(conf$token) || !exists("token", where = conf)) conf$token <- get_token()
  # Trying to get the response quadratic waiting time is implemented if the URL fails 
  res <-  tryCatch({
        httr::GET(
          url
          , if("bearer" %in% class(conf$token)) 
              httr::add_headers(Authorization = paste("Bearer", attr(conf$token, "bearer")$access_token, sep = " "))
            else if(is.character(conf$token)) 
              httr::add_headers(Authorization = paste("Bearer", conf$token, sep = " "))
            else 
              httr::config(token = conf$token)
        )
      }, warning = function(warning_condition) {
        message(paste("retrying because of warning ", warning_condition))
        towait = i * i
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        Sys.sleep(towait)  
        return(twitter_get(urls, i = i + 1, retries = retries))
      }, error = function(error_condition) {
        message(paste("retrying because of error ", error_condition))
        towait = i * i
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        Sys.sleep(towait)  
        return(twitter_get(urls, i = i + 1, retries = retries))
        
    })
  if(res$status_code == 200) {
    # Status code is 200 (OK) 
    if(exists("x-rate-limit-remaining", where = res$headers) && as.integer(res$headers[["x-rate-limit-remaining"]]) == 0 ) {
      # Rate limit has been reached
      conf$token = NULL
      if(length(tryed) < length(url)) {
        # there is still another endpoint to test
        message(paste("The Twitter application endpoint ",last_get$api_ver ,"is about to reach its rate limit, changing endpoint to another available version"))
        tryed <- c(tryed, last_get$api_ver)
        return(twitter_get(urls, i = i + 1, retries = retries, tryed = tryed))

      } else if(exists("x-rate-limit-reset", where = res$headers)) {
        # Sleeping the requested number of seconds 
        towait <- as.integer(floor(as.numeric(difftime(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01"),Sys.time(), units = "secs"))))  
        message(paste("The Twitter application endpoint is ",last_get$api_ver ," about to reach its rate limit, waiting", towait,"seconds  until", Sys.time() + towait, " as requested by Twitter"))
        if(towait < 1) {
          towait = i * i
        } else {
          towait = towait + 5  
        }
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        Sys.sleep(towait)  
      } else {
        # If no waiting time has been provided (should not happen) waiting 15 minutes which is the default rate limit window
        towait <- 15*60 
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        message(paste("The Twitter application endpoint ",last_get$api_ver ,"is about to reach its rate limit, but no waiting instruction has been detected. Waiting", towait,"seconds until", Sys.time()+towait))
        Sys.sleep(towait)  
      }
    }
    return(httr::content(res,as="text"))
  }
  else if(res$status_code == 420 && i <= retries) {
    # Adding 60 seconds wait if application is being requested to slow down, and retries is less than limit
    message("The Twitter application is slowing down (rate limited) by Twitter, waiting 1 minute before continuing")
    save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
    Sys.sleep(60)
    conf$token = NULL
    return(twitter_get(urls, i = i + 1, retries = retries, tryed = tryed))
  } else if(res$status_code == 429 && i < retries) {
    # If application is rate limited, waiting and retrying
    if(length(tryed) < length(url)) {
      # there is still another endpoint to test
      message(paste("The Twitter application endpoint ",last_get$api_ver ,"has reached its rate limit, changing endpoint to another available version"))
      tryed <- c(tryed, last_get$api_ver)
    } else if(exists("x-rate-limit-reset", where = res$headers)) {
      # Calculating the number of seconds requested to wait  
      towait <- as.integer(floor(as.numeric(difftime(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01"),Sys.time(), units = "secs"))))  
      message(paste("The Twitter application endpoint ",last_get$api_ver ,"has reached its rate limit, waiting", towait,"seconds  until", Sys.time()+towait, "as requested by twitter"))
      if(towait < 1) {
        towait = i * i
      } else {
        towait = towait + 5  
      }
      #resetting tries and last_get to start all over again
      tryed <- list()
      rm("api_ver", envir = last_get)

      save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
      Sys.sleep(towait)  
    } else {
      # Waiting 15 minutes before retry if no waiting instruction is identified
      towait <- 15*60 
      message(paste("The Twitter application endpoint has reached its rate limit, but no waiting instruction has been detected. Waiting", towait,"seconds"))
      save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
      Sys.sleep(towait + 10)  
    }
    conf$token = NULL
    # Logging the obtained error
    writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
    return(twitter_get(urls, i = i + 1, retries = retries, tryed = tryed))
  } else if(res$status_code >= 401 && res$status_code <= 403 && i >= retries) {
    # Stopping if non authorized by twitter
    writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
    message(res$status_code)
    stop(paste("Unauthorized by twitter APIi", res$status_code))
  } else if(last_get$api_ver == "2" && {
      mess <- jsonlite::fromJSON(httr::content(res,as="text"))
      if(is.na(is.na(mess$errors$parameters) || is.na(names(mess$errors$parameters))))
        FALSE
      else 
        names(mess$errors$parameters) == "since_id"
    }) {
    # Special error in API 2 when query is too old
    message("Error interpreted as too old query on Twitter API v2. Assuming empty result set")
    message(mess)
    stop("too-old")
  } else if(i <= retries) {
      # Other non-expected cases, retrying with a quadratic waiting rule at most (retries - i) more times
      message(paste("Error status code ",res$status_code,"returned by Twitter API waiting for", i*i, "seconds before retry with a new token"))  
      message(httr::content(res,as="text"))
      save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
      Sys.sleep(i * i)
      conf$token = NULL
      return(twitter_get(urls, i = i + 1, retries = retries, tryed = tryed))
  } else {
     #Run out of retries, stopping
     writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
     stop(paste("Run out of options to call API after",i, "retries. URL tryed:", url ))
  }
}
