# Twitter base endpoint for direct api calls
t_endpoint <- "https://api.twitter.com/1.1/"

# Twitter endpoint for getting rate limit status
ratelimit_endpoint <- paste(t_endpoint, "application/rate_limit_status.json", sep = "")

# Twitter endpoint for searching tweets
search_endopoint <-  paste(t_endpoint, "search/tweets.json", sep = "")

# Get twitter token bases on configuration based on configuration settings (app or user)
# This function is called when saving properties on shiny app and first time epitweetr performs a twitter search in a session
# request_new: I
get_token <- function(request_new = TRUE) {
  if(exists("access_token", where = conf$twitter_auth) && conf$twitter_auth$access_token!="") {  
    # Creating a new app token if app authentication is set
    # Removing existing rtweet token if exists
    if(file.exists("~/.rtweet_token.rds")) file.remove("~/.rtweet_token.rds")
    # Using rtweet call to create token
    token <- rtweet::create_token(
      app = conf$twitter_auth$app
      , consumer_key =conf$twitter_auth$api_key
      , consumer_secret =conf$twitter_auth$api_secret
      , access_token =conf$twitter_auth$access_token
      , access_secret = conf$twitter_auth$access_token_secret)
    # Removing user context
    token <- rtweet::bearer_token(token)
  } else {
    # Using delegated authentication otherwise, token will be reused if it exists and new one will be requested if it is not set
    token <- (
       if(interactive() && request_new)
         rtweet::get_token()
       else if(file.exists("~/.rtweet_token.rds")){
         readRDS("~/.rtweet_token.rds")
       } else if(request_new){
         stop("Cannot get a token on an non interactive session. Create it from configuration page first")
       } else 
         stop("Cannot get a token. Please create it from the configuration page")
     )
  } 
  return(token)
}

# Execute a get request on twitter API using the provided URL
# It wil internally deal with token generation and rate limits
# If request fails with an error or warning it will retry with a cuadratic waiting time
# Twitter response codes are interpreted as per twitter documention to slow down and wait as requested and to respect rate limits
# url: URL to try
# i: Number of tries done already
# This files to file.path(conf$data_dir, "lasterror.log") the last encountered error produced if any
twitter_get <- function(url, i = 0, retries = 20) {
  # Getting token if not set for current session
  if(is.null(conf$token) || !exists("token", where = conf)) conf$token <- get_token()
  # Trying to get the response quadratic waiting time is implemented if the URL fails 
  res <-  tryCatch({
        httr::GET(
          url
          , if("bearer" %in% class(conf$token)) 
              httr::add_headers(Authorization = paste("Bearer", attr(conf$token, "bearer")$access_token, sep = " "))
            else 
              httr::config(token = conf$token)
        )
      }, warning = function(warning_condition) {
        message(paste("retrying because of warning ", warning_condition))
        towait = i * i
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        Sys.sleep(towait)  
        return(twitter_get(url, i = i + 1, retries = retries))
      }, error = function(error_condition) {
        message(paste("retrying because of error ", error_condition))
        towait = i * i
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        Sys.sleep(towait)  
        return(twitter_get(url, i = i + 1, retries = retries))
        
    })
  if(res$status_code == 200) {
    # Status code is 200 (OK) 
    if(exists("x-rate-limit-remaining", where = res$headers) && as.integer(res$headers[["x-rate-limit-remaining"]]) == 0 ) {
      # Rate limit has been reacched
      conf$token = NULL
      if(exists("x-rate-limit-reset", where = res$headers)) {
        # Sleeping the requsted number of secons 
        towait <- as.integer(floor(as.numeric(difftime(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01"),Sys.time(), units = "secs"))))  
        message(paste("The Twitter application endpoint is about to reach its rate limit, waiting", towait,"seconds  until", Sys.time() + towait, " as requested by Twitter"))
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
        message(paste("The Twitter application endpoint is about to reach its rate limit, but no waiting instruction has been detected. Waiting", towait,"seconds until", Sys.time()+towait))
        Sys.sleep(towait)  
      }
    }
    return(res)
  }
  else if(res$status_code == 420 && i <= retries) {
    # Adding 60 seconds wait if application is being requested to slow down, and retries is less than limit
    message("The Twitter application is being slown down (rate limited) by Twitter, waiting 1 minute before continuing")
    save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
    Sys.sleep(60)
    conf$token = NULL
    return(twitter_get(url, i = i + 1, retries = retries))
  } else if(res$status_code == 429 && i < retries) {
    # If application is rate limited, waiting and retrying
    if(exists("x-rate-limit-reset", where = res$headers)) {
      # Calculating the number of seconds requested to wait  
      towait <- as.integer(floor(as.numeric(difftime(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01"),Sys.time(), units = "secs"))))  
      message(paste("The Twitter application endpoint has reached its rate limit, waiting", towait,"seconds  until", Sys.time()+towait, "as requested by twitter"))
      if(towait < 1) {
        towait = i * i
      } else {
        towait = towait + 5  
      }
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
    #Liogging the obtained error
    writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
    return(twitter_get(url, i = i + 1, retries = retries))
  } else if(res$status_code <= 401 || res$status_code <= 403) {
    # Stopping if non authorized by twitter
    writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
    stop(paste("Unauthorized by twitter API"))
  } else if(i <= retries) {
      # Other non expected cases, retrying with a quadratic waiting rule at most (retries - i) more times
      message(paste("Error status code ",res$status_code,"returned by Twitter API waiting for", i*i, "seconds before retry with a new token"))  
      save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
      Sys.sleep(i * i)
      conf$token = NULL
      return(twitter_get(url, i = i + 1, retries = retries))
  } else {
     #Run out of retries, stopping
     writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
     stop(paste("Run out of options to call API after",i, "retries. URL tryed:", url ))
  }
}
