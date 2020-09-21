# Twitter base enpoint for direct api calls
t_endpoint <- "https://api.twitter.com/1.1/"

# Twitter endpoint for getting rate limit status
ratelimit_endpoint <- paste(t_endpoint, "application/rate_limit_status.json", sep = "")

# Twitter endpoint for searching tweets
search_endopoint <-  paste(t_endpoint, "search/tweets.json", sep = "")

# Get twitter token
get_token <- function(request_new = TRUE) {
  if(exists("access_token", where = conf$twitter_auth) && conf$twitter_auth$access_token!="") {  
    if(file.exists("~/.rtweet_token.rds")) file.remove("~/.rtweet_token.rds")
    token <- rtweet::create_token(
      app = conf$twitter_auth$app
      , consumer_key =conf$twitter_auth$api_key
      , consumer_secret =conf$twitter_auth$api_secret
      , access_token =conf$twitter_auth$access_token
      , access_secret = conf$twitter_auth$access_token_secret)
    token <- rtweet::bearer_token(token)
  } else {
    token <- (
       if(interactive() && request_new)
         rtweet::get_token()
       else if(file.exists("~/.rtweet_token.rds")){
         readRDS("~/.rtweet_token.rds")
       } else if(request_new){
         stop("Cannot get a token on an non interactive session. Create it from configuration page first")
       } else 
         stop("Cannot ger a token. Please create it from the configuration page")
     )
  } 
  return(token)
}

# Execute a get request on twitter
# It wil internally deal with token génération and rate limits 
twitter_get <- function(url, i = 0, retries = 20) {
  if(is.null(conf$token) || !exists("token", where = conf)) conf$token <- get_token()
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
    if(exists("x-rate-limit-remaining", where = res$headers) && as.integer(res$headers[["x-rate-limit-remaining"]]) == 0 ) {
      conf$token = NULL
      if(exists("x-rate-limit-reset", where = res$headers)) {
        towait <- as.integer(floor(as.numeric(difftime(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01"),Sys.time(), units = "secs"))))  
        message(paste("The twitter application enpoint is about to get rate limitted, waiting", towait,"seconds  until", Sys.time() + towait, " as requested by twitter"))
        if(towait < 1) {
          towait = i * i
        } else {
          towait = towait + 5  
        }
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        Sys.sleep(towait)  
      } else {
        towait <- 15*60 
        save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
        message(paste("The twitter application enpoint is about to get rate limited, but no waiting instruction has been detected. Waiting", towait,"seconds until", Sys.time()+towait))
        Sys.sleep(towait)  
      }
    }
    return(res)
  }
  else if(res$status_code == 420 && i <= retries) {
    message("The twitter application is being slown down (rate limited) by twitter, waiting 1 minute before continuing")
    save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
    Sys.sleep(60)
    conf$token = NULL
    return(twitter_get(url, i = i + 1, retries = retries))
  } else if(res$status_code == 429 && i < retries) {
    if(exists("x-rate-limit-reset", where = res$headers)) {
      towait <- as.integer(floor(as.numeric(difftime(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01"),Sys.time(), units = "secs"))))  
      message(paste("The twitter application enpoint has reached its rate limit, waiting", towait,"seconds  until", Sys.time()+towait, "as requested by twitter"))
      if(towait < 1) {
        towait = i * i
      } else {
        towait = towait + 5  
      }
      save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
      Sys.sleep(towait)  
    } else {
      towait <- 15*60 
      message(paste("The twitter application enpoint has reached its rate limit, but no waiting instruction has been detected. Waiting", towait,"seconds"))
      save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
      Sys.sleep(towait + 10)  
    }
    conf$token = NULL
    writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
    return(twitter_get(url, i = i + 1, retries = retries))
  } else if(res$status_code <= 401 || res$status_code <= 403) {
    writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
    stop(paste("Unauthorized by twitter API"))
  } else if(i <= retries) {
      message(paste("Error status code ",res$status_code,"returned by twitter API waiting for", i*i, "seconds before retry with a new token"))  
      save_config(data_dir = conf$data_dir, topics = TRUE, properties = FALSE)
      Sys.sleep(i * i)
      conf$token = NULL
      return(twitter_get(url, i = i + 1, retries = retries))
  } else {
     writeLines(httr::content(res,as="text"), paste(conf$data_dir, "lasterror.log", sep = "/"))
     stop(paste("Run out of option to call API after",i, "retries. URL tryed:", url ))
  }
}
