#' Twitter base enpoint for direct api calls
t_endpoint <- "https://api.twitter.com/1.1/"

#' Twitter endpoint for getting rate limit status
ratelimit_endpoint <- paste(t_endpoint, "application/rate_limit_status.json", sep = "")

#' Twitter endpoint for searching tweets
search_endopoint <-  paste(t_endpoint, "search/tweets.json", sep = "")

#' Get twitter token
get_token <- function() {
  token <- rtweet::create_token(
    app = conf$twitter_auth$app
    , consumer_key =conf$twitter_auth$api_key
    , consumer_secret =conf$twitter_auth$api_secret
    , access_token =conf$twitter_auth$access_token
    , access_secret = conf$twitter_auth$access_token_secret)  
  return(token)
}

#' Execute a get request on twitter
#' It wil internally deal with token génération and rate limits 
twitter_get <- function(url, i = 0, retries = 20) {
  if(is.null(conf$token) || !exists("token", where = conf)) conf$token <- get_token()
  res <- httr::GET(url, httr::config(token = conf$token))
  if(res$status_code == 200) {
    if(exists("x-rate-limit-remaining", where = res$headers) && as.integer(res$headers[["x-rate-limit-remaining"]]) == 0 ) {
      conf$token = NULL
      if(exists("x-rate-limit-reset", where = res$headers)) {
        towait <- as.integer(floor(as.numeric(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01") - Sys.time())))  
        warning(paste("The twitter application enpoint is about to get rate limitted, waiting", towait,"seconds as requested by twitter"))
        Sys.sleep(towait)  
      } else {
        towait <- 15*60 
        warning(paste("The twitter application enpoint is about to get rate limited, but no waiting instruction has been detected. Waiting", towait,"seconds"))
        Sys.sleep(towait)  
      }
    }
    return(res)
  }
  else if(res$status_code == 420 && i <= retries) {
    warning("The twitter application is being slown down (rate limited) by twitter, waiting 1 minute before continuing")
    Sys.sleep(60)
    conf$token = NULL
    return(twitter_get(url, i = i + 1, retries = retries))
  } else if(res$status_code == 429 && i < retries) {
    if(exists("x-rate-limit-reset", where = res$headers)) {
      towait <- as.integer(floor(as.numeric(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01") - Sys.time())))  
      warning(paste("The twitter application enpoint has reached its rate limit, waiting", towait,"seconds as requested by twitter"))
      Sys.sleep(towait)  
    } else {
      towait <- 15*60 
      warning(paste("The twitter application enpoint has reached its rate limit, but no waiting instruction has been detected. Waiting", towait,"seconds"))
      Sys.sleep(towait)  
    }
    conf$token = NULL
    return(twitter_get(url, i = i + 1, retries = retries))
  } else if(i <= retries) {
      warning(paste("Error status code ",res$status_code,"returned by twitter API waiting for", i*i, "seconds before retry with a new token"))  
      Sys.sleep(i * i)
      conf$token = NULL
      return(twitter_get(url, i = i + 1, retries = retries))
  } else {
     stop(paste("Run out of option to call API after",i, "retries. URL tryed:", url ))
  }
}
