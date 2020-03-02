

#' Get twitter token
get_token <- function(conf) {
  token <- rtweet::create_token(
    app = conf$twitter_auth$app
    , consumer_key =conf$twitter_auth$api_key
    , consumer_secret =conf$twitter_auth$api_secret
    , access_token =conf$twitter_auth$access_token
    , access_secret = conf$twitter_auth$access_token_secret)  
  return(token)
}


#' Search for all tweets on topics defined on configuration
#' @export
search <- function(conf) {
  token <- get_token(conf)
  return(rtweet::search_tweets("ebola", n = 100, 
    include_rts = TRUE,
    token = token,
    retryonratelimit = FALSE, 
    type = "recent")
  )
}

#' Create a new get plan for importing tweets using the search api
get_plan <- function(scheduled, started = FALSE, done = FALSE, max = -1, min = 1, min_target = -1, next_on = 60) {
  me <- list(schedulded, started, done, bit64::as.integer64(max), bit64::as.integer64(min), bit64::as.integer64(min_target), next_on)
  class(me) <- append(class(me), "get_plan")
  return(me) 
}

#' next plan generic function
next_plan <- function(x) {
  UseMethod("next_plan",x)
}

#' get next plan for a previous plan
next_plan.get_plan <- function(previous) {
  return(get_plan(
    scheduled = ifelse(Sys.time()>previous$scheduled + 60*previous$next_on, Sys.time(), previous$scheduled + 60*previous$next_on)
    , min_target = ifelse(previous$max > 0, previous$max + 1, -1)
    , next_on = previous$next_on)
  )
}

#' get
twitter_get <- function(url, conf, token = NULL, i = 0) {
  if(is.null(token)) token <- get_token(conf)
  res <- httr::GET(url, httr::config(token = token))
  if(res$status_code == 200) {
    if(exists("x-rate-limit-remaining", where = res$headers) && as.integer(res$headers[["x-rate-limit-remaining"]]) == 0 ) {
      if(exists("x-rate-limit-reset", where = res$headers)) {
        towait <- as.integer(floor(as.numeric(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01") - Sys.time())))  
        warning(paste("The twitter application enpoint is about to get rate limitted, waiting", towait,"seconds as requested by twitter"))
        Sys.sleep(towait)  
      } else {
        towait <- 15*60 
        warning(paste("The twitter application enpoint is bout to get rate limited, but no waiting instruction has been detected. Waiting", towait,"seconds"))
        Sys.sleep(towait)  
      }
    }
    return(res)
  }
  else if(res$status_code == 420) {
    warning("The twitter application is being slown down (rate limited) by twitter, waiting 1 minute before continuing")
    Sys.sleep(60)
    return (res)
  } else if(res$status_code == 429) {
    if(exists("x-rate-limit-reset", where = res$headers)) {
      towait <- as.integer(floor(as.numeric(as.POSIXct(as.integer(res$headers[["x-rate-limit-reset"]]),  origin="1970-01-01") - Sys.time())))  
      warning(paste("The twitter application enpoint has reached its rate limit, waiting", towait,"seconds as requested by twitter"))
      Sys.sleep(towait)  
    } else {
      towait <- 15*60 
      warning(paste("The twitter application enpoint has reached its rate limit, but no waiting instruction has been detected. Waiting", towait,"seconds"))
      Sys.sleep(towait)  
    }
    return (res)
  } else if(i <= 20) {
      warning(paste("Error status code ",res$status_code,"returned by twitter API waiting for", i*i, "seconds before retry with a new token"))  
      Sys.sleep(i * i)
      res <- twitter_get(url, conf, token, i + 1)
  } else {
     stop(paste("Run out of option to call API after",i, "retries" ))  
  }
}
