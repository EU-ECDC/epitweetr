#' Twitter base enpoint for direct api calls
t_endpoint <- "https://api.twitter.com/1.1/"

#' Twitter endpoint for gettind rate limit status
ratelimit_endpoint <- paste(t_endpoint, "application/rate_limit_status.json", sep = "")

