
testPerf <- function(numTweets = 1000, full = FALSE) {
  start.time <- Sys.time()
  df <- epitweetr::get_todays_sample_tweets(numTweets, full)
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  message(as.numeric(time.taken, units="secs"))
  df

}
