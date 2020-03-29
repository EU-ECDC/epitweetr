#' Launches the geo-tagging loop
#' This function will try to geolocate all tweets before the current hour
#' @export
as.dataFrame <- function(spark_df, chunk_size) {
  dfs <- list()
  i <- 0
  jsonIterator <- rJava::J("org.ecdc.twitter.JavaBridge")$df2JsonIterator(spark_df, as.integer(chunk_size))
  while(jsonIterator$hasNext()) {
    i <- i + 1
    dfs[[i]] <- jsonlite::fromJSON(jsonIterator$theNext())
  }
  return(jsonlite::rbind_pages(dfs))
}

conf_languages_as_scala <- function() {
  lang_list <- lapply(conf$languages, function(l) {return(rJava::J("org.ecdc.twitter.Language")$apply(l$name, l$code, l$vectors))})
  return(rJava::.jarray(lang_list, "org.ecdc.twitter.Language"))
}

conf_geonames_as_scala <-function() {
  return(rJava::J("org.ecdc.twitter.Geonames")$apply(conf$geonames, destination = paste(conf$dataDir, "geo", sep="/")))
}

get_spark_session <- function() {
  return(rJava::J("org.ecdc.twitter.JavaBridge")$getSparkSession(conf$spark_cores))
}

get_spark_storage <- function(spark) {
  return(rJava::J("org.ecdc.twitter.JavaBridge")$getSparkStorage(spark))
}


