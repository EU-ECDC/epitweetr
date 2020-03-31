#' Launches the geo-tagging loop
#' This function will geoloacte all tweets before the current hour that has not been already geolocated
#' @export
geotag_tweets <- function() {
 # Creating parameters from configuration file as java objects
 tweet_path <- paste(conf$dataDir, "/tweets/search", sep = "")
 geolocated_path <- paste(conf$dataDir, "/tweets/geolocated", sep = "")
 index_path <- paste(conf$dataDir, "/geo/lang_vectors.index", sep = "") 
 langs <- conf_languages_as_scala()
 geonames <- conf_geonames_as_scala()

 #Creating spark session
 spark <- get_spark_session()
 storage <- get_spark_storage(spark)
  
 #Geolocating all non located tweets before current hour
 rJava::J("org.ecdc.twitter.Tweets")$extractLocations(tweet_path, geolocated_path, langs, geonames, TRUE, index_path, conf$geolocation_threshold, conf$spark_cores, spark, storage)

 #Closing spark session
 spark$stop()

}

#' Get the SQL like expression to extract tweet geolocation variables 
get_tweet_location_var <- function(varname) {
  if(varname == "longitude" || varname == "latitude")
    paste("coalesce("
      , "text_loc.geo_", varname
      , ", linked_text_loc.geo_", varname
      , ", ", varname
      , ", place_", varname
      , ", linked_place_", varname
      , ", place_full_name_loc.geo_",varname
      , ", linked_place_full_name_loc.geo_",varname
      , ") as tweet_", varname
      , sep = ""
    )  
  else 
    paste("coalesce("
      , "text_loc.", varname
      , ", linked_text_loc.", varname
      , ", place_full_name_loc.",varname
      , ", linked_place_full_name_loc.",varname
      , ") as tweet_", varname
      , sep = ""
    )  
}

#' Get the SQL like expression to extract user geolocation variables 
get_user_location_var <- function(varname) {
  if(varname == "longitude" || varname == "latitude")
    paste("coalesce("
      , "user_location_loc.geo_", varname
      , ", user_description_loc.geo_", varname
      , ") as user_", varname
      , sep = ""
    )  
  else 
    paste("coalesce("
      , "user_location_loc.", varname
      , ", user_description_loc.", varname
      , ") as user_", varname
      , sep = ""
    )  
}


#' Get all tweets from json files of search api and json file from geolocated tweets obtained by calling (geotag_tweets)
#' @export
get_geotagged_tweets <- function(regexp = list(".*"), vars = list("*")) {
 # Creating parameters from configuration file as java objects
 tweet_path <- paste(conf$dataDir, "/tweets/search", sep = "")
 geolocated_path <- paste(conf$dataDir, "/tweets/geolocated", sep = "")
 langs <- conf_languages_as_scala()

 #Creating spark session
 spark <- get_spark_session()
 storage <- get_spark_storage(spark)
  
 #Geolocating all non located tweets before current hour
 spark_df <- rJava::J("org.ecdc.twitter.Tweets")$getTweets(
   tweet_path
   , geolocated_path 
   , rJava::.jarray(lapply(regexp, function(s) rJava::.jnew("java.lang.String",s)), "java.lang.String")
   , rJava::.jarray(lapply(vars, function(s) rJava::.jnew("java.lang.String",s)), "java.lang.String")
   , langs
   , conf$spark_cores
   , spark
   , storage
 )
 #collecting datframe as R dataframe
 df <- as.dataFrame(spark_df, 100)

 #Closing spark session
 spark$stop()

 return(df)

}

