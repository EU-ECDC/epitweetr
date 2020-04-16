#' Launches the geo-tagging loop
#' This function will geoloacte all tweets before the current hour that has not been already geolocated
#' @export
geotag_tweets <- function() {
 # Creating parameters from configuration file as java objects
 tweet_path <- paste(conf$dataDir, "/tweets/search", sep = "")
 geolocated_path <- paste(conf$dataDir, "/tweets/geolocated", sep = "")
 index_path <- paste(conf$dataDir, "/geo/lang_vectors.index", sep = "") 

    cmd <- paste(
      "export OPENBLAS_NUM_THREADS=1"
      ,paste(
        "java"
        , paste("-Xmx", conf$spark_memory, sep = "")
        , " -jar epitweetr/java/ecdc-twitter-bundle-assembly-1.0.jar"
        , "extractLocations"
        , "sourcePath", paste("'", tweet_path, "'", sep="")
        , "destPath", paste("'", geolocated_path, "'", sep = "") 
        ,  conf_languages_as_arg()
        ,  conf_geonames_as_arg()
        , "reuseIndex", "true"
        , "indexPath", paste("'", index_path, "'", sep = "")
        , "minScore" , conf$geolocation_threshold
        , "parallelism", conf$spark_cores 
      )
      ,sep = '\n'
   )
   system(cmd)     
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
      , ")"
      , sep = ""
    )  
  else 
    paste("coalesce("
      , "text_loc.", varname
      , ", linked_text_loc.", varname
      , ", place_full_name_loc.",varname
      , ", linked_place_full_name_loc.",varname
      , ")"
      , sep = ""
    )  
}

#' Get the SQL like expression to extract user geolocation variables 
get_user_location_var <- function(varname) {
  if(varname == "longitude" || varname == "latitude")
    paste("coalesce("
      , "user_location_loc.geo_", varname
      , ", user_description_loc.geo_", varname
      , ")"
      , sep = ""
    )  
  else 
    paste("coalesce("
      , "user_location_loc.", varname
      , ", user_description_loc.", varname
      , ")" 
      , sep = ""
    )  
}


#' Get all tweets from json files of search api and json file from geolocated tweets obtained by calling (geotag_tweets)
#' @export
get_geotagged_tweets <- function(regexp = list(".*"), vars = list("*"), groupBy = list(), sortBy = list(), filterBy = list(), handler = NULL) {
 # Creating parameters from configuration file as java objects
 tweet_path <- paste(conf$dataDir, "/tweets/search", sep = "")
 geolocated_path <- paste(conf$dataDir, "/tweets/geolocated", sep = "")
  
 # Geolocating all non located tweets before current hour
 # If by configuration Rjava is activated it will be used for direct integration with JVM. Otherwise a system call will be performed
 df <- {
     cmd <- paste(
       "export OPENBLAS_NUM_THREADS=1"
       ,paste(
         "java"
         , paste("-Xmx", "4g", sep = "")
         , " -jar epitweetr/java/ecdc-twitter-bundle-assembly-1.0.jar"
         , "getTweets"
         , "tweetPath", paste("\"", tweet_path, "\"", sep="")
         , "geoPath", paste("\"", geolocated_path, "\"", sep = "") 
         , "pathFilter", paste("'", paste(regexp, collapse = ",") ,"'",sep="") 
         , "columns", paste("\"", paste(vars, collapse = "||") ,"\"",sep="") 
         , "groupBy", paste("\"", paste(groupBy, collapse = "||") ,"\"",sep="") 
         , "sortBy", paste("\"", paste(sortBy, collapse = "||") ,"\"",sep="") 
         , "filterBy", paste("\"", paste(filterBy, collapse = "||") ,"\"",sep="") 
         ,  conf_languages_as_arg()
         , "parallelism", conf$spark_cores 
       )
       ,sep = '\n'
     )
     #message(cmd) 
     con <- pipe(cmd)
     if(is.null(handler)) {
       jsonlite::stream_in(con, pagesize = 10000, verbose = TRUE)
     } else {
       tmp_file <- tempfile(pattern = "epitweetr", fileext = ".json")
       #message(tmp_file)
       con_tmp <- file(tmp_file, open = "wb") 
       jsonlite::stream_in(con, pagesize = 10000, verbose = TRUE, function(df) handler(df, con_tmp))
       con_tmp <- file(tmp_file, open = "r") 
       ret <- jsonlite::stream_in(con_tmp, pagesize = 10000, verbose = TRUE)
       #unlink(tmp_file)
       ret
     }    
   }
 return(df)
}

#' Getting a list of regions, sub regions and countries for using as a select
get_country_items <- function(order = "level") {
  `%>%` <- magrittr::`%>%`
  
  #If countries are already on cache we return them otherwise we calculate them
  if(exists(paste("country_items", order, sep = ""), where = cached)) {
    return (cached[["country_items", order, sep = ""]])
  }
  else {
    # Getting country data from package embeded csv, ignoring antartica
    # obtained from https://gist.github.com/AdrianBinDC/621321d6083b176b3a3215c0b75f8146#file-country-bounding-boxes-md
    countries <- read.csv(system.file("extdata", "countries.csv", package = get_package_name()), header = TRUE, stringsAsFactors=FALSE) %>% dplyr::filter(region != "")
    # using intermediate region when available
    countries$sub.region <- mapply(function(int, sub) {if(int == "") sub else int}, countries$intermediate.region, countries$sub.region)
    #getting regions and subregions
    regions <- (
      countries 
        %>% dplyr::group_by(region) 
        %>% dplyr::summarize(minLat = min(minLat), minLong = min(minLong), maxLat = max(maxLat), maxLong = max(maxLong))
        %>% dplyr::ungroup()
    )   
    subregions <- (
      countries 
        %>% dplyr::group_by(region, sub.region) 
        %>% dplyr::summarize(minLat = min(minLat), minLong = min(minLong), maxLat = max(maxLat), maxLong = max(maxLong))
        %>% dplyr::ungroup()
    )   

    # creating list containing results
    items <- list()
    # adding world items  
    row <- 1
    items[[row]] <- list(name = "World", codes = list(), level = 0, minLat = -90, maxLat = 90, minLong = -180, maxLong = 180 )
    for(r in 1:nrow(regions)) {
      row <- row + 1
      region <- regions$region[[r]]
      items[[row]] = list(name = region, codes = list(), level = 1, minLat = 90, maxLat = -90, minLong = 180, maxLong = -180)
      # filling region item
      for(c in 1:nrow(countries)) {
        if(countries$region[[c]] == region) {
          # adding country codes for region
          items[[row]]$codes[[length(items[[row]]$codes)+1]] <- countries$alpha.2[[c]]
          # calculating region bounding box 
          if(countries$minLat[[c]] < items[[row]]$minLat) items[[row]]$minLat <- countries$minLat[[c]]
          if(countries$maxLat[[c]] > items[[row]]$maxLat) items[[row]]$maxLat <- countries$maxLat[[c]]
          if(countries$minLong[[c]] < items[[row]]$minLong) items[[row]]$minLong <- countries$minLong[[c]]
          if(countries$maxLong[[c]] > items[[row]]$maxLong) items[[row]]$maxLong <- countries$maxLong[[c]]
        }
      }
      # Adding elements for subregions
      for(s in 1:nrow(subregions)) {
        if(subregions$region[[s]] == region ) {       
          row <- row + 1
          subregion <- subregions$sub.region[[s]]
          items[[row]] = list(name = subregion, codes = list(), level = 2, minLat = 90, maxLat = -90, minLong = 180, maxLong = -180)
          # filling sub region item
          for(c in 1:nrow(countries)) {
            if(countries$region[[c]] == region && countries$sub.region[[c]] == subregion) {
              # adding country codes for region
              items[[row]]$codes[[length(items[[row]]$codes)+1]] <- countries$alpha.2[[c]]
              # calculating region bounding box 
              if(countries$minLat[[c]] < items[[row]]$minLat) items[[row]]$minLat <- countries$minLat[[c]]
              if(countries$maxLat[[c]] > items[[row]]$maxLat) items[[row]]$maxLat <- countries$maxLat[[c]]
              if(countries$minLong[[c]] < items[[row]]$minLong) items[[row]]$minLong <- countries$minLong[[c]]
              if(countries$maxLong[[c]] > items[[row]]$maxLong) items[[row]]$maxLong <- countries$maxLong[[c]]
            }
          }
          # adding country items
          for(c in 1:nrow(countries)) {
            if(countries$region[[c]] == region && countries$sub.region[[c]] == subregion) {
              row <- row + 1
              country <- countries$name[[c]]
              items[[row]] <- list(
                name = country
                , codes = list(countries$alpha.2[[c]])
                , level = 3
                , minLat = countries$minLat[[c]]
                , maxLat = countries$maxLat[[c]]
                , minLong = countries$minLong[[c]]
                , maxLong = countries$maxLing[[c]]
              )
            }
          }
        }
      }
    }
    if(order == "level")
      items <- items[order(sapply(items,function(i) {paste(i$level, i$name)}))]

    cached[[paste("country_items", order, sep="_")]] <- items    
    return(items)
  }
}

#' Get country codes
#' usage 
#' map <- get_country_code_map()
#' map[["FR"]] 
get_country_code_map <- function() {
  regions <- get_country_items()  
  countries <- regions[sapply(regions, function(i) i$level == 3)]
  return(setNames(sapply(countries, function(r) r$name), sapply(countries, function(r) r$code)))
}

