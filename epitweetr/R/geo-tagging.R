#' Launches the geo-tagging loop
#' This function will geoloacte all tweets before the current hour that has not been already geolocated
#' @export
geotag_tweets <- function(tasks = get_tasks()) {
  stop_if_no_config(paste("Cannot get tweets without configuration setup")) 
  # Creating parameters from configuration file as java objects
  tweet_path <- paste(conf$data_dir, "/tweets/search", sep = "")
  geolocated_path <- paste(conf$data_dir, "/tweets/geolocated", sep = "")
  index_path <- paste(conf$data_dir, "/geo/lang_vectors.index", sep = "") 

  tasks <- tryCatch({
    tasks <- update_geotag_task(tasks, "running", "processing", start = TRUE)
    spark_job(
      paste(
	"extractLocations"
        , "sourcePath", paste("\"", tweet_path, "\"", sep="")
        , "destPath", paste("\"", geolocated_path, "\"", sep = "") 
        ,  conf_languages_as_arg()
        ,  conf_geonames_as_arg()
        , "langIndexPath", paste("\"", index_path, "\"", sep = "")
        , "minScore" , conf$geolocation_threshold
        , "parallelism", conf$spark_cores 
      )
    )
    
    # Setting status to succès
    tasks <- update_geotag_task(tasks, "success", "", end = TRUE)
    tasks
  }, warning = function(error_condition) {
    # Setting status to failed
    tasks <- update_geotag_task(tasks, "failed", paste("failed while", tasks$geotag$message," ", error_condition))
    tasks
  }, error = function(error_condition) {
    # Setting status to failed
    tasks <- update_geotag_task(tasks, "failed", paste("failed while", tasks$geotag$message," ", error_condition))
    tasks
  })
  return(tasks)
}

#' Get the SQL like expression to extract tweet geolocation variables 
get_tweet_location_var <- function(varname) {
  if(varname == "longitude" || varname == "latitude")
    paste("coalesce("
      , "text_loc.geo_", varname
      , ", linked_text_loc.geo_", varname
      , ")"
      , sep = ""
    )  
  else 
    paste("coalesce("
      , "text_loc.", varname
      , ", linked_text_loc.", varname
      , ")"
      , sep = ""
    )  
}

#' Get tweet geolocation used cols 
get_tweet_location_columns <- function(table) {
  if(table == "tweet")
    list(
    )
  else 
    list(
      "text_loc"
      ,"linked_text_loc"
    )
}
#' Get the SQL like expression to extract user geolocation variables 
get_user_location_var <- function(varname) {
  if(varname == "longitude" || varname == "latitude")
    paste("coalesce("
      , "tweet_", varname
      , ", place_", varname
      , ", place_full_name_loc.geo_",varname
      , ", linked_place_", varname
      , ", linked_place_full_name_loc.geo_",varname
      , ", user_location_loc.geo_", varname
      , ", user_description_loc.geo_", varname
      , ")"
      , sep = ""
    )  
  else 
    paste("coalesce("
      , "place_full_name_loc.",varname
      , ", linked_place_full_name_loc.",varname
      , ", user_location_loc.", varname
      , ", user_description_loc.", varname
      , ")" 
      , sep = ""
    )  
}
#' Get user geolocation used cols 
get_user_location_columns <- function(table) {
  if(table == "tweet")
    list(
      "tweet_longitude"
      , "tweet_latitude"
      , "place_longitude"
      , "place_latitude"
      , "linked_place_longitude"
      , "linked_place_latitude"
    )
  else 
    list(
      "user_location_loc"
      , "user_description_loc"
      , "place_full_name_loc"
      , "linked_place_full_name_loc"
      , "text_loc"
      , "linked_text_loc"
      , "place_full_name_loc"
      , "linked_place_full_name_loc"
      )
}

#' Get all tweets from json files of search api and json file from geolocated tweets obtained by calling (geotag_tweets)
#' @export
get_geotagged_tweets <- function(regexp = list(".*"), vars = list("*"), group_by = list(), sort_by = list(), filter_by = list(), sources_exp = list(), handler = NULL, params = NULL) {
 stop_if_no_config(paste("Cannot get tweets without configuration setup")) 
 # Creating parameters from configuration file as java objects
 tweet_path <- paste(conf$data_dir, "/tweets/search", sep = "")
 geolocated_path <- paste(conf$data_dir, "/tweets/geolocated", sep = "")
  
 # Geolocating all non located tweets before current hour
 # System call will be performed to execute java based geolocation
 # json results are piped as dataframe
 df <- spark_df(
    paste(
       "getTweets"
       ,"tweetPath", paste("\"", tweet_path, "\"", sep="")
       ,"geoPath", paste("\"", geolocated_path, "\"", sep = "") 
       ,"pathFilter", shQuote(paste(regexp, collapse = ",")) 
       ,"columns", paste("\"", paste(vars, collapse = "||") ,"\"",sep="") 
       ,"groupBy", paste("\"", paste(group_by, collapse = "||") ,"\"",sep="") 
       ,"sortBy", paste("\"", paste(sort_by, collapse = "||") ,"\"",sep="") 
       ,"filterBy", paste("\"", paste(filter_by, collapse = "||") ,"\"",sep="") 
       ,"sourceExpressions", paste(
          "\""
          , paste("tweet", paste(sources_exp$tweet, collapse = "||"), sep = "||")
          , "|||"
          , paste("geo", paste(sources_exp$geo, collapse = "||"), sep = "||") 
          , "\""
          , sep=""
        ) 
       ,conf_languages_as_arg()
       ,"parallelism", conf$spark_cores 
       ,if(!is.null(params)) paste("params", shQuote(params)) else ""
     )
    , handler
 )
 return(df)
}

#' Get a sample of todays tweet for evaluatin geolocation threshold
#' @export
get_todays_sample_tweets <- function(limit = 1000, text_col = "text", lang_col = NA) {
 stop_if_no_config(paste("Cannot get tweets without configuration setup")) 

 # Creating parameters from configuration file as java objects
 tweet_path <- paste(conf$data_dir, "/tweets/search", sep = "")
 index_path <- paste(conf$data_dir, "/geo/lang_vectors.index", sep = "") 
 spark_df(
   paste(
     "getSampleTweets"
     , "tweetPath", paste("\"", tweet_path, "\"", sep="")
     , "pathFilter", paste("\"", strftime(Sys.time(), format = ".*%Y\\.%m\\.%d.*") ,"\"",sep="") 
     ,  conf_languages_as_arg()
     ,  conf_geonames_as_arg()
     , "langIndexPath", paste("\"", index_path, "\"", sep = "")
     , "limit" , limit
     , "parallelism" , conf$spark_cores
     , paste(
         "textCol "
         , text_col
         , if(is.na(lang_col)) "" else " langCol "
         , if(is.na(lang_col)) "" else lang_col
         , sep = ""
       )
   )
 )
}


#' Get raw countries from file
get_raw_countries <- function() {
  df <- readxl::read_excel(get_countries_path()) 
  names(df)<-make.names(names(df),unique = TRUE)
  df$minLat <- as.numeric(df$minLat)
  df$maxLat <- as.numeric(df$maxLat)
  df$minLong <- as.numeric(df$minLong)
  df$maxLong <- as.numeric(df$maxLong)
  df
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
    countries <- get_raw_countries() %>% dplyr::filter(region != "")

    # using intermediate region when available
    countries$sub.region <- mapply(function(int, sub) {if(is.na(int)) sub else int}, countries$intermediate.region, countries$sub.region)
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
    items[[row]] <- list(name = "World (all)", codes = list(), level = 0, minLat = -90, maxLat = 90, minLong = -180, maxLong = 180 )
    row <- 2
    items[[row]] <- list(name = "World (geolocated)", codes = as.list(countries$alpha.2), level = 0, minLat = -90, maxLat = 90, minLong = -180, maxLong = 180 )
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
                , maxLong = countries$maxLong[[c]]
              )
            }
          }
        }
      }
    }
    
    # Getting EU classification
    eu_zones <- countries$EU.EEA[!is.na(countries$EU.EEA)] %>% unique
    eu_codes <- lapply(eu_zones, function(r) {list(name = r, codes = countries$alpha.2[!is.na(countries$EU.EEA) & countries$EU.EEA == r], level = 0.5)}) 
    eu_codes <- (
      if(length(eu_codes)==0) eu_codes
      else c(eu_codes, list(list(name = paste(eu_zones, collapse = "+"), codes = countries$alpha.2[!is.na(countries$EU.EEA)], level = 0.5)))
    )

    # Getting WHO classification
    who_zones <- countries$WHO.Region[!is.na(countries$WHO.Region)] %>% unique
    who_codes <- lapply(who_zones, function(r) {list(name = r, codes = countries$alpha.2[!is.na(countries$WHO.Region) & countries$WHO.Region == r], level = 0.6)}) 
    specials <- c(eu_codes, who_codes)
    for(s in 1:length(specials)) {
      row <- row + 1
      items[[row]] = list(name = specials[[s]]$name, codes = list(), level = specials[[s]]$level, minLat = 90, maxLat = -90, minLong = 180, maxLong = -180) 
      for(c in 1:nrow(countries)) {
        if(countries$alpha.2[[c]] %in% specials[[s]]$codes) {
          # adding country codes for region
          items[[row]]$codes[[length(items[[row]]$codes)+1]] <- countries$alpha.2[[c]]
          # calculating region bounding box 
          if(countries$minLat[[c]] < items[[row]]$minLat) items[[row]]$minLat <- countries$minLat[[c]]
          if(countries$maxLat[[c]] > items[[row]]$maxLat) items[[row]]$maxLat <- countries$maxLat[[c]]
          if(countries$minLong[[c]] < items[[row]]$minLong) items[[row]]$minLong <- countries$minLong[[c]]
          if(countries$maxLong[[c]] > items[[row]]$maxLong) items[[row]]$maxLong <- countries$maxLong[[c]]
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

#' Get country codes by name
#' usage 
#' map <- get_country_codes_by_name()
#' map[["Asia"]] 
get_country_codes_by_name <- function() {
  regions <- get_country_items()  
  return(setNames(sapply(regions, function(r) r$code), sapply(regions, function(r) r$name)))
}

#' Get regions data as dataframe
get_regions_df <- function() {
  regions <- get_country_items()
  data.frame(
    Name = sapply(regions, function(r) r$name) , 
    Codes = sapply(regions, function(r) paste(r$codes, collapse = ", ")), 
    Level = sapply(regions, function(r) r$level), 
    MinLatitude = as.numeric(sapply(regions, function(r) r$minLat)), 
    MaxLatitude = as.numeric(sapply(regions, function(r) r$maxLat)), 
    MinLongitude = as.numeric(sapply(regions, function(r) r$minLong)), 
    MaxLingityde = as.numeric(sapply(regions, function(r) r$maxLong))
  ) 
}

#' Get country indexes
#' usage 
#' map <- get_country_index_map()
#' map[["FR"]] 
get_country_index_map <- function() {
  regions <- get_country_items()
  all_index <- 1:length(regions) 
  indexes <- all_index[sapply(all_index, function(i) regions[[i]]$level == 3)]
  return(setNames(indexes, sapply(indexes, function(i) regions[[i]]$code)))
}

#' Dowloading and indexing a fresh version of geonames database from the  provided URL
update_geonames <- function(tasks) {
  tasks <- tryCatch({
    tasks <- update_geonames_task(tasks, "running", "downloading", start = TRUE)
    # Create geo folder if it does not exists
    if(!file.exists(file.path(conf$data_dir, "geo"))) {
      dir.create(file.path(conf$data_dir, "geo"))   
    }
    # Downloading geonames
    temp <- tempfile()
    temp_dir <- tempdir()
    download.file(tasks$geonames$url, temp, mode="wb")
    
    # Decompressing file
    tasks <- update_geonames_task(tasks, "running", "decompressing")
    unzip(zipfile = temp, files = "allCountries.txt", exdir = temp_dir)
    file.remove(temp)
    file.rename(from = file.path(temp_dir, "allCountries.txt"), to = file.path(conf$data_dir, "geo", "allCountries.txt"))
    
    # Assembling geonames datasets
    tasks <- update_geonames_task(tasks, "running", "assembling")
    spark_job(
       paste(
        "updateGeonames"
        , conf_geonames_as_arg()
        , "assemble", paste("\"TRUE\"",sep="") 
        , "index", paste("\"FALSE\"",sep="") 
        , "parallelism", conf$spark_cores 
      )
    )
    
    # Indexing geonames datasets
    tasks <- update_geonames_task(tasks, "running", "indexing")
    spark_job(
      paste(
        "updateGeonames"
        , conf_geonames_as_arg()
        , "assemble", paste("\"FALSE\"",sep="") 
        , "index", paste("\"TRUE\"",sep="") 
        , "parallelism", conf$spark_cores 
      )
    )
    
    # Setting status to succès
    tasks <- update_geonames_task(tasks, "success", "", end = TRUE)
    tasks
  }, warning = function(error_condition) {
    # Setting status to failed
    tasks <- update_geonames_task(tasks, "failed", paste("failed while", tasks$geonames$message," geonames", error_condition))
    tasks
  }, error = function(error_condition) {
    # Setting status to failed
    tasks <- update_geonames_task(tasks, "failed", paste("failed while", tasks$geonames$message," geonames", error_condition))
    tasks
  })
  return(tasks)
}


#' Dowloading and indexing a fresh version of language models tagges for update
#' @export
update_languages <- function(tasks) {
  index_path <- paste(conf$data_dir, "/geo/lang_vectors.index", sep = "") 
  tasks <- tryCatch({
    tasks <- update_languages_task(tasks, "running", "downloading", start = TRUE)

    # Create languages folder if it does not exist
    if(!file.exists(file.path(conf$data_dir, "languages"))) {
      dir.create(file.path(conf$data_dir, "languages"))   
    }

    # executing actions per language
    for(i in 1:length(tasks$languages$statuses)) {
      # downloading when necessary
      if(tasks$languages$statuses[[i]] %in% c("to add", "to update")) {
        tasks <- update_languages_task(tasks, "running", paste("downloading", tasks$languages$name[[i]]), lang_code = tasks$languages$code[[i]], lang_start = TRUE)
        temp <- tempfile()
        download.file(tasks$languages$url[[i]],temp,mode="wb")
        file.rename(from = file.path(temp), to = tasks$languages$vectors[[i]])
        tasks <- update_languages_task(tasks, "running", paste("downloaded", tasks$languages$name[[i]]), lang_code = tasks$languages$code[[i]], lang_done = TRUE)
      }  
    }
    # Indexing languages
    tasks <- update_languages_task(tasks, "running", "indexing")
    spark_job(
      paste(
        "updateLanguages"
        , conf_geonames_as_arg()
        , conf_languages_as_arg()
        , "langIndexPath", paste("\"", index_path, "\"", sep = "")
        , "parallelism", conf$spark_cores 
      )
    )
    
    # Setting status to succès
    tasks <- update_languages_task(tasks, "success", "", end = TRUE)
    tasks
  }, warning = function(error_condition) {
    # Setting status to failed
    tasks <- update_languages_task(tasks, "failed", paste("failed while", tasks$languages$message," languages", error_condition))
    tasks
  }, error = function(error_condition) {
    # Setting status to failed
    tasks <- update_languages_task(tasks, "failed", paste("failed while", tasks$languages$message," languages", error_condition))
    tasks
  })
  return(tasks)
}

