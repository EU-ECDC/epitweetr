#Environment for storing configuration
conf <- new.env()
#Environment for storing cached data
cached <- new.env()

#' Get package name
get_package_name <- function() environmentName(environment(setup_config))

#' Get platform dependent keyring
#'
get_key_ring <- function(backend = NULL) {
  if(!is.null(backend)) {
    Sys.setenv(kr_backend = backend)
  }
  Sys.setenv(kr_name = "ecdc_wtitter_tool_kr_name")
  Sys.setenv(kr_service = "kr_service")
  keyring = Sys.getenv("kr_backend")
  if(keyring == "secret_service") {
    kb <- keyring::backend_secret_service$new()
  } else if(keyring == "wincred") {
    kb <- keyring::backend_wincred$new()
  } else if(keyring == "macos") {
    kb <- keyring::backend_macos$new()
  } else if(keyring == "file") {
    kb <- keyring::backend_file$new()
  } else {
    kb <- keyring::backend_env$new()
  }
  kr_name <- Sys.getenv("kr_name") 
  if(kb$has_keyring_support()) {
     krs <- kb$keyring_list()
     if(nrow(krs[krs$keyring == kr_name,]) == 0) {
       kb$keyring_create(kr_name)
       #kb$.__enclos_env__$private$keyring_create_direct(kr_name, kr_passphrase)
     }
    kb$keyring_set_default(kr_name)
  }

  if(kb$keyring_is_locked(keyring = kr_name) && Sys.getenv("ecdc_wtitter_tool_kr_password") != "") {
    kb$keyring_unlock(keyring = kr_name, password =  Sys.getenv("ecdc_wtitter_tool_kr_password"))
  } else if(kb$keyring_is_locked(keyring = kr_name)) {
    kb$keyring_unlock(keyring = kr_name)
  }
  return (kb)
}

#' Set a secret from the chosen secret management backend
set_secret <- function(secret, value) {
  get_key_ring()$set_with_value(service = Sys.getenv("kr_service"), username = secret, password = value)
}

#' Checks weather a secret is set on the secret management backend
is_secret_set <- function(secret) {
 secrets <- get_key_ring()$list(service =  Sys.getenv("kr_service")) 
 return(nrow(secrets[secrets$username == secret, ])>0) 
}
#' Get a secret from the chosen secret management backend
get_secret <- function(secret) {
  get_key_ring()$get(service =  Sys.getenv("kr_service"), username = secret)
}

#' get empty config for initialization
get_empty_config <- function() {
  ret <- list()
  ret$keyring <- 
   if(.Platform$OS.type == "windows") "wincred"
   else if(.Platform$OS.type == "mac") "macos"
   else "file"
  ret$dataDir <- paste(getwd(),"data", sep = "/")
  ret$schedule_span <- 90
  ret$geolocation_threshold <- 5
  ret$spark_cores <- parallel::detectCores(all.tests = FALSE, logical = TRUE)
  ret$spark_memory <- "12g"
  ret$rJava <- TRUE
  ret$topics <- list()
  ret$topics_md5 <- ""
  return(ret)
}

#' Build configuration values for application from a configuration json file
#' @export
setup_config <- function(paths = list(props = "data/properties.json", topics = "data/topics.json"), topics_path = "data/topics.xlsx", ignore_keyring=FALSE, save_first = list()) 
{
  if(length(save_first) > 0) {
    save_config(paths = paths[unlist(save_first)])
  }
  #Loading last created configuration from json file on temp variable if exists or load default empty conf instead
  temp <- get_empty_config()
  
  if(exists("props", where = paths) && file.exists(paths$props)) {
    temp = merge_configs(list(temp, jsonlite::read_json(paths$props, simplifyVector = FALSE, auto_unbox = TRUE)))
    #Setting config  variables filled only from json file  
    conf$keyring <- temp$keyring
    conf$schedule_span <- temp$schedule_span
    conf$dataDir <- temp$dataDir
    conf$geonames <- temp$geonames
    conf$languages <- temp$languages
    conf$spark_cores <- temp$spark_cores
    conf$spark_memory <- temp$spark_memory
    conf$rJava <- temp$rJava
    conf$geolocation_threshold <- temp$geolocation_threshold
  }
  if(file.exists(paths$topics)) {
    temp = merge_configs(list(temp, jsonlite::read_json(paths$topics, simplifyVector = FALSE, auto_unbox = TRUE)))
    #Getting topics from excel topics files if it has changed since las load
    topics <- 
      if(file.exists(topics_path)) {
        t <- list()
        t$md5 <- as.vector(tools::md5sum(topics_path))
        if(t$md5 != temp$topics_md5) { 
          t$df <- readxl::read_excel(topics_path)
        }
        t
      } else {
        list()
      }
    
    #Meging topics from config json and topic excel topics if this last one has changed
    #Each time a topic is found on file, all its occurrencies will be processed at the same time, to ensure consistent multi query topics updates based on position
    if(exists("df", where = topics)) {
      distinct_topics <- as.list(unique(topics$df$Topic))
      adjusted_topics <- list()
      i_adjusted <- 1
      #For each distinct topic on excel file
      for(i_topic in 1:length(distinct_topics)) {
        topic <- distinct_topics[[i_topic]]
        if(!grepl("^[A-Za-z_0-9][A-Za-z_0-9 \\-]*$", topic)) {
          stop(paste("topic name", topic, "is invalid, it must contains only by alphanumeric letters, digits spaces '-' and '_' and not start with spaces, '-' or '_'", sep = " "))
        }
        i_tmp <- 1
        queries <- topics$df[topics$df$Topic == topic, ]
        #For each distincit query on excel file on current topic
        for(i_query in 1:nrow(queries)) {
          #Looking for the next matching entry in json file
          while(i_tmp <= length(temp$topics) && temp$topics[[i_tmp]]$topic != topic) { i_tmp <- i_tmp + 1 }
          if(i_tmp <= length(temp$topics)) {
            #reusing an existing query
            adjusted_topics[[i_adjusted]] <- temp$topics[[i_tmp]]
            adjusted_topics[[i_adjusted]]$query <- queries$Query[[i_query]]
          } else {
            #creating a new query  
            adjusted_topics[[i_adjusted]] <- list()
            adjusted_topics[[i_adjusted]]$query <- queries$Query[[i_query]]
            adjusted_topics[[i_adjusted]]$topic <- queries$Topic[[i_query]]
          }
          i_adjusted <- i_adjusted + 1
          i_tmp <- i_tmp + 1
        }
      }
      temp$topics <- adjusted_topics
      temp$topics_md5 <- topics$md5
    }
 
    #Loading topic related infomation on config file 
    conf$topics_md5 <- temp$topics_md5 
    conf$topics <- temp$topics
  
    #Copying plans
    if(length(temp$topics)>0) {
      for(i in 1:length(temp$topics)) {
        if(!exists("plan", where = temp$topics[[i]]) || length(temp$topics[[i]]$plan) == 0) {
          conf$topics[[i]]$plan <- list()
        }
        else {
          conf$topics[[i]]$plan <-  
          lapply(1:length(temp$topics[[i]]$plan), 
            function(j) get_plan(
              expected_end = temp$topics[[i]]$plan[[j]]$expected_end
              , scheduled_for = temp$topics[[i]]$plan[[j]]$scheduled_for
              , start_on = temp$topics[[i]]$plan[[j]]$start_on
              , end_on = temp$topics[[i]]$plan[[j]]$end_on
              , max_id = temp$topics[[i]]$plan[[j]]$max_id
              , since_id = temp$topics[[i]]$plan[[j]]$since_id
              , since_target = temp$topics[[i]]$plan[[j]]$since_target
              , results_span = temp$topics[[i]]$plan[[j]]$results_span
              , requests = temp$topics[[i]]$plan[[j]]$requests
              , progress = temp$topics[[i]]$plan[[j]]$progress
          ))
        }
      }
    } 
  }
  
  #Getting variables stored on keyring
  #Setting up keyring
  if(!ignore_keyring) {
    kr <- get_key_ring(conf$keyring)
    conf$twitter_auth <- list()
    # Fetching and updating variables from keyring
    for(v in c("app", "access_token", "access_token_secret", "api_key", "api_secret")) {
      if(is_secret_set(v)) {
        conf$twitter_auth[[v]] <- get_secret(v)
      }
    }
  }
}

#' Save the configuration options to disk
#' @export
save_config <- function(paths = list(props = "data/properties.json", topics = "data/topics.json")) {
  if(exists("props", where = paths)) {
    temp <- list()
    temp$dataDir <- conf$dataDir
    temp$schedule_span <- conf$schedule_span
    temp$geonames <- conf$geonames
    temp$languages <- conf$languages
    temp$keyring <- conf$keyring
    temp$spark_cores <- conf$spark_cores
    temp$spark_memory <- conf$spark_memory
    temp$rJava <- conf$rJava
    temp$geolocation_threshold <- conf$geolocation_threshold
    jsonlite::write_json(temp, paths$props, pretty = TRUE, force = TRUE, auto_unbox = TRUE)
  }
  if(exists("topics", where = paths)) {
    temp <- list()
    temp$topics <- conf$topics
    temp$topics_md5 <- conf$topics_md5
    jsonlite::write_json(temp, paths$topics, pretty = TRUE, force = TRUE, auto_unbox = TRUE)
  }
}

#' Update twitter auth tokens on configuration object
#' @export
set_twitter_app_auth <- function(app, access_token, access_token_secret, api_key, api_secret) {
  conf$twitter_auth$app <- app
  conf$twitter_auth$access_token <- access_token
  conf$twitter_auth$access_token_secret <- access_token_secret
  conf$twitter_auth$api_key <- api_key
  conf$twitter_auth$api_secret <- api_secret
  for(v in c("app", "access_token", "access_token_secret", "api_key", "api_secret")) {
    set_secret(v, conf$twitter_auth[[v]])
  }
}

#' Merging two or more configuration files as a list
merge_configs <- function(configs) {
  if(length(configs)==0)
    stop("No configurations provided for merge")
  else if(length(configs)==1)
    configs[[1]]
  else {
    first <- configs[[1]]
    rest <- merge_configs(configs[-1])
    keys <- unique(c(names(first), names(rest)))
    as.list(setNames(mapply(function(x, y) if(is.null(y)) x else y, first[keys], rest[keys]), keys))
  }
      
}
