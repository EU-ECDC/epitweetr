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
  kr_name <- NULL
  if(keyring == "file" ) {
     kr_name <- Sys.getenv("kr_name") 
     krs <- kb$keyring_list()
     if(nrow(krs[krs$keyring == kr_name,]) == 0) {
       kb$keyring_create(kr_name)
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
get_empty_config <- function(data_dir) {
  ret <- list()
  ret$keyring <- 
   if(.Platform$OS.type == "windows") "wincred"
   else if(.Platform$OS.type == "mac") "macos"
   else "file"
  ret$data_dir <- data_dir
  ret$schedule_span <- 90
  ret$schedule_start_hour <- 8
  ret$languages <- list(
    list(code="en", name="English", vectors=paste(ret$data_dir, "languages/en.txt.gz", sep = "/"), modified_on=strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    list(code="fr", name="French", vectors=paste(ret$data_dir, "languages/fr.txt.gz", sep = "/"), modified_on=strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    list(code="es", name="Spanish", vectors=paste(ret$data_dir, "languages/es.txt.gz", sep = "/"), modified_on=strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    list(code="pt", name="Portuguese", vectors=paste(ret$data_dir, "languages/pt.txt.gz", sep = "/"), modified_on=strftime(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  )
  ret$lang_updated_on <- NA
  ret$geonames_updated_on <- NA
  ret$geonames_url <- "http://download.geonames.org/export/dump/allCountries.zip"
  ret$geolocation_threshold <- 5
  ret$known_users <- list()
  ret$spark_cores <- parallel::detectCores(all.tests = FALSE, logical = TRUE)
  ret$spark_memory <- "12g"
  ret$topics <- list()
  ret$topics_md5 <- ""
  ret$alert_alpha <- 0.025
  ret$alert_history <- 7
  ret$use_mkl <- FALSE
  ret$geonames_simplify <- TRUE
  ret$regions_disclaimer <- ""
  ret$smtp_host <- ""
  ret$smtp_port <- 25
  ret$smtp_from <- ""
  ret$smtp_login <- ""
  ret$smtp_password <- ""
  ret$smtp_insecure <- FALSE
  return(ret)
}

#' Build configuration values for application from a configuration json file
#' @export
setup_config <- function(
  data_dir = 
    if(exists("data_dir", where = conf)) 
      conf$data_dir 
    else if(Sys.getenv("EPI_HOME")!="") 
      Sys.getenv("EPI_HOME") 
    else 
      file.path(getwd(), "epitweetr")
  , ignore_keyring = FALSE
  , ignore_properties = FALSE
  , ignore_topics = FALSE
  , save_first = list()
) 
{
  conf$data_dir <- data_dir
  paths <- list(props = get_properties_path(), topics = get_plans_path())
  topics_path <- get_topics_path(data_dir)

  if(length(save_first) > 0) {
    save_config(data_dir = data_dir, properties = "props" %in% save_first, "topics" %in% save_first)
  }
  #Loading last created configuration from json file on temp variable if exists or load default empty conf instead
  temp <- get_empty_config(data_dir)
  
  if(!ignore_properties && exists("props", where = paths)) {
    if(file.exists(paths$props)) {
      temp = merge_configs(list(temp, jsonlite::read_json(paths$props, simplifyVector = FALSE, auto_unbox = TRUE)))
    }
    #Setting config  variables filled only from json file  
    conf$keyring <- temp$keyring
    conf$schedule_span <- temp$schedule_span
    conf$schedule_start_hour <- temp$schedule_start_hour
    conf$languages <- temp$languages
    for(i in 1:length(conf$languages)) {conf$languages[[i]]$vectors <- file.path(conf$data_dir, "languages", paste(conf$languages[[i]]$code, "txt", "gz", sep = "."))} 
    conf$lang_updated_on <- temp$lang_updated_on
    conf$geonames_updated_on <- temp$geonames_updated_on
    conf$geonames_url <- temp$geonames_url
    conf$known_users <- temp$known_users
    conf$spark_cores <- temp$spark_cores
    conf$spark_memory <- temp$spark_memory
    conf$geolocation_threshold <- temp$geolocation_threshold
    conf$alert_alpha <- temp$alert_alpha
    conf$alert_history <- temp$alert_history
    conf$use_mkl <- temp$use_mkl
    conf$geonames_simplify <- temp$geonames_simplify
    conf$regions_disclaimer <- temp$regions_disclaimer
    conf$smtp_host <- temp$smtp_host
    conf$smtp_port <- temp$smtp_port
    conf$smtp_from <- temp$smtp_from
    conf$smtp_login <- temp$smtp_login
    conf$smtp_insecure <- temp$smtp_insecure
  }
  if(!ignore_topics && exists("topics", where = paths)){
    if(file.exists(paths$topics)) {
      temp = merge_configs(list(temp, jsonlite::read_json(paths$topics, simplifyVector = FALSE, auto_unbox = TRUE)))
    }
    #Getting topics from excel topics files if it has changed since las load
    #If user has not ovewritten 
    topics <- {
      t <- list()
      t$md5 <- as.vector(tools::md5sum(topics_path))
      if(t$md5 != temp$topics_md5) { 
        t$df <- readxl::read_excel(topics_path)
      }
      t
    }
    
    #Merging topics from config json and topic excel topics if this last one has changed
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
    copy_plans_from(temp)  
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
    if(is_secret_set("smtp_password")) {
      conf$smtp_password <- get_secret("smtp_password")
    }
  }
}

#' Copying plans from temporary file (non typed) to conf, making sure plans have the right type
copy_plans_from <- function(temp) {
  #Copying plans
  if(length(temp$topics)>0) {
    for(i in 1:length(temp$topics)) {
      if(!exists("plan", where = temp$topics[[i]]) || length(temp$topics[[i]]$plan) == 0) {
        conf$topics[[i]]$plan <- list()
      }
      else {
        conf$topics[[i]]$plan <- ( 
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
        )))
      }
    }
  }
}
get_properties_path <- function() file.path(conf$data_dir, "properties.json")
get_plans_path <- function() file.path(conf$data_dir, "topics.json")

#' Save the configuration options to disk
#' @export
save_config <- function(data_dir = conf$data_dir, properties= TRUE, topics = TRUE) {
  # creating data directory if does nor exists
  if(!file.exists(conf$data_dir)){
    dir.create(conf$data_dir, showWarnings = FALSE)
  }  

  if(properties) {
    temp <- list()
    temp$schedule_span <- conf$schedule_span
    temp$schedule_start_hour <- conf$schedule_start_hour
    temp$languages <- conf$languages
    temp$lang_updated_on <- conf$lang_updated_on
    temp$geonames_updated_on <- conf$geonames_updated_on
    temp$geonames_url <- conf$geonames_url
    temp$keyring <- conf$keyring
    temp$known_users <- conf$known_users
    temp$spark_cores <- conf$spark_cores
    temp$spark_memory <- conf$spark_memory
    temp$geolocation_threshold <- conf$geolocation_threshold
    temp$alert_alpha <- conf$alert_alpha
    temp$alert_history <- conf$alert_history
    temp$use_mkl <- conf$use_mkl
    temp$geonames_simplify <- conf$geonames_simplify
    temp$regions_disclaimer <- conf$regions_disclaimer
    temp$smtp_host <- conf$smtp_host
    temp$smtp_port <- conf$smtp_port
    temp$smtp_from <- conf$smtp_from
    temp$smtp_login <- conf$smtp_login
    if(!is.null(conf$smtp_password) && conf$smtp_password != "") set_secret("smtp_password", conf$smtp_password)
    temp$smtp_insecure <- conf$smtp_insecure
    jsonlite::write_json(temp, get_properties_path(), pretty = TRUE, force = TRUE, auto_unbox = TRUE)
  }
  if(topics) {
    temp <- list()
    temp$topics <- conf$topics
    temp$topics_md5 <- conf$topics_md5
    # Transforming Int64 to string to ensure not loosing precision on read
    for(i in 1:length(conf$topics)) {         
      for(j in 1:length(conf$topics[[i]]$plan)) {
        temp$topics[[i]]$plan[[j]]$since_id = as.character(conf$topics[[i]]$plan[[j]]$since_id)
        temp$topics[[i]]$plan[[j]]$max_id = as.character(conf$topics[[i]]$plan[[j]]$max_id)
        temp$topics[[i]]$plan[[j]]$since_target = as.character(conf$topics[[i]]$plan[[j]]$since_target)
      }
    }
    jsonlite::write_json(temp, get_plans_path(), pretty = TRUE, force = TRUE, auto_unbox = TRUE)
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

#' Get available languages file path
get_available_languages_path <- function() {
  path <- paste(conf$data_dir, "languages.xlsx", sep = "/")
  if(!file.exists(path))
    path <- system.file("extdata", "languages.xlsx", package = get_package_name())
  path
}

#' Get current available languages
get_available_languages <- function() {
  readxl::read_excel(get_available_languages_path()) 
}

#' Check config setup before continue
stop_if_no_config <- function(error_message = "Cannot continue wihout setting up a configuration") {
  if(!exists("data_dir", where = conf)) {
    stop("Cannot register please setup configuration")  
  }
}

#' Call config if necessary
setup_config_if_not_already <- function() {
  if(!exists("data_dir", where = conf)) {
    setup_config() 
  }
}

#' Get topics file path either from user or package location
get_known_users_path <- function(data_dir = conf$data_dir) {
    users_path <- paste(data_dir, "users.xlsx", sep = "/")
    if(!file.exists(users_path))
      users_path <- system.file("extdata", "users.xlsx", package = get_package_name())
    return(users_path)
}

#' Get current known users
get_known_users <- function() {
  gsub("@", "", readxl::read_excel(get_known_users_path())[[1]])
}

#' Save know users to json filr
export_known_users <- function() {
  jsonlite::write_json(get_known_users(), path = file.path(conf$data_dir, "known_users.json"))
}

#' Get topics file path either from user or package location
get_topics_path <- function(data_dir = conf$data_dir) {
    topics_path <- paste(data_dir, "topics.xlsx", sep = "/")
    if(!file.exists(topics_path))
      topics_path <- system.file("extdata", "topics.xlsx", package = get_package_name())
    return(topics_path)
}

#' Remove language 
remove_config_language <- function(code) {
  # Timestaming action 
  conf$lang_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
  # Removing the language
  conf$languages <- conf$languages[sapply(conf$languages, function(l) l$code != code)] 
}

#' Add language
add_config_language <- function(code, name) {
  # Timestaming action 
  conf$lang_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
  index <- (1:length(conf$languages))[sapply(conf$languages, function(l) l$code == code)]
  if(length(index)>0) {
    # Language code is already on the task list
    conf$languages[[index]]$code <- code
    conf$languages[[index]]$name <- name
    conf$languages[[index]]$vectors=paste(conf$data_dir, "/languages/", code, ".txt.gz", sep = "")
    conf$languages[[index]]$modified_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
  } else {
    # Language code is not the list
    conf$languages <- c(
      conf$languages,
      list(list(
        code = code,
        name = name,
        vectors = paste(conf$data_dir, "/languages/", code, ".txt.gz", sep = ""),
        modified_on = strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      ))
    )
  } 
}

