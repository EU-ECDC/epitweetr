# Environment for storing configuration
conf <- new.env()

# Get package name
get_package_name <- function() environmentName(environment(setup_config))

# Get the keyring for the provided backend or a platform dependent default if backend id null
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

# Set a secret from the chosen secret management backend
set_secret <- function(secret, value) {
  get_key_ring()$set_with_value(service = Sys.getenv("kr_service"), username = secret, password = value)
}

# Checks weather a secret is set on the secret management backend
is_secret_set <- function(secret) {
 secrets <- get_key_ring()$list(service =  Sys.getenv("kr_service")) 
 return(nrow(secrets[secrets$username == secret, ])>0) 
}

# Get a secret from the chosen secret management backend
get_secret <- function(secret) {
  get_key_ring()$get(service =  Sys.getenv("kr_service"), username = secret)
}

# get empty config for initialization
get_empty_config <- function(data_dir) {
  ret <- list()
  ret$keyring <- 
   if(.Platform$OS.type == "windows") "wincred"
   else if(Sys.info()[['sysname']] == "Darwin") "macos"
   else "file"
  ret$data_dir <- data_dir
  ret$collect_span <- 60
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
  ret$dep_updated_on <- NA
  ret$geotag_requested_on <- NA
  ret$aggregate_requested_on <- NA
  ret$alerts_requested_on <- NA
  ret$geonames_url <- "http://download.geonames.org/export/dump/allCountries.zip"
  ret$geolocation_threshold <- 5
  ret$known_users <- list()
  ret$spark_cores <- {
    all_cores <- floor(parallel::detectCores(all.tests = FALSE, logical = TRUE)*0.5)
    if(is.na(all_cores)) 1
    else if(all_cores <1) 1 
    else all_cores
  }
  ret$spark_memory <- "4g"
  ret$topics <- list()
  ret$topics_md5 <- ""
  ret$alert_alpha <- 0.025
  ret$alert_alpha_outlier <- 0.05
  ret$alert_k_decay <- 4
  ret$alert_history <- 7
  ret$alert_same_weekday_baseline <- FALSE
  ret$alert_with_retweets <- FALSE
  ret$alert_with_bonferroni_correction <- TRUE
  ret$use_mkl <- FALSE
  ret$geonames_simplify <- TRUE
  ret$regions_disclaimer <- ""
  ret$smtp_host <- ""
  ret$smtp_port <- 25
  ret$smtp_from <- ""
  ret$smtp_login <- ""
  ret$smtp_password <- ""
  ret$smtp_insecure <- FALSE
  ret$force_date_format <- ""
  ret$maven_repo <- "https://repo1.maven.org/maven2"
  ret$winutils_url <- "http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe"
  return(ret)
}

#' @title Load epitweetr application settings
#' @description Load epitweetr application settings from the designated data directory
#' @param data_dir Path to the directory containing the application settings (it must exist). 
#' If not provided it takes the value of the latest call to setup_config in the current session, or the value of the EPI_HOME environment variable or epitweetr subdirectory in the working directory, 
#' default: if (exists("data_dir", where = conf)) conf$data_dir else if (Sys.getenv("EPI_HOME") !=
#'    "") Sys.getenv("EPI_HOME") else file.path(getwd(), "epitweetr")
#' @param ignore_keyring Whether to skip loading settings from the keyring (Twitter and SMTP credentials), default: FALSE
#' @param ignore_properties Whether to skip loading settings managed by the Shiny app in properties.json file, Default: FALSE
#' @param ignore_topics Whether to skip loading settings defined in the topics.xlsx file and download plans from topics.json file, default: FALSE
#' @param save_first Whether to save current settings before loading new ones from disk, default: list()
#' @return Nothing
#' @details epitweetr relies on settings and data stored in a system folder, so before loading the dashboard, collecting tweets or detecting alerts the user has to designate this folder.
#' When a user wants to use epitweetr from the R console they will need to call this function for initialisation.
#' The 'data_folder' can also be given as a parameter for program launch functions \code{\link{epitweetr_app}}, \code{\link{search_loop}} or \code{\link{detect_loop}}, which will internally call this function.
#'
#' This call will fill (or refresh) a package scoped environment 'conf' that will store the settings. Settings stored in conf are:
#' \itemize{
#'   \item{ General properties of the Shiny app (stored in properties.json)}
#'   \item{Download plans from the Twitter collection process (stored in topics.json merged with data from the topics.xlsx file}
#'   \item{Credentials for Twitter API and SMTP stored in the defined keyring}
#' }
#'
#' When calling this function and the keyring is locked, a password will be prompted to unlock the keyring.
#' This behaviour can be changed by setting the enviroment variable 'ecdc_wtitter_tool_kr_password' with the password.
#' 
#' Changes made to conf can be stored permanently (except for 'data_dir') using:
#' \itemize{
#'   \item{\code{\link{save_config}}, or}
#'    \item{\code{\link{set_twitter_app_auth}}}
#' }
#' @examples
#' if(FALSE){
#'    library(epitweetr)
#'    #loading system settings
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#' }
#' @seealso
#' \code{\link{save_config}}
#' \code{\link{set_twitter_app_auth}}
#' \code{\link{epitweetr_app}}
#' \code{\link{search_loop}}
#' \code{\link{detect_loop}}
#' @rdname setup_config
#' @export
#' @importFrom jsonlite read_json
#' @importFrom tools md5sum
#' @importFrom readxl read_excel
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
    conf$collect_span <- temp$collect_span
    conf$schedule_span <- temp$schedule_span
    conf$schedule_start_hour <- temp$schedule_start_hour
    conf$languages <- temp$languages
    for(i in 1:length(conf$languages)) {conf$languages[[i]]$vectors <- file.path(conf$data_dir, "languages", paste(conf$languages[[i]]$code, "txt", "gz", sep = "."))} 
    conf$lang_updated_on <- temp$lang_updated_on
    conf$geonames_updated_on <- temp$geonames_updated_on
    conf$dep_updated_on <- temp$dep_updated_on
    conf$geotag_requested_on <- temp$geotag_requested_on
    conf$aggregate_requested_on <- temp$aggregate_requested_on
    conf$alerts_requested_on <- temp$alerts_requested_on
    conf$geonames_url <- temp$geonames_url
    conf$known_users <- temp$known_users
    conf$spark_cores <- temp$spark_cores
    conf$spark_memory <- temp$spark_memory
    conf$geolocation_threshold <- temp$geolocation_threshold
    conf$alert_alpha <- temp$alert_alpha
    conf$alert_alpha_outlier <- temp$alert_alpha_outlier
    conf$alert_k_decay <- temp$alert_k_decay
    conf$alert_history <- temp$alert_history
    conf$alert_same_weekday_baseline <- temp$alert_same_weekday_baseline
    conf$alert_with_retweets <- temp$alert_with_retweets
    conf$alert_with_bonferroni_correction <- temp$alert_with_bonferroni_correction
    conf$use_mkl <- temp$use_mkl
    conf$geonames_simplify <- temp$geonames_simplify
    conf$regions_disclaimer <- temp$regions_disclaimer
    conf$smtp_host <- temp$smtp_host
    conf$smtp_port <- temp$smtp_port
    conf$smtp_from <- temp$smtp_from
    conf$smtp_login <- temp$smtp_login
    conf$smtp_insecure <- temp$smtp_insecure
    conf$force_date_format <- temp$force_date_format
    conf$maven_repo <- temp$maven_repo
    conf$winutils_url <- temp$winutils_url
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
        #For each distinct query on excel file on current topic
        for(i_query in 1:nrow(queries)) {
          #Looking for the next matching entry in json file
          while(i_tmp <= length(temp$topics) && temp$topics[[i_tmp]]$topic != topic) { i_tmp <- i_tmp + 1 }
          if(i_tmp <= length(temp$topics)) {
            #reusing an existing query
            adjusted_topics[[i_adjusted]] <- temp$topics[[i_tmp]]
            adjusted_topics[[i_adjusted]]$query <- queries$Query[[i_query]]
            adjusted_topics[[i_adjusted]]$label <- queries$Label[[i_query]]
            adjusted_topics[[i_adjusted]]$alpha <-  if(!is.null(queries$Alpha[[i_query]]) && !is.na(queries$Alpha[[i_query]])) queries$Alpha[[i_query]] else conf$alert_alpha
            adjusted_topics[[i_adjusted]]$alpha_outlier <-  (
              if(!is.null(queries$`Outliers Alpha`[[i_query]]) && !is.na(queries$`Outliers Alpha`[[i_query]])) 
                queries$`Outliers Alpha`[[i_query]] 
              else 
                conf$alert_alpha_outlier
            )
          } else {
            #creating a new query  
            adjusted_topics[[i_adjusted]] <- list()
            adjusted_topics[[i_adjusted]]$query <- queries$Query[[i_query]]
            adjusted_topics[[i_adjusted]]$topic <- queries$Topic[[i_query]]
            adjusted_topics[[i_adjusted]]$label <- queries$Label[[i_query]]
            adjusted_topics[[i_adjusted]]$alpha <- if(!is.null(queries$Alpha[[i_query]]) && !is.na(queries$Alpha[[i_query]])) queries$Alpha[[i_query]] else conf$alert_alpha
            adjusted_topics[[i_adjusted]]$alpha_outlier <- (
              if(!is.null(queries$`Outliers Alpha`[[i_query]]) && !is.na(queries$`Outliers Alpha`[[i_query]])) 
                queries$`Outliers Alpha`[[i_query]] 
              else 
                conf$alert_alpha_outlier
            )
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

# Copying plans from temporary file (non typed) to conf, making sure plans have the right type
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

#' @title Save the configuration changes 
#' @description Permanently saves configuration changes to the data folder (excluding Twitter credentials, but not SMTP credentials)
#' @param data_dir Path to a directory to save configuration settings, Default: conf$data_dir
#' @param properties Whether to save the general properties to the properties.json file, default: TRUE
#' @param topics Whether to save topic download plans to the topics.json file, default: TRUE
#' @return Nothing
#' @details Permanently saves configuration changes to the data folder (excluding Twitter credentials, but not SMTP credentials)
#' to save Twitter credentials please use \code{\link{set_twitter_app_auth}}
#' @examples 
#' if(FALSE){
#'    library(epitweetr)
#'    #load configuration
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    #make some changes
#'    #conf$collect_span = 90
#'    #saving changes    
#'    save_config()
#' }
#' @rdname save_config
#' @seealso
#' \code{\link{setup_config}}
#' \code{\link{set_twitter_app_auth}}
#' @export 
save_config <- function(data_dir = conf$data_dir, properties= TRUE, topics = TRUE) {
  # creating data directory if does nor exists
  if(!file.exists(conf$data_dir)){
    dir.create(conf$data_dir, showWarnings = FALSE)
  }  

  if(properties) {
    temp <- list()
    temp$collect_span <- conf$collect_span
    temp$schedule_span <- conf$schedule_span
    temp$schedule_start_hour <- conf$schedule_start_hour
    temp$languages <- conf$languages
    temp$dep_updated_on <- conf$dep_updated_on
    temp$lang_updated_on <- conf$lang_updated_on
    temp$geonames_updated_on <- conf$geonames_updated_on
    temp$geotag_requested_on <- conf$geotag_requested_on
    temp$aggregate_requested_on <- conf$aggregate_requested_on
    temp$alerts_requested_on <- conf$alerts_requested_on
    temp$geonames_url <- conf$geonames_url
    temp$keyring <- conf$keyring
    temp$known_users <- conf$known_users
    temp$spark_cores <- conf$spark_cores
    temp$spark_memory <- conf$spark_memory
    temp$geolocation_threshold <- conf$geolocation_threshold
    temp$alert_alpha <- conf$alert_alpha
    temp$alert_alpha_outlier <- conf$alert_alpha_outlier
    temp$alert_k_decay <- conf$alert_k_decay
    temp$alert_history <- conf$alert_history
    temp$alert_same_weekday_baseline <- conf$alert_same_weekday_baseline
    temp$alert_with_retweets <- conf$alert_with_retweets
    temp$alert_with_bonferroni_correction <- conf$alert_with_bonferroni_correction
    temp$use_mkl <- conf$use_mkl
    temp$geonames_simplify <- conf$geonames_simplify
    temp$regions_disclaimer <- conf$regions_disclaimer
    temp$smtp_host <- conf$smtp_host
    temp$smtp_port <- conf$smtp_port
    temp$smtp_from <- conf$smtp_from
    temp$smtp_login <- conf$smtp_login
    if(!is.null(conf$smtp_password) && conf$smtp_password != "") set_secret("smtp_password", conf$smtp_password)
    temp$smtp_insecure <- conf$smtp_insecure
    temp$force_date_format <- conf$force_date_format
    temp$maven_repo <- conf$maven_repo
    temp$winutils_url <- conf$winutils_url
    write_json_atomic(temp, get_properties_path(), pretty = TRUE, force = TRUE, auto_unbox = TRUE)
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
    write_json_atomic(temp, get_plans_path(), pretty = TRUE, force = TRUE, auto_unbox = TRUE)
  }
}

#' @title Save Twitter App credentials
#' @description Update Twitter authentication tokens in a configuration object
#' @param app Application name
#' @param access_token Access token as provided by Twitter
#' @param access_token_secret Access token secret as provided by Twitter
#' @param api_key API key as provided by Twitter 
#' @param api_secret API secret as provided by Twitter
#' @return Nothing
#' @details Update Twitter authentication tokens in configuration object
#' @examples 
#' if(FALSE){
#'  #Setting the configuration values
#'    set_twitter_app_auth(
#'      app = "my super app", 
#'      access_token = "123456", 
#'      access_token_secret = "123456", 
#'      api_key = "123456", 
#'      api_secret = "123456"
#'    )
#' }
#' @seealso
#' \code{\link{save_config}}
#' @rdname set_twitter_app_auth
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

# Merging two or more configuration files as a list
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

# Get topics dataframe as displayed on the shinty configuration tab
get_topics_df <- function() {
  data.frame(
    Topics = sapply(conf$topics, function(t) t$topic), 
    Label = sapply(conf$topics, function(t) t$label), 
    Query = sapply(conf$topics, function(t) t$query), 
    QueryLength = sapply(conf$topics, function(t) nchar(t$query)), 
    ActivePlans = sapply(conf$topics, function(t) length(t$plan)), 
    Progress = sapply(conf$topics, function(t) {if(length(t$plan)>0) mean(unlist(lapply(t$plan, function(p) p$progress))) else 0}), 
    Requests = sapply(conf$topics, function(t) {if(length(t$plan)>0) sum(unlist(lapply(t$plan, function(p) p$requests))) else 0}),
    Alpha = sapply(conf$topics, function(t) t$alpha),
    OutliersAlpha = sapply(conf$topics, function(t) t$alpha_outlier),
    stringsAsFactors=FALSE
  )
}

#Get topic labels as named array that can be used for translation
get_topics_labels <- function() {
  `%>%` <- magrittr::`%>%`
  t <- ( 
    get_topics_df() %>% 
      dplyr::group_by(.data$Topics) %>%
      dplyr::summarise(label = .data$Label[which(!is.na(.data$Label))[1]]) %>%
      dplyr::ungroup()
  )
  setNames(t$label, t$Topics)
}

#Get topic alphas as named array that can be used for translation
get_topics_alphas <- function() {
  `%>%` <- magrittr::`%>%`
  t <- ( 
    get_topics_df() %>% 
      dplyr::group_by(.data$Topics) %>%
      dplyr::summarise(alpha = .data$Alpha[which(!is.na(.data$Alpha))[1]]) %>%
      dplyr::ungroup()
  )
  setNames(t$alpha, t$Topics)
}

#Get topic outliers alphas as named array that can be used for translation
get_topics_alpha_outliers <- function() {
  `%>%` <- magrittr::`%>%`
  t <- ( 
    get_topics_df() %>% 
      dplyr::group_by(.data$Topics) %>%
      dplyr::summarise(alpha_outlier = .data$OutliersAlpha[which(!is.na(.data$OutliersAlpha))[1]]) %>%
      dplyr::ungroup()
  )
  setNames(t$alpha_outlier, t$Topics)
}

#Get topic k_decay as named array that can be used for translation
get_topics_k_decays <- function() {
  `%>%` <- magrittr::`%>%`
  t <- ( 
    get_topics_df() %>% 
      dplyr::group_by(.data$Topics) %>%
      dplyr::summarise(k_decay = conf$alert_k_decay) %>%
      dplyr::ungroup()
  )
  setNames(t$k_decay, t$Topics)
}

# Check config setup before continue
stop_if_no_config <- function(error_message = "Cannot continue wihout setting up a configuration") {
  if(!exists("data_dir", where = conf)) {
    stop("Cannot register please setup configuration")  
  }
}

# Call config if necessary
setup_config_if_not_already <- function() {
  if(!exists("data_dir", where = conf)) {
    setup_config() 
  }
}

# Get current available languages from the available language excel file
get_available_languages <- function() {
  readxl::read_excel(get_available_languages_path()) 
}

# Remove language from the used languages on conf
remove_config_language <- function(code) {
  # Timestaming action 
  conf$lang_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
  # Removing the language
  conf$languages <- conf$languages[sapply(conf$languages, function(l) l$code != code)] 
}

# Add language on to the used languages on conf
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

# Get current known users list from the important user files
get_known_users <- function() {
  gsub("@", "", readxl::read_excel(get_known_users_path())[[1]])
}

# Wrapper for jsonlite write_json ensuring atomic file write it replaces always the existing file. It ignores appends modifiers
write_json_atomic <- function(x, path, ...) {
  file_name <- tail(strsplit(path, "/|\\\\")[[1]], 1)
  dir_name <- substring(path, 1, nchar(path) - nchar(file_name) - 1)
  swap_file <- tempfile(pattern=paste("~", file_name, sep = ""), tmpdir=dir_name)
  jsonlite::write_json(x = x, path = swap_file, ...)
  file.rename(swap_file, path)
}
