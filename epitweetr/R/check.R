
# check if Java is installed
check_java_present <- function() {
  if(is_java_present())
    TRUE
  else {
    warning(paste(
      "epitweetr could not detect Java. Please install it and make sure java binary is on path or that JAVA_HOME is set",
      "epitweetr needs 64bits Java between ", java_min_version(), " and ", java_max_version()
    ))
  }
}

# check if Java is installed
is_java_present <- function() {
  programs <- "java"
  # If Java home is not set checking if the system can found Java on the path
  if(Sys.getenv("JAVA_HOME") == "" || is.null(Sys.getenv("JAVA_HOME"))) {
    length(grep(programs, Sys.which(programs))) == length(programs)  
  } else {
    #checking if Java binary can be found from java_home
    if(.Platform$OS.type == "windows")
      file.exists(file.path(Sys.getenv("JAVA_HOME"), "bin", "java.exe"))
    else
      file.exists(file.path(Sys.getenv("JAVA_HOME"), "bin", "java"))
  }
}

# get Java version vector
get_java_version_vector <- function() {
  if(.Platform$OS.type == "windows") {
    tmp <- tempfile()
    shell(paste(java_path(), "-version >", tmp,"2>&1"))
    con <- file(tmp)
    jver <- readLines(con)
    close(con) 
    file.remove(tmp)
    jver
  
  } else {
    con <- pipe(paste(java_path(), "-version 2>&1"))
    jver <- readLines(con)
    close(con) 
    jver
  }
}

# check if Java is 64 bits
check_java_64 <- function() {
  if(any(grepl("64-Bit", get_java_version_vector())))
    TRUE
  else {
    warning(paste("Your current Java version does not seem to be 64 bits. epitweetr needs Java 64 bits between ", java_min_version(), " and ", java_max_version()))
    FALSE  
  }
}

# get Java version
get_java_version <- function() {
  jver <- get_java_version_vector()
  index <- grep("version", jver)
  if(length(index)== 0)
    NULL
  else {
    j_version_parts <- strsplit(gsub("[^0-9]",".", jver[[index[[1]]]]), "\\.+")[[1]]
    j_version_parts <- j_version_parts[grepl("[0-9]", j_version_parts)]
    if(length(j_version_parts) > 1 && as.integer(j_version_parts[[1]]) == 1)
      as.integer(j_version_parts[[2]])
    else
      as.integer(j_version_parts[[1]])
  }

}

java_min_version <- function() 8
java_max_version <- function() 11
java_tested_version <- function() 14
# check Java version
check_java_version <- function() {
  java_version <- get_java_version() 
  if(is.null(java_version)) {
    warning("epitweetr cannot identify your Java version")
    FALSE
  } else if(java_version >= java_min_version() && java_version <= java_max_version()) {
    TRUE  
  } else if(java_version <= java_tested_version()){
    warning(paste(
      "Your current Java version is", 
      java_version, 
      ". epitweetr needs Java versions between ", 
      java_min_version(), 
      " and ", 
      java_max_version(), 
      ". Some users have reported that your version also works, but it is recommended to use previous compatible Java versions"
    ))
    FALSE  
  }
}



is_64bit <- function() {
  if(.Platform$OS.type == "windows") {
    Sys.getenv("PROCESSOR_ARCHITECTURE") == "AMD64"
  } else {
    grepl("64", system("uname -a", intern = TRUE))
  }
}

# check 64 architecture
check_64 <- function() {
  if(is_64bit()) {
    TRUE
  } else {
    warning("Your R seems to be 32bits, epitweetr needs a 64bits R")
    FALSE
  }
}


# check msvcr100 on Windows machines
check_winmsvcr100 <- function() {
  #inspired from sparklyr https://github.com/sparklyr/sparklyr/blob/master/R/install_spark_windows.R
  if(.Platform$OS.type == "windows") {
    systemRoot <- Sys.getenv("SystemRoot")
    systemDir <- "system32"
    msvcr100Path <- normalizePath(file.path(systemRoot, systemDir, "msvcr100.dll"), winslash = "/", mustWork = FALSE)
    if(file.exists(msvcr100Path))
      TRUE
    else {
      warning(paste("Running Spark on Windows requires the",
        "Microsoft Visual C++ 2010 SP1 Redistributable Package.",
        "Please download and install from: \nhttps://www.microsoft.com/download/en/details.aspx?id=13523",
        "Then restart R after the installation is completed"
      ))
      FALSE
    }
  } else {
    TRUE
  }
  
}

#check winutils is installed
check_winutils <- function() {
  if(.Platform$OS.type == "windows") {
    if(file.exists(get_winutils_path()))
      TRUE
    else {
      warning("To run SPARK on Windows you need winutils which is a HADOOP binary, you can download it by running the update dependencies task")  
      FALSE
    }
  } else {
    TRUE
  }
}

#check download Java dependencies has been executed
check_java_deps <- function() {
  if(!dir.exists(get_jars_dest_path())) {
    warning("epitweetr needs the dependencies task to run and download all Java dependencies")
    FALSE 
  } else {
    con <- file(get_sbt_file_dep_path())
    deps <- readLines(con)
    close(con)
    downloaded <- list.files(get_jars_dest_path())
    if(length(deps) != length(downloaded)) {
      warning("epitweetr needs the dependencies task to run and download all Java dependencies")
      FALSE 
    } else {
      TRUE  
    }
  }
}

#check geonames is downloaded and indexed
check_geonames <- function() {
  if(!file.exists(get_geonames_txt_path())) {
    warning("epitweetr needs geonames to be downloaded and unzipped for geolocating tweets. Please run the geonames task of the requirements and alert pipeline")
    FALSE
  } else {
    dir <- get_geonames_index_path()
    ret <- (
      if(!file.exists(dir)) 
        FALSE
      else {
        dirs <- list.dirs(dir, recursive = FALSE)
        ind_dir <- dirs[grepl(".*/lucIndex[^/]*", dirs)]
        if(length(ind_dir) == 0)
          FALSE
        else
          length(list.files(ind_dir, "_.*\\.cfs")) > 0
       }
    )
    if(ret)
      TRUE
    else {
      warning("geonames has not been indexed. Please execute the geonames task")
      FALSE
    }
  }
}


#check that languages are downloaded and indexed
check_languages <- function() {
  lang_files <- sapply(conf$languages, function(l) l$vectors)
  if(length(lang_files) == 0 || !all(sapply(lang_files, function(f) file.exists(f)))) {
    warning("epitweetr needs languages models to be downloaded for geolocating tweets. Please activate some languages and launch the languages task")
    FALSE  
  } else {
    dir <- get_lang_index_path()
    ret <- (
      if(!file.exists(dir)) 
        FALSE
      else {
        dirs <- list.dirs(dir, recursive = FALSE)
        ind_dir <- dirs[grepl(".*/lucIndex[^/]*", dirs)]
        if(length(ind_dir) == 0)
          FALSE
        else
          length(list.files(ind_dir, "_.*\\.cfs")) > 0
       }
    )
    if(ret)
      TRUE
    else {
      warning("Language vector index has not been built. Please activate some languages and launch the languages task")
      FALSE
    }
  }
}

# check if geolocated files are present (geotag has successfully run)
check_geolocated_present <- function() {
  last_month <- tail(list.dirs(path = file.path(conf$data_dir, "tweets", "geolocated"), recursive = FALSE), n = 1)
  last_file <- tail(list.files(last_month, pattern = "*.json.gz"), n = 1)
  if(length(last_file) > 0) {
    TRUE
  } else {
    warning("No geolocated files found. Please execute the geotag task")
    FALSE
  }
}

# check if aggregated files are present (aggregate has successfully run)
check_series_present <- function(series = c("country_counts", "geolocated", "topwords")) { 
   rds_counts <- sapply(series, function(ds) length(list.files(path = file.path(conf$data_dir, "series"), recursive=TRUE, pattern = paste(ds, ".*\\.Rds", sep=""))))
   fs_counts <- sapply(series, function(ds) length(list.files(path = file.path(conf$data_dir, "fs", ds), recursive=TRUE, pattern = ".*\\.fdt")))
   if(all(rds_counts > 0) || all(fs_counts > 0))
      TRUE
    else {
      warning("No aggregated series found. Please run the search loop to collect and aggregate data")
      FALSE
    }
}

# check if tweet files are present & tweet collection has run
check_tweets_present <- function() {
  if(!is.na(last_fs_updates("tweets"))) {
    TRUE
  } else {
    warning("No tweets found on database. Please execute the Data collection")
    FALSE
  }
}

# check if alert files are present
check_alerts_present <- function() {
  dirs <- list.dirs(path = file.path(conf$data_dir, "alerts"), recursive = FALSE)
  dirs <- dirs[!grepl(".*NA$", dirs)]
  last_year <- tail(dirs, n = 1)
  last_file <- tail(list.files(path = last_year, pattern = "*.json"), n = 1) 
  
  if(length(last_file) > 0) {
    TRUE
  } else {
    warning("No alert files found. Please execute the alerts task")
    FALSE
  }
}

# check if taskScheduleR is installed on Windows
check_scheduler <- function() {
  if(.Platform$OS.type == "windows") {
    installed <- tryCatch({func <-  taskscheduleR::taskscheduler_create;TRUE}, warning = function(w) FALSE, error = function(e) FALSE)
    if(installed) {
      TRUE  
    } else {
      warning("To activate tasks, you need to install taskscheduleR package manually. Please run install.packages('taskscheduleR') on an R session and restart epitweetr")
      FALSE  
    }
  } else {
    TRUE  
  }
}

# check if Pandoc is installed
check_pandoc <- function() {
  if(unname(Sys.which("pandoc")) == "") {
    warning("For exporting the PDF report from the dashboard, Pandoc has to be installed.")
    FALSE  
  } else
    TRUE
}

# check if Pandoc is installed
check_tex <- function() {
  if(unname(Sys.which("pdflatex")) == "") {
    warning(paste(
      "For exporting the PDF report from the dashboard, a tex distribution has to be installed.",
      "We suggest to install tinytex with the following commands on an r session:",
      "install.packages('tinytex'); tinytex::install_tinytex()"
    ))
    FALSE  
  } else
    TRUE
}

# check search is running
check_search_running <- function() {
  if(is_search_running())
    TRUE
  else {
    warning(paste0(
      "Data collection & processing pipeline is not running. On Windows, you can activate it by clicking on the 'activate' Data collection & processing button on the config page ",
      "You can also manually run the Data collection & processing pipeline by executing the following command on a separate R session. epitweetr::search_loop('",
      conf$data_dir,
      "')"
    ))
    FALSE
  }
}

# check fs is running
check_fs_running <- function() {
  if(is_fs_running())
    TRUE
  else {
    warning(paste0(
      "Embedded epitweetr database is not running. On Windows, you can activate it by clicking on the 'activate' epitweetr database button on the config page ",
      "You can also manually run the Data collection & processing pipeline by executing the following command on a separate R session. epitweetr::search_loop('",
      conf$data_dir,
      "')"
    ))
    FALSE
  }
}

# check detect is running
check_detect_running <- function() {
  if(is_detect_running())
    TRUE
  else {
    warning(paste0(
      "Requirements & alerts pipeline is not running. On Windows, you can activate it by clicking on the 'activate' Requirements & alerts button on the config page ",
      "You can also manually run the Requirements & alerts pipeline by executing the following command on a separate R session. epitweetr::detect_loop('",
      conf$data_dir,
      "')"
    ))
    FALSE
  }
}

# check fs is running
check_fs_running <- function() {
  if(is_fs_running())
    TRUE
  else {
    warning(paste0(
      "Embedded database is not running. On Windows, you can activate it by clicking on the 'activate' database service button on the config page ",
      "You can also manually run the fs service by executing the following command on a separate R session. epitweetr::fs_loop('",
      conf$data_dir,
      "')"
    ))
    FALSE
  }
}

# check Twitter authentication
last_check_twitter_auth <- new.env()

check_twitter_auth <- function() {
  token <- get_token(request_new = FALSE)
  ok <- "Token" %in% class(token) || "bearer" %in% class(token)
  last_check_twitter_auth$value <- ok
  if(ok)
    TRUE 
  else {
    warning("Cannot create a Twitter token, please choose an authentication method on the configuration page")
    FALSE
  }
}
check_last_twitter_auth <- function() {
  if(exists("value", last_check_twitter_auth) && last_check_twitter_auth$value)
    TRUE 
  else {
    warning("Cannot create a Twitter token, please choose an authentication method on the configuration page")
    FALSE
  }
}

#check manual tasks requested
check_manual_task_request <- function() {
  if(
    is.na(conf$dep_updated_on)
    || is.na(conf$geonames_updated_on)
    || is.na(conf$lang_updated_on)
  ) {
    warning("Before running the Requirements & alerts pipeline, you have to click on 'activate' Requirements & alerts pipeline button on configuration page")
    FALSE  
  } else 
    TRUE
  
    
}

# can move from temp to data directory
check_move_from_temp <- function() {
  ok <- tryCatch({
      temp <- tempfile()
      fileConn<-file(temp)
      writeLines(c("hello temp"), fileConn)
      close(fileConn)
      file.rename(temp, file.path(conf$data_dir, "deleteme.temp"))
      file.remove(file.path(conf$data_dir, "deleteme.temp"))
      TRUE
    }, 
    warning = function(m) {FALSE}, 
    error = function(e) { FALSE }
  ) 
  if(ok)
    TRUE 
  else {
    warning(paste(
      "Cannot move files from temporary folder to data dir, possible cross link device.",
      "Please set TMPDIR, TMP or TEMP environment variable to the same drive as the data folder",
      conf$data_dir))
    FALSE
  }
}




#' @title Run automatic sanity checks
#' @description It runs a set of automated sanity checks for helping the user to troubleshot issues 
#' @return Data frame containing the statuses of all realized checks
#' @details This function executes a series of sanity checks, concerning, Java, bitness, task status, dependencies and Twitter authentication.
#' @examples 
#' if(FALSE){
#'    #importing epitweer
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    #running all tests
#'    check_all()
#' }
#' @rdname check_all
#' @export 
check_all <- function() {
  check_twitter_auth()
  checks <- list(
    scheduler = check_scheduler,
    twitter_auth = check_last_twitter_auth,
    search_running = check_search_running,
    database_running = check_fs_running,
    tweets = check_tweets_present, 
    os64 = check_64, 
    java = check_java_present, 
    java64 = check_java_64, 
    java_version = check_java_version, 
    winmsvc = check_winmsvcr100, 
    detect_activation = check_manual_task_request,
    detection_running = check_detect_running,
    winutils = check_winutils, 
    java_deps = check_java_deps, 
    move_from_temp = check_move_from_temp, 
    geonames = check_geonames, 
    languages = check_languages, 
    aggregate = check_series_present, 
    alerts = check_alerts_present,
    pandoc = check_pandoc,
    tex = check_tex
  )
  results <- sapply(
    checks, 
    function(check) 
      tryCatch(
        if(check()) "ok" else "ko", 
        warning = function(m) {as.character(m$message)}, 
        error = function(e) { as.character(e) }) 
  )
  data.frame(check = names(checks), passed = results == "ok", message = ifelse(results == "ok", "", results))  
}

#Environment for storing last admin email
checks <- new.env()

#' @title Send email to administrator if a failure of epitweetr is detected 
#' @description It validates if epitweetr is not collecting tweets, aggregating tweets or not calculating alerts
#' @param send_mail Boolean. Whether an email should be sent to the defined administrator , default: TRUE
#' @param one_per_day Boolean. Whether a limit of one email per day will be applied, default: TRUE
#' @return A list of health check errors found 
#' @details This function sends an email to the defined administrator if epitweetr is not collecting tweets, aggregating tweets or not calculating alerts
#' @examples 
#' if(FALSE){
#'    #importing epitweer
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    #sending the email to the administrator if epitweetr components are not properly working
#'    health_check()
#' }
#' @rdname health_check
#' @export 
health_check <- function(send_mail = TRUE, one_per_day = TRUE) {
  #health check is only done if one_per_day limit is disabled or if no email was already sent since yesterday and it is after 8AM.
  start_of_day <- strptime(strftime(Sys.time(), "%Y-%m-%d"), "%Y-%m-%d")
  alerts <- list()
  if(!one_per_day || !exists("last_check", checks) || (checks$last_check < start_of_day) && as.numeric(strftime(Sys.time(), "%H")) >= 8) {
    # check 01: if last tweet collected date is more than 1 hour
    last_collected <- file.mtime(get_tweet_togeo_path())
    if(is.na(last_collected) || as.numeric(Sys.time() - last_collected, unit = "hours") >= 1 ) {
      alerts <- append(alerts, paste("Tweets have not been collected for more than ", as.integer(as.numeric(Sys.time() - last_collected, unit = "hours")), "hours"))
    }
    
    # Check 02: if geolocated tweets has not been done during more than one hour
    collection_times <- last_fs_updates("tweets")
    last_geo <- collection_times$tweets
    if(is.na(last_geo) || as.numeric(Sys.time() - last_geo, unit = "hours") >= 1 ) {
      alerts <- append(alerts, paste("Tweets have not been geolocated for more than ", as.integer(as.numeric(Sys.time() - last_geo, unit = "hours")), "hours"))
    }
    # Check 03: if any time series has not been processed in more than one hour
    collection_times <- last_fs_updates(c("topwords", "country_counts", "geolocated"))
    for(i in 1:length(collection_times)) {
      serie <- names(collection_times)[[i]]
      last_collect <- collection_times[[serie]]
      if(is.na(last_collect) || as.numeric(Sys.time() - last_collect, unit = "hours") >= 1 ) {
        alerts <- append(alerts, paste("Serie,", serie, "has not been produced for more than ", as.integer(as.numeric(Sys.time() - last_collect, unit = "hours")), "hours"))
      }
    }

    # Check 04: if alert detection has not finished in more than 2 hours + schedule span
    tasks <- get_tasks()
    last_detect_end <- tasks$alert$end_on
    if(is.na(last_detect_end) || as.numeric(Sys.time() - last_detect_end, unit = "hours") >= (2 + conf$schedule_span / 60) ) {
      alerts <- append(alerts, paste("The alerts tasks has not been finished since", as.integer(as.numeric(Sys.time() - last_detect_end, unit = "hours")), "hours"))
    }

    # Check 05: if any task is on aborted status
    for(i in 1:length(tasks)) {
      if(tasks[[i]]$status == "aborted") {
        alerts <- append(alerts, paste("Task,", tasks[[i]]$task, "is on aborted status"))
      }
    }
    if(send_mail && length(alerts) > 1 && !is.null(conf$admin_email) && conf$admin_email != "" && conf$smtp_host != "") {
      # creating the message to send
      msg <- ( 
        emayili::envelope() %>% 
        emayili::from(conf$smtp_from) %>% 
        emayili::to(conf$admin_email) %>% 
        emayili::subject("Epitweetr may not be properly running") %>% 
        emayili::html(
          paste(
            "<p>The following errors were found during epitweetr health check, please check your epitweetr installation</p><p><ul><li>", 
            paste(alerts, collapse = "</li><li>"),
            "</li></ul></p>"
          )
        )
      )
      smtp <- ( 
        if(is.na(conf$smtp_password) || is.null(conf$smtp_password) ||  conf$smtp_password == "") 
          emayili::server(host = conf$smtp_host, port=conf$smtp_port, insecure=conf$smtp_insecure, reuse = FALSE)
        else 
          emayili::server(host = conf$smtp_host, port=conf$smtp_port, username=conf$smtp_login, insecure=conf$smtp_insecure, password=conf$smtp_password, reuse = FALSE)
      )
      message("Health check failed, sending email")
      smtp(msg)
      checks$last_check <- Sys.time()
    }
  }
  alerts
}

