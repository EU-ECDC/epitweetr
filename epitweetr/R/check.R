
# check if java is installed
check_java_present <- function() {
  if(is_java_present())
    TRUE
  else {
    warning(paste(
      "epitweetr could not detect java. Please install it and make sure java binary is on path or that JAVA_HOME is set",
      "epitweetr needs 64bits java between ", java_min_version(), " and ", java_max_version()
    ))
  }
}
# check if java is installed
is_java_present <- function() {
  programs <- "java"
  # If java home is not set cheking if the system can found java on the path
  if(Sys.getenv("JAVA_HOME") == "" || is.null(Sys.getenv("JAVA_HOME"))) {
    length(grep(programs, Sys.which(programs))) == length(programs)  
  } else {
    #checking if java binary can be foung from java_home
    if(.Platform$OS.type == "windows")
      file.exists(file.path(Sys.getenv("JAVA_HOME"), "bin", "java.exe"))
    else
      file.exists(file.path(Sys.getenv("JAVA_HOME"), "bin", "java"))
  }
}

# get java version vector
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

# check if java is 64 bits
check_java_64 <- function() {
  if(any(grepl("64-Bit", get_java_version_vector())))
    TRUE
  else {
    warning(paste("Your current java version does not seem to be 64 bits. epitweetr needs java 64 bits between ", java_min_version(), " and ", java_max_version()))
    FALSE  
  }
}

# get java version
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
# check java version
check_java_version <- function() {
  java_version <- get_java_version() 
  if(is.null(java_version)) {
    warning("epitweetr cannot identify your java version")
    FALSE
  } else if(java_version >= java_min_version() && java_version <= java_max_version()) {
    TRUE  
  } else if(java_version <= java_tested_version()){
    warning(paste(
      "Your current java version is", 
      java_version, 
      ". epitweetr needs java versions between ", 
      java_min_version(), 
      " and ", 
      java_max_version(), 
      ". Some users have reported that your version also works, but is is recommended to use previous compatible java versions"
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


# check msvcr100 on windows machines
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

#check download java dependencies has been executed
check_java_deps <- function() {
  if(!dir.exists(get_jars_dest_path())) {
    warning("epitweetr needs the dependencies task to run and download all java dependencies")
    FALSE 
  } else {
    con <- file(get_sbt_file_dep_path())
    deps <- readLines(con)
    close(con)
    downloaded <- list.files(get_jars_dest_path())
    if(length(deps) != length(downloaded)) {
      warning("epitweetr needs the dependencies task to run and download all java dependencies")
      FALSE 
    } else {
      TRUE  
    }
  }
}

#check geonames is downloaded and indexed
check_geonames <- function() {
  if(!file.exists(get_geonames_txt_path())) {
    warning("epitweetr needs geonames to be downloaded and unziped for geolocating tweets. Please run the geonames task of the detection pipeline")
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
   counts <- sapply(series, function(ds) length(list.files(path = file.path(conf$data_dir, "series"), recursive=TRUE, pattern = paste(ds, ".*\\.Rds", sep="")))) 
   if(all(counts > 0))
     TRUE
   else {
     warning("No aggregated series found. Please execute the aggregate task")  
     FALSE
   }
}

# check if tweets files are present tweet collect has run
check_tweets_present <- function() {
  last_topic <- tail(list.dirs(path = file.path(conf$data_dir, "tweets", "search"), recursive = FALSE), n = 1)
  last_year <- tail(list.dirs(path = last_topic, recursive = FALSE), n = 1)
  last_file <- tail(list.files(path = last_year, pattern = "*.json.gz"), n = 1)
  
  if(length(last_file) > 0) {
    TRUE
  } else {
    warning("No tweet files found. Please execute the search loop")
    FALSE
  }
}

# check if alert files are present
check_alerts_present <- function() {
  last_year <- tail(list.dirs(path = file.path(conf$data_dir, "alerts"), recursive = FALSE), n = 1)
  last_file <- tail(list.files(path = last_year, pattern = "*.json"), n = 1) 
  
  if(length(last_file) > 0) {
    TRUE
  } else {
    warning("No alert files found. Please execute the alerts task")
    FALSE
  }
}

# check if taskScheduleR is installed on windows
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

# check if pandoc is installed
check_pandoc <- function() {
  if(unname(Sys.which("pandoc")) == "") {
    warning("For exporting the PDF report from the dashboard, Pandoc has to be installed.")
    FALSE  
  } else
    TRUE
}

# check if pandoc is installed
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
      "Search loop is not running. On Windows, you can activate it by clicking on the 'activate' Tweet search button on the config page ",
      "You can also manually run the search loop by executing the following command on a separate R session. epitweetr::search_loop('",
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
      "Detection pipeline is not running. On Windows, you can activate it by clicking on the 'activate' detection pipeline button on the config page ",
      "You can also manually run the detection loop by executing the following command on a separate R session. epitweetr::detect_loop('",
      conf$data_dir,
      "')"
    ))
    FALSE
  }
}

# check Twitter authentication
check_twitter_auth <- function() {
  ok <- tryCatch({
    token <- get_token(request_new = FALSE)
    "Token" %in% class(token) || "bearer" %in% class(token)
    }, 
    warning = function(m) {FALSE}, 
    error = function(e) { FALSE }
  ) 
  if(ok)
    
    TRUE 
  else {
    warning("Cannot create a Twitter token, please choose an authentication method on the configuration page")
    FALSE
  }
}

#check manual tasks requesteds
check_manual_task_request <- function() {
  if(
    is.na(conf$dep_updated_on)
    || is.na(conf$geonames_updated_on)
    || is.na(conf$lang_updated_on)
  ) {
    warning("Before running the detect loop, you have to click on 'activate' detection pipeline button on configuration page")
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
      "Please set one of TMPDIR, TMP or TEMP environment variable to the same drive as the data folder",
      conf$data_dir))
    FALSE
  }
}

#' @title Run automatic sanity checks
#' @description run  a set of automated sanity checks for helping the user to troubleqhot issues 
#' @return Dataframe containing the statuses of all realized checks
#' @details This function executes a series of sanity checks, concerninr, java, bitness, task statusn dependencies and Twitter authentication.
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
  checks <- list(
    scheduler = check_scheduler,
    twitter_auth = check_twitter_auth,
    search_running = check_search_running,
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
    geotag = check_geolocated_present, 
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
