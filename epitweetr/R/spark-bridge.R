java_path <-  function() {
  if(Sys.getenv("JAVA_HOME")=="") 
    "java"
  else if(.Platform$OS.type == "windows")
    paste("\"", Sys.getenv("JAVA_HOME"), "\\bin\\" , "java\"", sep = "")
  else
    paste("\"", Sys.getenv("JAVA_HOME"), "/bin/" , "java\"", sep = "")
}

#' getting a string for passing  languages as paramater for system calls
conf_languages_as_arg <- function() {
  paste(
    "langCodes \"", paste(lapply(conf$languages, function(l) l$code), collapse = ","), "\" ",
    "langNames \"", paste(lapply(conf$languages, function(l) l$name), collapse = ","), "\" ",
    "langPaths \"", paste(lapply(conf$languages, function(l) l$vectors), collapse = ","), "\" ",
    sep = ""
  )
}

#' getting a string for passing geonames as paramater for system calls
conf_geonames_as_arg <- function() {
  return(
    paste(
      "geonamesSource", paste("\"", file.path(conf$data_dir, "geo", "allCountries.txt"), "\"", sep = "")
      , "geonamesDestination", paste("\"", paste(conf$data_dir, "geo", sep="/"), "\"", sep = "")
    )
  )
}


#' making a spark call via system call
spark_job <- function(args) {
  cmd <- paste(
    if(.Platform$OS.type != "windows") { 
      "export OPENBLAS_NUM_THREADS=1"
    }
    else {
      Sys.setenv(OPENBLAS_NUM_THREADS=1, HADOOP_HOME=system.file("hadoop", package = get_package_name()))
      "" 
    }
    ,paste(
      java_path() 
      , paste("-Xmx", conf$spark_memory, sep = "")
      , paste(" -jar", system.file("java", "ecdc-twitter-bundle-assembly-1.0.jar", package = get_package_name()))
      , args
    )
    ,sep = if(.Platform$OS.type == "windows") '\r\n' else '\n'
  )

  message(cmd)
  system(cmd)
}


#' getting a spark data frame by piping a system call
spark_df <- function(args, handler = NULL) {
  half_mem <- paste(ceiling(as.integer(gsub("[^0-9]*", "", conf$spark_memory))/2), gsub("[0-9]*", "", conf$spark_memory), sep = "")
  cmd <- paste(
    if(.Platform$OS.type != "windows") 
      "export OPENBLAS_NUM_THREADS=1"
    else {
      Sys.setenv(OPENBLAS_NUM_THREADS=1, HADOOP_HOME=system.file("hadoop", package = get_package_name()))
      "" 
    }
   ,paste(
      paste(
        if(.Platform$OS.type == "windows") "call "
	, java_path()
	, sep = ""
       )
     , "-Dfile.encoding=UTF8"
     , paste("-Xmx", half_mem, sep = "")
     , paste(" -jar", system.file("java", "ecdc-twitter-bundle-assembly-1.0.jar", package = get_package_name()))
     , args
    )
    ,sep = '\n'
  )
  message(cmd) 
  con <- pipe(cmd, encoding = "UTF-8")
  if(is.null(handler)) {
    jsonlite::stream_in(con, pagesize = 10000, verbose = TRUE)
  } else {
    tmp_file <- tempfile(pattern = "epitweetr", fileext = ".json")
    #message(tmp_file)
    con_tmp <- file(tmp_file, open = "w", encoding = "UTF-8") 
    jsonlite::stream_in(con, pagesize = 10000, verbose = TRUE, function(df) handler(df, con_tmp))
    close(con_tmp)
    con_tmp <- file(tmp_file, open = "r", encoding = "UTF-8") 
    ret <- jsonlite::stream_in(con_tmp, pagesize = 10000, verbose = TRUE)
    close(con_tmp)
    unlink(tmp_file)
    ret
  }
}
