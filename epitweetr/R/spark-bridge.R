java_path <-  function() {
  if(Sys.getenv("JAVA_HOME")=="") 
    "java"
  else if(.Platform$OS.type == "windows")
    paste("\"", Sys.getenv("JAVA_HOME"), "\\bin\\" , "java\"", sep = "")
  else
    paste("\"", Sys.getenv("JAVA_HOME"), "/bin/" , "java\"", sep = "")
}
set_blas_env <- function() {
  if(.Platform$OS.type == "windows") {
    if(conf$use_mkl && file.exists("C:/Program Files (x86)/IntelSWTools/compilers_and_libraries/windows/redist/intel64_win/mkl")) {
      mkl_path = "C:\\Program Files (x86)\\IntelSWTools\\compilers_and_libraries\\windows\\redist\\intel64_win\\mkl"
      Sys.setenv(PATH = paste(mkl_path, Sys.getenv("PATH"), sep = ";"))
    }
  }
}

get_blas_java_opt <- function() {
  if(.Platform$OS.type == "windows") {
    if(conf$use_mkl && file.exists("C:/Program Files (x86)/IntelSWTools/compilers_and_libraries/windows/redist/intel64_win/mkl")) {
      "-Dcom.github.fommil.netlib.NativeSystemBLAS.natives=mkl_rt.dll"
    } else ""
  }
  else ""

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
      , "geonamesSimplify", paste("\"", conf$geonames_simplify, "\"", sep = "")
    )
  )
}


#' making a spark call via system call
spark_job <- function(args) {
  set_blas_env()
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
      , paste(get_blas_java_opt())
      , paste(" -jar", system.file("java", "ecdc-twitter-bundle-assembly-1.0.jar", package = get_package_name()))
      , args
    )
    ,sep = if(.Platform$OS.type == "windows") '\r\n' else '\n'
  )

  #message(cmd)
  system(cmd)
}


#' getting a spark data frame by piping a system call
spark_df <- function(args, handler = NULL) {
  set_blas_env()
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
     , paste(get_blas_java_opt())
     , paste(" -jar", system.file("java", "ecdc-twitter-bundle-assembly-1.0.jar", package = get_package_name()))
     , args
    )
    ,sep = '\n'
  )
  #message(cmd) 
  con <- if(.Platform$OS.type == "windows") {
    t <- tempfile()
    tcmd <- paste(cmd, shQuote(t), sep=" >")
    shell(tcmd)
    file(t, encoding = "UTF-8")
  } else {
    pipe(cmd, encoding = "UTF-8")
  }
  df <- if(is.null(handler)) {
    jsonlite::stream_in(con, pagesize = 10000, verbose = FALSE)
  } else {
    tmp_file <- tempfile(pattern = "epitweetr", fileext = ".json")
    #message(tmp_file)
    con_tmp <- file(tmp_file, open = "w", encoding = "UTF-8") 
    jsonlite::stream_in(con, pagesize = 10000, verbose = FALSE, function(df) handler(df, con_tmp))
    close(con_tmp)
    con_tmp <- file(tmp_file, open = "r", encoding = "UTF-8") 
    ret <- jsonlite::stream_in(con_tmp, pagesize = 10000, verbose = FALSE)
    close(con_tmp)
    unlink(tmp_file)
    ret
  }
  if(.Platform$OS.type == "windows") unlink(t)
  df
}
