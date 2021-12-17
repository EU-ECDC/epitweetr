


# get the Java path using JAVA_HOME if set
java_path <-  function() {
  if(Sys.getenv("JAVA_HOME")=="") 
    "java"
  else if(.Platform$OS.type == "windows")
    paste("\"", Sys.getenv("JAVA_HOME"), "\\bin\\" , "java\"", sep = "")
  else
    paste("\"", Sys.getenv("JAVA_HOME"), "/bin/" , "java\"", sep = "")
}

# Setting the blas environment variables depending on chosen configuration and OS
set_blas_env <- function() {
  if(.Platform$OS.type == "windows") {
    if(conf$use_mkl && file.exists("C:/Program Files (x86)/IntelSWTools/compilers_and_libraries/windows/redist/intel64_win/mkl")) {
      mkl_path = "C:\\Program Files (x86)\\IntelSWTools\\compilers_and_libraries\\windows\\redist\\intel64_win\\mkl"
      Sys.setenv(PATH = paste(mkl_path, Sys.getenv("PATH"), sep = ";"))
    }
  }
}

# Getting Java options for using MKL BLAS if setup
get_blas_java_opt <- function() {
  if(.Platform$OS.type == "windows") {
    if(conf$use_mkl && file.exists("C:/Program Files (x86)/IntelSWTools/compilers_and_libraries/windows/redist/intel64_win/mkl")) {
      "-Dcom.github.fommil.netlib.NativeSystemBLAS.natives=mkl_rt.dll"
    } else ""
  }
  else ""

}

# getting a string for passing  languages as paramater for system calls
conf_languages_as_arg <- function() {
  paste(
    "langCodes \"", paste(lapply(conf$languages, function(l) l$code), collapse = ","), "\" ",
    "langNames \"", paste(lapply(conf$languages, function(l) l$name), collapse = ","), "\" ",
    "langPaths \"", paste(lapply(conf$languages, function(l) l$vectors), collapse = ","), "\" ",
    sep = ""
  )
}

# getting a string for passing geonames as paramater for system calls
conf_geonames_as_arg <- function() {
  return(
    paste(
      "geonamesSource", paste("\"", file.path(conf$data_dir, "geo", "allCountries.txt"), "\"", sep = "")
      , "geonamesDestination", paste("\"", paste(conf$data_dir, "geo", sep="/"), "\"", sep = "")
      , "geonamesSimplify", paste("\"", conf$geonames_simplify, "\"", sep = "")
    )
  )
}


# making a spark call via system call withouth returnuing any value
spark_job <- function(args) {
  # Setting BLAS environmebt variables
  set_blas_env()
  cmd <- paste(
    if(.Platform$OS.type != "windows") { #Default non windows environment variables within the command 
      "export OPENBLAS_NUM_THREADS=1"
    }
    else {
      Sys.setenv(OPENBLAS_NUM_THREADS=1, HADOOP_HOME=get_winutils_hadoop_home_path()) #Default windows environment variables
      "" 
    }
    ,paste(
      java_path() #Java executable
      , paste(
          "-cp \"", 
          get_app_jar_path(), #epitweetr embededd jar path with
          if(.Platform$OS.type != "windows") ":" else ";",
          get_jars_dest_path(),  #Java class path with all dependencies
          "/*\"", 
          sep=""
        )
      , paste("-Xmx", conf$spark_memory, sep = "") #memory limits
      , paste(get_blas_java_opt()) #Java blas specific options
      , "org.ecdc.twitter.Tweets" #Java scala main class
      , args #arguments to the call
    )
    ,sep = if(.Platform$OS.type == "windows") '\r\n' else '\n'
  )

  #message(cmd) # uncomment this to see the command sent
  res <- system(cmd)
  if(res != 0)
    stop(paste("Error encountered while exeuting: ", cmd))
}


# getting a spark data frame by piping a system call
spark_df <- function(args, handler = NULL) {
  # Setting BLAS environmebt variables
  set_blas_env()
  # Setting the memory to the half of allocated since R will use memory for loading the dataframe
  half_mem <- paste(ceiling(as.integer(gsub("[^0-9]*", "", conf$spark_memory))/2), gsub("[0-9]*", "", conf$spark_memory), sep = "")
  cmd <- paste(
    if(.Platform$OS.type != "windows")  #Default non windows environment variables 
      "export OPENBLAS_NUM_THREADS=1"
    else {
      Sys.setenv(OPENBLAS_NUM_THREADS=1, HADOOP_HOME=get_winutils_hadoop_home_path()) # Setting windowd default environment variables
      "" 
    }
   ,paste(
      paste(
        if(.Platform$OS.type == "windows") "call " else "", 
        java_path(), # Java executable
        sep = ""
       ), 
      paste(
        "-cp \"", 
        get_app_jar_path(),  #epitweetr embededd jar path with
        if(.Platform$OS.type != "windows") ":" else ";",
        get_jars_dest_path(), #Java class path with all dependencies
        "/*\"", 
        sep=""
      ),
      "-Dfile.encoding=UTF8", #Setting Java encoding to UTF-8
      paste("-Xmx", half_mem, sep = ""), #Setting Java memory
      paste(get_blas_java_opt()),
      "org.ecdc.twitter.Tweets", #epitweetr scala main class
      args
    )
    ,sep = '\n'
  )
  #message(cmd) # uncomment this to see the command sent
  con <- if(.Platform$OS.type == "windows") {
    # on windows we send the results to a temporary files
    t <- tempfile()
    tcmd <- paste(cmd, shQuote(t), sep=" >")
    shell(tcmd)
    file(t, encoding = "UTF-8")
  } else {
    # on other OS we get the results directly from the command line
    pipe(cmd, encoding = "UTF-8")
  }
  # here we have a connection ro read returning json lines 
  df <- if(is.null(handler)) {
    #If no custom transformation will be done on data we pass the json lines to jsonlite to interprete it as an R dataframe
    jsonlite::stream_in(con, pagesize = 10000, verbose = FALSE)
  } else {
    # If a custom transfotmation is going to be done (a handler function has been set)
    # this function will be calles on pages of 10k lines using the jsonlite stream_in function
    # the transformed results will be stored on a temporary file that will be read by stream_in
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

#' @title Updates Java dependencies
#' @description Download Java dependencies of the application mainly related to Apache SPARK and Lucene,  
#' @param tasks Task object for reporting progress and error messages, default: get_tasks()
#' @return The list of tasks updated with produced messages
#' @details Run a one shot task consisting of downloading Java and Scala dependencies, this is separated by the following subtasks
#' \itemize{
#'   \item{Download jar dependencies from configuration maven repo to project data folder. This includes, scala, spark, lucene. Packages to be downloaded are defined in package file 'sbt-deps.txt'}
#'   \item{Download winutils from configuration url to project data folder. For more details on winutils please see 
#'     \url{https://issues.apache.org/jira/browse/HADOOP-13223} and \url{https://issues.apache.org/jira/browse/HADOOP-16816}
#'   }
#' }
#'
#' The URLs to download the JAR dependencies (maven package manager) and Winutils are on the configuration tab of the Shiny app.
#'
#' Normally this function is not called directly by the user but from the \code{\link{detect_loop}} function.
#' @examples 
#' if(FALSE){
#'    library(epitweetr)
#'    # setting up the data folder
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'
#'    # geolocating last tweets
#'    tasks <- download_dependencies()
#' }
#' @rdname download_dependencies
#' @seealso
#'  \code{\link{detect_loop}}
#'
#'  \code{\link{get_tasks}}
#'  
#' @export 
#' @importFrom utils download.file 
download_dependencies <- function(tasks = get_tasks()) {
  tasks <- tryCatch({
    tasks <- update_dep_task(tasks, "running", "getting sbt dependencies", start = TRUE)
    # downloading SPARK, lucene and other scala dependencies from the provided maven repository 
    while(is_fs_running()) {
      tasks <- update_dep_task(tasks, "running", paste(
        "Embeded database is running, please turn it OFF in order to download dependencies. You can do this on the task scheduler on windows or manually kill the process on other platforms."
      ))
      Sys.sleep(5)
    } 
    tasks <- download_sbt_dependencies(tasks)
    if(.Platform$OS.type == "windows") {
      tasks <- update_dep_task(tasks, "running", "getting winutils")
      # on windows, downloading winutils from the URL set from the shiny app which is a prerequisite of SPARK
      tasks <- download_winutils()
    }
    # ensuring that storage system is running
    while(!is_fs_running()) {
      tasks <- update_dep_task(tasks, "running", paste(
        "Embeded database is NOT running. On Windows, you can activate it by clicking on the 'activate' database service button on the config page ",
        "You can also manually run the fs service by executing the following command on a separate R session. epitweetr::fs_loop('",
        conf$data_dir,
        "')"
      ))
      Sys.sleep(5)
    } 
    # running migration if necessary 
    tasks <- update_dep_task(tasks, "running", "migrating any old json files to embeded database")
    tasks <- json2lucene(tasks)

    # Setting status to succes
    tasks <- update_dep_task(tasks, "success", "", end = TRUE)
    tasks
  }, error = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$dependencies$message," :", error_condition))
    tasks <- update_dep_task(tasks, "failed", paste("failed while", tasks$dependencies$message," getting dependencies ", error_condition))
    tasks
  }, warning = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$dependencies$message," ", error_condition))
    tasks <- update_dep_task(tasks, "failed", paste("failed while", tasks$dependencies$message," getting dependencies ", error_condition))
    tasks
  })
  return(tasks)
}

# Download SBT dependencies from maven (scala, spark, lucene, netlib (BLAS), httpclient) into package folder
download_sbt_dependencies <- function(tasks = get_tasks()) {
  if(!exists("maven_repo", where = tasks$dependencies)) stop("Before running detect loop you have to manually update 'Java/Scala dependencies' to set the used repository")
  # reading the dependencies to download from embeded list
  con <- file(get_sbt_file_dep_path())
  deps <- readLines(con)
  close(con)
  
  # transforming the dependencies listed into maven URLs there are two possible formats that will be stored on urls and urls2
  parts <- strsplit(deps, "/")
  versions <- sapply(parts, function(p) {l <- nchar(p[[4]]); substr(p[[5]], l + 2, nchar(p[[5]]))})
  versions <- gsub("\\.([^\\.]*)$", "", versions)
  versions2 <- gsub("\\-([^\\-]*)$", "", versions)
  urls <- paste(tasks$dependencies$maven_repo , gsub("\\.", "/", sapply(parts, `[[`, 3)), sapply(parts, `[[`, 4), versions, sapply(parts, `[[`, 5), sep = "/")
  urls2 <- paste(tasks$dependencies$maven_repo , gsub("\\.", "/", sapply(parts, `[[`, 3)), sapply(parts, `[[`, 4), versions2, sapply(parts, `[[`, 5), sep = "/")
  
  #Creating destination directory
  if(!dir.exists(get_jars_dest_path())) 
    dir.create(get_jars_dest_path()) 
  else {
    #Deleting existing files
    existing <- list.files(get_jars_dest_path(), full.names=TRUE)
    tryCatch({
      file.remove(existing)
    }, warning = function(c) {
      warning(paste("Warning during removing JAR files in ", get_jars_dest_path(), c))
    }, error = function(c) {
      stop(paste("EPitweetr cannot remove JAR fils files in ", get_jars_dest_path(), " please remove the files manually.", c))
    })
  }

  # destination file
  dest_file <- file.path(get_jars_dest_path(), paste(sapply(parts, `[[`, 3), sapply(parts, `[[`, 4), sapply(parts, `[[`, 5), sep = "_"))
  
  # iterating over all files 
  for(i in 1:length(urls)) {
    tryCatch({
      tasks <- update_dep_task(tasks, "running", paste("downloading", urls[[i]]))
      # downloading files on first format 
      download.file(url = urls[[i]], destfile = dest_file[[i]], mode = "wb")
    }
    ,warning = function(c) {
      tasks <- update_dep_task(tasks, "running", paste("downloading", urls2[[i]]))
      # downloading files on secong format 
      download.file(url = urls2[[i]], destfile = dest_file[[i]], mode = "wb")
    }
    ,error = function(c) {
      tasks <- update_dep_task(tasks, "running", paste("downloading", urls2[[i]]))
      download.file(url = urls2[[i]], destfile = dest_file[[i]], mode = "wb")
    })
  }
  return(tasks)
}

# Download winutils (necessary for spark on Windows, 
# for more details on winutils please see https://issues.apache.org/jira/browse/HADOOP-13223 and https://issues.apache.org/jira/browse/HADOOP-16816
download_winutils <- function(tasks = get_tasks()) {
  if(!dir.exists(get_winutils_hadoop_home_path())) dir.create(get_winutils_hadoop_home_path())
  if(!dir.exists(file.path(get_winutils_hadoop_home_path(), "bin"))) dir.create(file.path(get_winutils_hadoop_home_path(), "bin"))
  if(!exists("winutils_url", where = tasks$dependencies)) stop("Before running detect loop you have to manually activate 'Java/Scala dependencies' to set the winutils to use")
  tasks <- update_dep_task(tasks, "running", paste("downloading", tasks$dependencies$winutils_url))
  download.file(url = tasks$dependencies$winutils_url, destfile = get_winutils_path(), mode = "wb")
  return(tasks)
}



