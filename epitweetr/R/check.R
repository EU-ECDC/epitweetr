
# check if java is installed
check_java_present <- function() {
  programs <- "java"
  # If java home is not set cheking if the system can found java on the path
  if(Sys.getenv("JAVA_HOME") == "" || is.null(Sys.getenv("JAVA_HOME"))) {
    length(grepl(programs, Sys.which(programs))) == length(programs)  
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
  if(.Platform$OS.type == "windows")
    stop("Non implemented")
  else {
    con <- pipe(paste(java_path(), "-version 2>&1"))
    jver <- readLines(con)
    close(con) 
    jver
  }
}

# check if java is 64 bits
check_java_64 <- function() {
  any(grepl("64-Bit", get_java_version_vector()))
}

# get java version
get_java_version <- function() {
  jver <- get_java_version_vector()
  index <- grep("version", get_java_version_vector())
  if(length(index)== 0)
    NULL
  else {
    j_version_parts <- strsplit(gsub("[a-z A-Z\"]", "", jver[[index[[1]]]]), "\\.")[[1]]
    p1 <- as.integer(j_version_parts[[1]])
    p2 <- as.integer(j_version_parts[[2]])
    if(p1 == 1)
      p2
    else
      p1
  }

}

java_min_version <- function() 8
java_max_version <- function() 11
# check java version
check_java_version <- function() {
  java_version <- get_java_version() 
  if(is.null(java_version)) {
    warning("epitweetr cannot identify your java version")
    FALSE
  } else if(java_version >= java_min_version() && java_version <= java_max_version()) {
    TRUE  
  } else {
    warning(paste("Your current java version is", java_version, " epitweetr needs java to be between ", java_min_version(), " and ", java_max_version()))
    FALSE  
  }
}


# check if aggregated files are present (aggregate has successfully run)
check_series_present <- function(series = c("country_counts", "geolocated", "topwords")) { 
   counts <- sapply(series, function(ds) length(list.files(path = file.path(conf$data_dir, "series"), recursive=TRUE, pattern = paste(ds, ".*\\.Rds", sep="")))) 
   all(counts > 0)
} 

