#' @title Function used for migrating tweets from to old to the new file system
#' @description migrates geolocated tweets from the old to the new file system allowing full text search using Apache Lucene Indexes
#' @param tasks named list, current tasks for logging and updating progress default: get_tasks()
#' @param chunk_size, integer, the chunk size for indexing tweets, default: 400
#' @return the updated tasks.
#' @details This function can be called manually to perform the migration of tweets between v0.0.x to v2+
#' It iterates over existing tweets collected with epitweetr v0.0.x series
#' joins base tweets and geolocated tweets and then sends themes to the Lucene index via the dedicated REST API.
#' Migrated files will be moved to search_archive and geo_archive folders. Users can backup and remove these folders when migration ends to gain disk space.
#' Series folders are maintained for migrated tweets
#' @examples
#' if(FALSE){
#'    library(epitweetr)
#'    # setting up the data folder
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose()) 
#'    # runnint the migration
#'    json2lucene()
#' }
#' @rdname json2lucene
#' @export
json2lucene <- function(tasks = get_tasks(), chunk_size = 400) {
  stop_if_no_config(paste("Cannot migrate if no configuration is setup")) 
    
  if(!file.exists(get_search_archive_path())) dir.create(get_search_archive_path(), recursive = TRUE)
  total_size <- get_folder_size(get_search_path())
  read_size <- 0
  total_lines <- 0
  line_size = 70000 #Empiric observation set as initial value in bytes. It will be recalculated later
  start.time <- Sys.time()
  tasks <- update_dep_task(tasks, "running", "Scanning dates to migrate")
  dates <- sort(unique(unlist(c(
    get_file_names(get_search_path(), pattern = "[0-9][0-9][0-9][0-9]\\.[0-9][0-9]\\.[0-9][0-9].*\\.json\\.gz$"),
    get_file_names(get_geo_path(), pattern = "[0-9][0-9][0-9][0-9]\\.[0-9][0-9]\\.[0-9][0-9].*\\.json\\.gz$")
  ))))
  for(i in 1:length(dates)) {
    while(is_search_running()) {
      tasks <- update_dep_task(tasks, "running", paste(
        "Search loop is currently running. Migrating from previous version storage is not supported while search loop is running. Please deactivate it"
      ))
      Sys.sleep(5)
    }
    search_files <- list.files(get_search_path(), recursive=T, full.names=T, pattern = paste0(gsub("\\.", "\\\\.", dates[[i]]), "$"))
    geo_dirs <- list.files(get_geo_path(), recursive=T, full.names=T, pattern = paste0(gsub("\\.", "\\\\.", dates[[i]]), "$"), include.dirs = T) 
    geo_files <- list.files(geo_dirs, recursive=T, full.names=T) 
    tasks <- update_dep_task(tasks, "running", paste("Scanning files for ", dates[[i]]))
    files <- c(
      lapply(search_files, function(f) list(type="search", path=f, topic = tail(strsplit(f, "/")[[1]], n=3)[[1]]))
      ,lapply(geo_files, function(d) list(type="geo", path=d))
    )
    created_dates = c()
    if(length(files) > 0) {
      tasks <- update_dep_task(tasks, "running", paste("Iterating over ", length(files), "files "))
      for(j in 1:length(files)) {
        file_size <- file.size(files[[j]]$path)
        con <- gzfile(files[[j]]$path, open = "rb")
        read_time <- 0
        lines <- readLines(con = con, n =  if(files[[j]]$type == "geo") 100 *  chunk_size else chunk_size)
        file_lines <- 0
        last_commit <- Sys.time()
        while(length(lines) > 0) {
          post_time <- Sys.time()
          if(files[[j]]$type == "search")
            created_dates = unique(c(created_dates, store_v1_search(lines, files[[j]]$topic)))
          if(files[[j]]$type == "geo" && length(created_dates) > 0) {
             store_geo(lines, created_dates, chunk_size = chunk_size)
          }
          post_time <- as.numeric(difftime(Sys.time(), post_time, units='secs'))
          total_lines <- total_lines + length(lines)
          file_lines <- file_lines + length(lines)
          estimate_read_size <- read_size + file_lines * line_size   
          time.taken <-  as.numeric(difftime(Sys.time(), start.time, units='secs'))
          tasks <- update_dep_task(tasks, "running",
            paste0(
              "\r", round(100*estimate_read_size/total_size, 2), "%. ", 
              round(estimate_read_size/(1024 * 1024)), "/" , round(total_size /(1024 * 1024)), "MB", 
              " ", round(read_size /(1024*1024*time.taken), 2), "MB/sec",
              " remaining time ", sec2hms((time.taken * (total_size - estimate_read_size))/estimate_read_size),
              " req/read/index: ", length(lines), "/", round(read_time, 2), "/",round(post_time, 2), " secs", 
              ". Processing '", dates[[i]], " ", 
              if(files[[j]]$type == "search") files[[j]]$topic else "geolocation", "' "
            )
          )
          read_time <- Sys.time()
          lines <- readLines(con = con, n =  if(files[[j]]$type == "geo") 100 *  chunk_size else chunk_size)
          read_time <-  as.numeric(difftime(Sys.time(), read_time, units='secs'))
          # committing each 60 secs
          if(as.numeric(difftime(Sys.time(), last_commit, units = "secs")) > 60) { 
            commit_tweets()
            last_commit <- Sys.time()
          }
        }
        close(con)
        #Committing file tweets
        commit_tweets()

        read_size <- read_size + file_size
        line_size <- read_size / total_lines  
      }

      #After processing all tweets and geolocation for the particular date name, we can safely move the file to the archive folder
      tasks <- update_dep_task(tasks, "running",paste("Archiving files for ", dates[[i]]))
      lapply(files, archive_search_geo_file)
    }
  }
  tasks
}

# Getting file names on a particular folder matching the provided pattern
get_file_names <- function(path, ext, pattern) {
  files <- list.files(file.path(path), recursive=T, full.names=F, include.dirs = T, pattern = pattern)
  names <- unique(sapply(files, function(f) tail(strsplit(f, "/")[[1]], n = 1)))
  names
}

# Get folder size recursively
get_folder_size <- function(path) {
  files <-list.files(path,full.names = T, recursive = T, include.dirs = FALSE)
  return( sum(file.size(files)))
}

# Archive the provided file
archive_search_geo_file <- function(file) {
  x <- file$path
  froms <- c(x, file.path(dirname(x), paste0(".", basename(x), ".crc")),  file.path(dirname(x), paste0("._SUCCESS.crc")), file.path(dirname(x), paste0("._SUCCESS.crc")))
  # renaming if destination files exists already
  for(from in froms) { 
    if(file$type == "search")
      to <- sub("search", 'search_archive', from) 
    else if (file$type == "geo")
      to <- sub("geolocated", 'geo_archive', from)
    else 
      stop("The provided file to archive must be a named list with properties type (geo or search) and path")
    todir <- dirname(to)
    if (!isTRUE(file.info(todir)$isdir)) dir.create(todir, recursive=TRUE)
    if(file.exists(to)) {
      to <-gsub(".json.gz", paste0(floor(stats::runif(1, min=1, max=500)),".json.gz"), to)
    }
    if(file.exists(from))
      file.rename(from = from,  to = to)
  }

  #removing empty folders
  fromdir <- dirname(from)
  while(grepl(paste0('^', get_geo_path()), fromdir) || grepl(paste0('^', get_search_path()), fromdir)) {
    if(dir.exists(fromdir) && length(list.files(fromdir, include.dirs=T)) ==0) {
      message(paste("removing", fromdir))
      unlink(fromdir, recursive = T, force = FALSE)
    }
    fromdir <- dirname(fromdir)
  }
}

# Store the geolocated tweets on the new embedded database
store_geo <- function(lines, created_dates, async = T, chunk_size = 100) {
  valid_index <- unlist(lapply(lines, function(l) jsonlite::validate(l)))
  bad_lines <- lines[!valid_index]
  if(length(bad_lines)>0)
    message(paste(length(bad_lines), "of", length(lines), "bad line(s) found!! These will be ignored"))
  lines <- lines[valid_index] 
  ngroupes <- ceiling(length(lines)/chunk_size)
  #grouping lines into chunks
  chunks <- lapply(1:ngroupes, function(g) lines[sapply(1:length(lines), function(ix) (ix %% (ngroupes)) == (g - 1)) ])
  chunks <- lapply(chunks, function(chunk) paste0("[", paste0(chunk, collapse = ", "), "]"))
  if(length(chunks) > 0) {
    if(async) {
      requests <-lapply(chunks, function(line) { 
        request <- crul::HttpRequest$new(url = paste0(get_scala_geolocated_tweets_url(), "?", paste0("created=", created_dates, collapse = '&')), headers = list(`Content-Type`= "application/json", charset="utf-8"))
        request$post(body = line, encode = "raw")
        request
      })
      async <- crul::AsyncVaried$new(.list = requests)
      async$request()
      status <- async$status_code()
      if(length(status[status == 406])>0)
        message(paste("Storage rejected ", length(status[status == 406]), "geolocation request for not having a valid content"))

      if(length(status[status != 200 & status != 406])>0)
        stop(async$parse()[status != 200 & status != 406])

    } else {
      for( i in 1:length(chunks)) {
        post_result <- httr::POST(url=get_scala_geolocated_tweets_url(), httr::content_type_json(), body=chunks[i], encode = "raw", encoding = "UTF-8")
        if(httr::status_code(post_result) != 200) {
          stop(paste("Storage rejected ", length(status[status == 406]), "geolocation request for not having a valid content"))
        }
      }
    }
  }
}

# Store the tweets collected as JSON data on the new embedded database
store_v1_search <- function(lines, topic, async = T) {
  valid_index <- unlist(lapply(lines, function(l) jsonlite::validate(l)))
  bad_lines <- lines[!valid_index]
  if(length(bad_lines)>0)
    message(paste(length(bad_lines), "of", length(lines), "bad line(s) found!! These will be ignored"))
    
  lines <- lines[valid_index] 
  if(length(lines) > 0) {
    if(async) {
      requests <-lapply(lines, function(line) {
        request <- crul::HttpRequest$new(url = paste0(get_scala_tweets_url(), "?topic=", curl::curl_escape(topic), "&geolocate=false"), headers = list(`Content-Type`= "application/json", charset="utf-8"))
        request$post(body = line, encode = "raw")
        request
      })
      async <- crul::AsyncVaried$new(.list = requests)
      async$request()
      status <- async$status_code()
      if(length(status[status == 406])>0)
        message(paste(length(bad_lines), "of", length(lines), "bad line(s) found!! These will be ignored"))
       

      if(length(status[status != 200 & status != 406])>0)
        stop(async$parse()[status != 200 & status != 406])
      dates <- unique(unlist(lapply(async$parse()[status == 200], function(l) jsonlite::parse_json(l)$dates), recursive = T))
      return(dates)
    } else {
      for( i in 1:length(lines)) {
        post_result <- httr::POST(url=paste0(get_scala_tweets_url(), "?topic=", curl::curl_escape(topic), "&geolocate=false"), httr::content_type_json(), body=lines[i], encode = "raw", encoding = "UTF-8")
        if(httr::status_code(post_result) != 200) {
          stop("running",substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100))
        }
      }
      stop("dealing with dates is not yet implemented")
    }
  }
}

# Commit the changes on the embedded database
commit_tweets <- function() {
  post_result <- httr::POST(url=get_scala_commit_url(), httr::content_type_json(), encode = "raw", encoding = "UTF-8")
  if(httr::status_code(post_result) != 200) {
    stop(print(substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100)))
  }
}

# Print duration in seconds as duration in days, hours, minutes and seconds
sec2hms <- function(seconds) {
  x <- abs(as.numeric(seconds))
  sprintf("%d days %02d:%02d:%02ds", x %/% 86400,  x %% 86400 %/% 3600, x %% 3600 %/% 60,  x %% 60 %/% 1)
}
