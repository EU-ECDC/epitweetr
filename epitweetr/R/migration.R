json2lucene <- function(chunk_size = 400) {
  stop_if_no_config(paste("Cannot migrate if no configuration is setup")) 
  
  if(!file.exists(get_search_archive_path())) dir.create(get_search_archive_path())
  topics <- list.files(get_search_path())
  total_size <- get_folder_size(get_search_path())
  read_size <- 0
  total_lines <- 0
  line_size = 70000 #Empiric observation set as initial value in bytes. It will be fixed later
  start.time <- Sys.time()
  for(i in 1:length(topics)) {
    files <- list.files(file.path(get_search_path(), topics[[i]]), recursive=T, full.names=T)
    if(length(files) > 0) {
      for(j in 1:length(files)) {
        if(grepl("\\.gz$", files[j])) {
          file_size <- file.size(files[[j]])
          con <- gzfile(files[j], open = "rb")
          read_time <- 0
          lines <- readLines(con = con, n = chunk_size)
          file_lines <- 0
          while(length(lines) > 0) {
            post_time <- Sys.time()
            store_v1_search(lines, topics[[i]])
            post_time <- as.numeric(difftime(Sys.time(), post_time, units='secs'))
            total_lines <- total_lines + length(lines)
            file_lines <- file_lines + length(lines)
            estimate_read_size <- read_size + file_lines * line_size   
            time.taken <-  as.numeric(difftime(Sys.time(), start.time, units='secs'))
            cat(paste0(
              "\r", round(100*estimate_read_size/total_size, 2), "%. ", 
              round(estimate_read_size/(1024 * 1024)), "/" , round(total_size /(1024 * 1024)), "MB", 
              " ", round(read_size /(1024*1024*time.taken), 2), "MB/sec",
              " remaining time ", sec2hms((time.taken * (total_size - estimate_read_size))/estimate_read_size),
              " req/read/index: ", length(lines), "/", round(read_time, 2), "/",round(post_time, 2), " secs", 
              ". Processing '", topics[[i]], "' "
            ))

            read_time <- Sys.time()
            lines = readLines(con = con, n = chunk_size)
            read_time <-  as.numeric(difftime(Sys.time(), read_time, units='secs'))
            # commiting each 10k lines (1M tweets)
            if(floor((file_lines + chunk_size)/10000) > floor(file_lines/10000)) {
              commit_tweets()
            }
          }
          close(con)
          #Commiting file tweets
          commit_tweets()
          #After commit we can safely move the file to the archive folder
          archive_search_file(files[[j]])
          read_size <- read_size + file_size
          line_size <- read_size / total_lines  
        }
      }
    }
  }
}

get_folder_size <- function(path) {
  files <-list.files(path,full.names = T, recursive = T, include.dirs = F)
  return( sum(file.size(files)))
}

archive_search_file <- function(file) {
  from <- file
  to <- sub("search", 'search_archive', from) 
  todir <- dirname(to)
  if (!isTRUE(file.info(todir)$isdir)) dir.create(todir, recursive=TRUE)
  if(file.exists(to)) {
    to <-gsub(".json..gz", paste0(floor(runif(1, min=1, max=500)),".json.gz"), to)
  }
  file.rename(from = from,  to = to)

}

store_v1_search <- function(lines, topic, async = T) {
  valid_index <- unlist(lapply(lines, function(l) jsonlite::validate(l)))
  bad_lines <- lines[!valid_index]
  if(length(bad_lines)>0)
    print(paste(length(bad_lines), "of", length(lines), "bad line found!! they will be ignored"))
  lines <- lines[valid_index] 
  if(length(lines) > 0) {
    if(async) {
      requests <-lapply(lines, function(line) {
        request <- crul::HttpRequest$new(url = paste0("http://localhost:8080/tweets?topic=", curl::curl_escape(topic)), headers = list(`Content-Type`= "application/json", charset="utf-8"))
        request$post(body = line, encode = "raw")
        request
      })
      async <- crul::AsyncVaried$new(.list = requests)
      async$request()
      status <- async$status_code()
      if(length(status[status == 406])>0)
        print(paste("Storage rejected ", length(status[status == 406]), "tweet request for not having a valid content"))

      if(length(status[status != 200 & status != 406])>0)
        stop(async$parse()[status != 200 & status != 406])

    } else {
      #migration_log(lines[i])
      for( i in 1:length(lines)) {
        post_result <- httr::POST(url=paste0("http://localhost:8080/tweets?topic=", curl::curl_escape(topic)), httr::content_type_json(), body=lines[i], encode = "raw", encoding = "UTF-8")
        if(httr::status_code(post_result) != 200) {
          migration_log(httr::content(post_result, "text", encoding = "UTF-8"))
          print(substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100))
          stop()
        }
      }
    }
  }
}

commit_tweets <- function() {
  post_result <- httr::POST(url="http://localhost:8080/commit", httr::content_type_json(), encode = "raw", encoding = "UTF-8")
  if(httr::status_code(post_result) != 200) {
    migration_log(httr::content(post_result, "text", encoding = "UTF-8"))
    print(substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100))
    stop()
  }
}
migration_log <- function(lines) {
  fileConn<-file(file.path(conf$data_dir, "migration.json"), open = "w", encoding = "UTF-8")
  writeLines(lines, fileConn)
  close(fileConn)
}

sec2hms <- function(seconds) {
  x <- abs(as.numeric(seconds))
  sprintf("%d days %02d:%02d:%02ds", x %/% 86400,  x %% 86400 %/% 3600, x %% 3600 %/% 60,  x %% 60 %/% 1)
}
