json2lucene <- function(chunk_size = 1000) {
  stop_if_no_config(paste("Cannot migrate if no configuration is setup")) 
  topics <- list.files(get_search_path())
  total_size <- get_folder_size(get_search_path())
  read_size <- 0
  total_lines <- 0
  line_size = 1000
  start.time <- Sys.time()
  for(i in 1:length(topics)) {
    files <- list.files(file.path(get_search_path(), topics[[i]]), recursive=T, full.names=T)
    for(j in 1:length(files)) {
      if(grepl("\\.gz$", files[j])) {
        file_size <- file.size(files[[j]])
        con <- gzfile(files[j], open = "rb")
        lines <- readLines(con = con, n = chunk_size)
        file_lines <- 0
        while(length(lines) > 0) {
          store_v1_search(lines)
          total_lines <- total_lines + length(lines)
          file_lines <- file_lines + length(lines)
          estimate_read_size <- read_size + file_lines * line_size   
          time.taken <-  as.numeric(difftime(Sys.time(), start.time, units='secs'))
          cat(paste0(
            "\r", round(100*estimate_read_size/total_size, 2), "%. ", 
            round(estimate_read_size/(1024 * 1024)), "/" , round(total_size /(1024 * 1024)), "MB", 
            " ", round(read_size /(1024*1024*time.taken), 2), "MB/sec",
            " expecting to finish on ", Sys.time() + (time.taken * (total_size - estimate_read_size))/estimate_read_size,
            ". Processing '", topics[[i]], "' "
          ))
          lines = readLines(con = con, n = chunk_size)
        }
        close(con)
        read_size <- read_size + file_size
        line_size <- read_size / total_lines  
      }
    }
  }
}

get_folder_size <- function(path) {
  files <-list.files(path,full.names = T, recursive = T, include.dirs = F)
  return( sum(file.size(files)))
}

store_v1_search <- function(lines) {
  for(i in 1:length(lines)) {
    migration_log(lines[i])
    post_result <- httr::POST(url="http://localhost:8080/tweets", httr::content_type_json(), body=lines[i], encode = "raw", encoding = "UTF-8")
    print(post_result)
    migration_log(httr::content(post_result, "text"))
    print(substring(httr::content(post_result, "text"), 1, 100))
    stop()
  }
}

migration_log <- function(lines) {
  fileConn<-file(file.path(conf$data_dir, "migration.json"), open = "w", encoding = "UTF-8")
  writeLines(lines, fileConn)
  close(fileConn)
}
