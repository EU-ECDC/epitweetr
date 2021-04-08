json2lucene <- function(maxlines = 10) {
  stop_if_no_config(paste("Cannot migrate if no configuration is setup")) 
  topics <- list.files(get_search_path())
  for(i in 1:length(topics)) {
    print(paste("Migrating topics for", topics[i]))
    total <- 0
    files <- list.files(file.path(get_search_path(), topics[[i]]), recursive=T, full.names=T)
    for(j in 1:length(files)) {
      cat(paste0("\r", round(10000*j/length(files))/100, "%"))
      if(grepl("\\.gz$", files[j])) {
        con <- gzfile(files[j], open = "rb")
        lines = readLines(con = con, n = maxLines)
        total = total + length(lines)
        while(length(lines) > 0) {
          lines = readLines(con = con, n = maxLines)
          total = total + length(lines)
        }
        close(con)
      }
    }
    print(paste(total, "lines found"))
  }
}

