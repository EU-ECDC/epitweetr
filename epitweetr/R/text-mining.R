#' Load stop words
#' @export
get_stop_words <- function(language_code) {
  #If language stopwords are already cached we retrun them
  stop_id <- paste("stop_words", language_code, sep  = "_")
  if(exists(stop_id, where = cached)) {
    return (cached[[stop_id]])
  }
  else {
    for(i in 1:length(conf$languages)) {
      if(conf$languages[[i]]$code == language_code && file.exists(conf$languages[[i]]$vectors)) {
        con<-gzfile(conf$languages[[i]]$vectors) 
        fixed <- c("RT", "http", "https", "t.co", "amp") 
        stop_words <- tail(gsub(" .*", "", readLines(con, n = 500, encoding="UTF-8")), -1)
        close(con)
        if(length(stop_words) > 0) {
          stop_words <- c(fixed, stop_words)
          cached[[stop_id]] <- stop_words
          return(stop_words)
        } else {
          return(vector(mode="character", length=0))
        }
      }
    }
  }
}

#' calculate top words for a text dataframe
pipe_top_words <- function(df, text_col, lang_col, group_by, max_words = 1000, con_out, page_size) {
  `%>%` <- magrittr::`%>%`
  # Tokenisation and counting top maw_words popular words
  # the count will be done separately for each present group
  groups <- unique(data.frame(df[, group_by]))
  colnames(groups) <- group_by
  if(nrow(groups) > 0) {
    for(i in 1:nrow(groups)) {
      in_group <- df[sapply(1:nrow(df), function(j) paste(df[j, group_by], collapse = "||") == paste(groups[i,],collapse = "||")), ]
      wc <- (
        Reduce( #Using Language based stop words
          x = lapply(conf$languages, function(l) {(
                in_group
                   %>% dplyr::filter((!!as.symbol(lang_col)) == l$code)
                   %>% tidytext::unnest_tokens(tokens, (!!as.symbol(text_col)), drop = TRUE, stopwords=get_stop_words(l$code)) 
             )})
          , f = function(a, b) dplyr::bind_rows(a, b)
          )
          %>% dplyr::group_by(tokens) 
          %>% dplyr::summarize(count = dplyr::n()) 
          %>% dplyr::ungroup() 
          %>% dplyr::arrange(-count) 
          %>% head(max_words)
      )
      # Adding the grouping vars on dataframe
      for(k in 1:length(group_by))
        wc[[group_by[[k]]]] <- groups[i, k] 
      # Saving the data to JSON out file 
      jsonlite::stream_out(wc, con_out, pagesize = page_size, verbose = FALSE) 
    }
  }
}

