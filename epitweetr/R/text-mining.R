# Load stop words from language files which will be a fixed set of twitter stop words plus first 500 more popular words on language
get_stop_words <- function(language_code) {
  #If language stopwords are already cached we rerun them
  stop_id <- paste("stop_words", language_code, sep  = "_")
  if(exists(stop_id, where = cached)) {
    return (cached[[stop_id]])
  }
  else {
    for(i in 1:length(conf$languages)) {
      if(conf$languages[[i]]$code == language_code && file.exists(conf$languages[[i]]$vectors)) {
        con<-gzfile(conf$languages[[i]]$vectors) 
        fixed <- c("rt", "RT", "http", "https", "t.co", "amp", "via") 
        stop_words <- tail(gsub(" .*", "", readLines(con, n = 500, encoding="UTF-8")), -1)
        close(con)
        if(length(stop_words) > 0) {
          stop_words <- c(fixed, stop_words)
          stop_words <- stop_words[grep("'|\\\\", stop_words, invert = TRUE)]
          cached[[stop_id]] <- stop_words
          return(stop_words)
        } else {
          return(vector(mode="character", length=0))
        }
      }
    }
  }
}

# calculate top words for a text data frame this function is being used for calculating topwords on chunks of tweets
# the connection is the target file to write the results into
pipe_top_words <- function(df, text_col, lang_col, topic_word_to_exclude, max_words = 1000, con_out, page_size) {
  `%>%` <- magrittr::`%>%`
  # Tokenisation and counting top max_words popular words
  # the count will be done separately for each present group

  if(!("tweet_geo_country_code" %in% colnames(df)))
    df$tweet_geo_country_code <- NA
  
  wc <- if(nrow(df)==0) {
   data.frame(topic = character(), created_date=character(), tweet_geo_country_code=character(), tokens=character(), count=numeric(), original=numeric(), retwets=numeric())
  } else {
    # making a language by language union using tidytext unnest_tokens function passing languages specific topwords 
    temp <- Reduce( #Using Language based stop words
      x = lapply(conf$languages, function(l) {(
            df %>% 
              dplyr::filter((!!as.symbol(lang_col)) == l$code) %>%
              tidytext::unnest_tokens(.data$tokens, (!!as.symbol(text_col)), drop = TRUE, stopwords=get_stop_words(l$code))
         )})
      , f = function(a, b) dplyr::bind_rows(a, b)
      )

    # making the group by count and top
    temp %>% 
      dplyr::filter(nchar(.data$tokens)>1 & !(paste(.data$topic, .data$tokens, sep="_") %in% topic_word_to_exclude))  %>%
      dplyr::group_by(.data$tokens, .data$topic, .data$created_date, .data$tweet_geo_country_code)  %>%
      dplyr::summarize(count = dplyr::n(), original = sum(!.data$is_retweet), retweets = sum(.data$is_retweet))  %>%
      dplyr::ungroup()  %>%
      dplyr::group_by(.data$topic, .data$created_date, .data$tweet_geo_country_code)  %>%
      dplyr::top_n(n = max_words, wt = .data$count) %>%
      dplyr::ungroup() 
  }
  # Saving the data to JSON out file
  jsonlite::stream_out(wc, con_out, pagesize = page_size, verbose = FALSE)
}

