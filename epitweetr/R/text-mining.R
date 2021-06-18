# Load stop words from language files which will be a fixed set of twitter stop words plus first 500 more popular words on language
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
        fixed <- c("rt", "RT", "http", "https", "t.co", "amp", "via") 
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

# calculate top words for a text dataframe this function is being used for calculating topwords on chunks of tweets
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

#' @export
# Returns a dataframe with geolocation columns based on a given text column and optionally a language col
geolocate_text <- function(df, text_col = "text", lang_col=NULL, min_score = NULL) {
  `%>%` <- magrittr::`%>%`
  if(is.null(lang_col)) df[[lang_col]] <- NA
  to_geolocate = df %>% 
    dplyr::transmute(id = as.character(dplyr::row_number()), text = .data[[text_col]], lang =.data[[lang_col]] ) %>%
    jsonlite::toJSON()
  geo_uri = paste(get_scala_geolocate_text_url(), "?jsonnl=true", sep = "")
  if(!is.null(min_score)) {
    geo_uri <- paste(geo_uri,"&minScore=", min_score, sep = "")
  }

  ret = stream_post(uri = geo_uri, body = to_geolocate) %>%
    dplyr::arrange(as.integer(.data$id))
  dplyr::select(ret, -which(names(ret) %in% c("text", "id", "lang")))
}


get_training_df <- function(tweets_to_add = 100) {
  data_types<-c("text","text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text")
  current <- readxl::read_excel(get_geo_training_path(), col_types = data_types)
  current$Text <- stringr::str_trim(current$Text)
  current <- current %>% dplyr::distinct(.data$`Text`, .data$Lang, .keep_all = T)
  locations <- current %>% dplyr::filter(Type == "Location" & `Location OK/KO` == "OK")
  add_locations <- if(nrow(locations) == 0) "&locationSamples=true" else "&locationSamples=false" 
  non_locations <- current %>% dplyr::filter(Type == "Location" & `Location OK/KO` == "KO")
  non_location_langs <- unique(non_locations$Lang)
  excl_langs <- if(length(non_location_langs) > 0) paste("&excludedLangs=", paste(non_location_langs, collapse = "&excludedLangs="), sep = "") else "" 

  geo_training_uri <- paste(get_scala_geo_training_url(), "?jsonnl=true", add_locations, excl_langs, sep = "")
  geo_training_url <- url(geo_training_uri)
  geo_training <- jsonlite::stream_in(geo_training_url) 
  if(nrow(geo_training) > 0) {
    geo_training <- geo_training %>%
    dplyr::transmute(
      Type = "Location", 
      `Text` = .data$word, 
      `Location in text` = NA, 
      `Location OK/KO` = ifelse(.data$isLocation, "OK", "KO"), 
      `Associate country code` = NA, 
      `Associate with`=NA, 
      `Source` = "Epitweetr", 
      `Tweet Id` = NA, 
      `Lang` = .data$lang,
      `Tweet part` = NA, 
      `Epitweetr match` = NA,
      `Epitweetr country match` = NA,
      `Epitweetr country code match` = NA
    )
    geo_training$Text <- stringr::str_trim(geo_training$Text)
    dplyr::distinct(.data$`Text`, .data$Lang, .keep_all = T)
  }
  # Adding new tweets
  to_add <- tweets_to_add


  to_tag <- search_tweets(max = to_add) 
  to_tag$text <- stringr::str_trim(to_tag$text)
  to_tag$user_description <- stringr::str_trim(to_tag$user_description)
  to_tag$user_location <- stringr::str_trim(to_tag$user_location)
  texts <- to_tag %>%
    dplyr::transmute(
      Type = "Text", 
      `Text`=.data$text,
      `Location in text` = NA, 
      `Location OK/KO` = "?", 
      `Associate country code` = NA, 
      `Associate with`=NA, 
      `Source` = "Tweet", 
      `Tweet Id` = .data$tweet_id, 
      `Lang` = .data$lang,
      `Tweet part` = "text", 
      `Epitweetr match` = NA,
      `Epitweetr country match` = NA,
      `Epitweetr country code match` = NA
    ) %>%
    dplyr::filter(!.data$`Text` %in% current$`Text`) %>%
    dplyr::distinct(.data$`Text`, .data$Lang, .keep_all = T)
    
  user_desc <- to_tag %>%
    dplyr::filter(!is.na(.data$user_description)) %>%
    dplyr::transmute(
      Type = "Text", 
      `Text`=.data$user_description,
      `Location in text` = NA, 
      `Location OK/KO` = "?", 
      `Associate country code` = NA, 
      `Associate with`= NA, 
      `Source` = "Tweet", 
      `Tweet Id` = .data$tweet_id, 
      `Lang` = .data$lang,
      `Tweet part` = "user description", 
      `Epitweetr match` = NA,
      `Epitweetr country match` = NA,
      `Epitweetr country code match` = NA,
    ) %>%
    dplyr::filter(!.data$`Text` %in% current$`Text`) %>%
    dplyr::distinct(.data$`Text`, .data$Lang, .keep_all = T)
  
  user_loc <- to_tag %>%
    dplyr::filter(!is.na(.data$user_location)) %>%
    dplyr::transmute(
      Type = "Text",
      `Text`=.data$user_location,
      `Location in text` = NA, 
      `Location OK/KO` = "?", 
      `Associate country code` = NA, 
      `Associate with`=NA, 
      `Source` = "Tweet", 
      `Tweet Id` = .data$tweet_id, 
      `Lang` = .data$lang,
      `Tweet part` = "user location", 
      `Epitweetr match` = NA,
      `Epitweetr country match` = NA,
      `Epitweetr country code match` = NA,
    ) %>%
    dplyr::filter(!.data$`Text` %in% current$`Text`) %>%
    dplyr::distinct(.data$`Text`, .data$Lang, .keep_all = T)

  ret = jsonlite::rbind_pages(list(
    current,
    geo_training,
    texts,
    user_desc,
    user_loc
  )) %>% dplyr::filter(!is.na(.data$Text) & !.data$Text == "") %>%
    dplyr::distinct(.data$`Text`, .data$Lang, .keep_all = T)

  text_togeo <- ret %>% dplyr::transmute(
    Text = ifelse(!is.na(.data$`Associate with`), .data$`Associate with`, .data$Text), 
    Lang = ifelse(!is.na(.data$`Associate with`), "all", .data$Lang)
  ) 
  #getting current geolocation evaluation
  geoloc = geolocate_text(df = text_togeo, text_col="Text", lang_col = "Lang")
  ret$`Epitweetr match` <- geoloc$geo_name
  ret$`Epitweetr country match` <- geoloc$geo_country
  ret$`Epitweetr country code match` <- geoloc$geo_country_code
  ret$`Location in text` <- ifelse((is.na(ret$`Location OK/KO`) | ret$`Location OK/KO`=="?") & ret$Type == "Text", geoloc$tags, ret$`Location in text`)
  ret
}


update_training_excel <- function(tweets_to_add = 100) {
  training_df <- get_training_df(tweets_to_add = 100)
  wb <- openxlsx::loadWorkbook(get_geo_training_path())
  # we have to remove the existing worksheet since writing on the same was producing a corrupted file
  openxlsx::removeWorksheet(wb, "geolocation")
  openxlsx::addWorksheet(wb, "geolocation")
  # writing data to the worksheet
  openxlsx::writeDataTable(wb, sheet = "geolocation", training_df, colNames = T, startRow = 1, startCol = "A")
  # setting some minimal formatting
  openxlsx::setColWidths(wb, "geolocation", cols = c(2), widths = c(70))
  openxlsx::setColWidths(wb, "geolocation", cols = c(3, 11, 12, 13), widths = c(25))
  openxlsx::setColWidths(wb, "geolocation", cols = c(4, 5, 6, 10), widths = c(17))
  openxlsx::setRowHeights(wb, "geolocation", rows = 1, heights = 5)
  openxlsx::addStyle(
    wb, 
    sheet = "geolocation", 
    style = openxlsx:: createStyle(fontSize = 10, halign = "center", fgFill = "#ff860d", border = c("top", "bottom", "left", "right"), textDecoration="bold", wrapText = T), 
    rows = 1, 
    cols = 1, 
    gridExpand = FALSE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "geolocation", 
    style = openxlsx:: createStyle(fontSize = 10, halign = "center", fgFill = "#ffde59", border = c("top", "bottom", "left", "right"), textDecoration="bold", wrapText = T), 
    rows = 1, 
    cols = 2:6, 
    gridExpand = FALSE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "geolocation", 
    style = openxlsx:: createStyle(fontSize = 10, halign = "center", fgFill = "#f7d1d5", border = c("top", "bottom", "left", "right"), textDecoration="bold", wrapText = T), 
    rows = 1, 
    cols = 7:10, 
    gridExpand = FALSE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "geolocation", 
    style = openxlsx:: createStyle(fontSize = 10, halign = "center", fgFill = "#dedce6", border = c("top", "bottom", "left", "right"), textDecoration="bold", wrapText = T), 
    rows = 1, 
    cols = 11:13, 
    gridExpand = FALSE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "geolocation", 
    style = openxlsx:: createStyle(fontSize = 10, border = c("top", "bottom", "left", "right")), 
    rows = 1:nrow(training_df)+1, 
    cols = 1:13, 
    gridExpand = TRUE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "geolocation", 
    style = openxlsx:: createStyle(fontSize = 10, border = c("top", "bottom", "left", "right"), wrapText=T), 
    rows = 2:nrow(training_df)+1, 
    cols = 2, 
    gridExpand = TRUE
  )
  openxlsx::freezePane(wb, "geolocation",firstActiveRow = T)
  openxlsx::saveWorkbook(wb, get_user_geo_training_path() ,overwrite = T) 
}

