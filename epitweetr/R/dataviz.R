
#' Search for all tweets on topics defined on configuration
#' @param varname 
#'
#' @export



#Function to set the font family
set_font_family <- function(varname){
  return(varname)
}

#Function to get the font family
get_font_family <- function(font){
  return (font)
}

#Capitalize first letter of a string
#' Title
#'
#' @param x 
#'
#' @return
#' @export
#'
#' @examples
firstup <- function(x) {
  x <- tolower(x)
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  x
}


#Treat data before output
#' Title
#'
#' @param df 
#' @param s_topic 
#' @param date 
#' @param geo_country_code 
#' @param s_country 
#' @param date_min 
#' @param date_max 
#'
#' @return
#' @export
#'
#' @examples
data_treatment <- function(df,s_topic,s_country, date, geo_country_code, date_min="1900-01-01",date_max="2100-01-01", alpha = 0.025, no_historic = 7 ){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%` 
  #If user_geo_country_code selected, get the location of user and tweet. If different, use user location
  #if(geo_country_code == "user_geo_country_code"){ 
  #  df$user_geo_country_code <- dplyr::coalesce(df$user_geo_country_code,df$tweet_geo_country_code)
  #}
  #Renaming date and geo_country_code (tweet or user) 
  #colnames(df)[colnames(df)== geo_country_code] <- "geo_country_code"
  df$tweet_geo_country_code <- "geo_country_code"
  #colnames(df)[colnames(df)== date] <- "date"
  
  
  if(date == "created_weeknum"){
    date_min <-as.Date(paste(date_min,"1"),"%Y%U %u")
    date_max <-as.Date(paste(date_max,"1"),"%Y%U %u")
    #df$created_weeknum <-as.Date(paste(df$created_weeknum,"1"),"%Y%U %u")
  }
  
  #Getting all dates for selected s_country and topic  !! What to do if all s_country selected ? 
  dates <- seq(as.Date(date_min), as.Date(date_max), "weeks")
  if(date=="created_weeknum"){
    dates <- as.integer(strftime(dates, format = "%Y%V"))
    date_min <- as.integer(strftime(date_min, format = "%Y%V"))
    date_max <- as.integer(strftime(date_max, format = "%Y%V"))
    
  }
  
  s_country <- if(length(s_country)>0) s_country else list(NA)
  series <- lapply(1:length(s_country), function(i) {
    alerts <- 
      get_alerts(
        topic = s_topic, 
        country_codes= if(is.na(s_country[[i]])) list() else list(s_country[[i]]), 
        start = as.Date(date_min), 
        end = as.Date(date_max), 
        no_historic=no_historic, 
        alpha=alpha
      )
    alerts$topic <- firstup(stringr::str_replace_all(s_topic, "%20", " "))
    alerts <- dplyr::rename(alerts, date = reporting_date) 
    alerts <- dplyr::rename(alerts, number_of_tweets = count) 
    alerts$date <- as.Date(alerts$date, origin = '1970-01-01')
    alerts$geo_country_code <- if(is.na(s_country[[i]])) "All" else s_country[[i]]
    alerts
  })

  df <- Reduce(x = series, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
    
  if(date=="created_weeknum"){
    df <- df %>% 
      dplyr::group_by(week = strftime(date, "%G%V"), topic, geo_country_code) %>% 
      dplyr::summarise(c = sum(count), a = max(alert), d = min(date)) %>% 
      dplyr::select(d , c, a, topic, geo_country_code) %>% 
      dplyr::rename(date = d, count = c, alert = a)
  }

  return(df)
  
}

#' Plot trend_line
#'
#' @param df 
#'
#' @return
#' @export
#'
#' @examples
#' 
#Setting font family 
font<-set_font_family("Helvetica") 

plot_trendline <- function(df,s_country,s_topic,date_min,date_max,selected_countries){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`

  if(length(s_country)>0){ #if several countries selected, aggregation by country
    #Transforming country code into label
    regions <- get_country_items()
    map<-get_country_code_map()
    df$geo_country_code <- map[(df$geo_country_code)]
    df <- dplyr::rename(df,country = geo_country_code)
    time_alarm <- data.frame(date = df$date[which(df$alert == 1)], country = df$country[which(df$alert == 1)], y = vapply(which(df$alert == 1), function(i) 0, double(1)))
    
    fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets)) +
      ggplot2::geom_line(ggplot2::aes(colour=country)) + {
        if(nrow(time_alarm) > 0) ggplot2::geom_point(data = time_alarm, mapping = ggplot2::aes(x = date, y = y, colour = country), shape = 2) 
      } +
      ggplot2::labs(title=ifelse(length(selected_countries)==1,paste0("Number of tweets mentioning ",s_topic,"\n from ",date_min, " to ",date_max," in ", regions[[as.integer(selected_countries)]]$name),paste0("Number of tweets mentioning ",s_topic,"\n from ",date_min, " to ",date_max))) +
      ggplot2::xlab('Day and month') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_y_continuous(breaks = function(x) unique(floor(pretty(seq(0, (max(x) + 1) * 1.1)))))+
      ggplot2::scale_x_date(date_labels = "%d %b",
                            expand = c(0, 0),
                            breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic(base_family = get_font_family(font)) +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold",lineheight = 0.9),
                     axis.text = ggplot2::element_text(colour = "black", size = 8),
                     axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                         margin = ggplot2::margin(-15, 0, 0, 0)),
                     axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
                     axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 10),
                     legend.position=ifelse(length(s_country)==1,"none","top"))
    
    df <- dplyr::rename(df,"Country" = country)
  } else{ #no countries selected, without aggregation by country #has to be factorized and need to generate all dates
    time_alarm <- data.frame(date = df$date[which(df$alert == 1)], y = vapply(which(df$alert == 1), function(i) 0, double(1)))
    
    fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets)) +
      ggplot2::geom_line(colour = "#65b32e") + {
        if(nrow(time_alarm) > 0) ggplot2::geom_point(data = time_alarm, mapping = ggplot2::aes(x = date, y = y, colour = "#65b32e"), shape = 2)
      } + 
      ggplot2::labs(title=paste0("Number of tweets mentioning ",s_topic,"\n from ",date_min, " to ",date_max)) +
      ggplot2::xlab('Day and month') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_y_continuous(breaks = function(x) unique(floor(pretty(seq(0, (max(x) + 1) * 1.1)))))+
      #ggplot2::scale_y_continuous(limits = c(0, max(df$number_of_tweets)), expand = c(0,0)) +
      ggplot2::scale_x_date(date_labels = "%d %b",
                            expand = c(0, 0),
                            breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic(base_family = get_font_family(font)) +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold"),
                     axis.text = ggplot2::element_text(colour = "black", size = 8),
                     axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                         margin = ggplot2::margin(-15, 0, 0, 0)),
                     axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
                     axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 10),
                     legend.position="none"
                     )
    
  }
  df <- dplyr::rename(df,"Number of tweets" = number_of_tweets, "Tweet date" = date,"Topic"= topic)
  list("chart" = fig_line, "data" = df) 
}



######## trend_line function
#' Title
#'
#' @param s_topic 
#' @param s_country 
#' @param type_date 
#' @param geo_country_code 
#' @param date_min 
#' @param date_max 
#'
#' @return
#' @export
#'
#' @examples
trend_line <- function(
  s_topic=c()
  , s_country=c()
  , type_date="created_date"
  , geo_country_code="tweet_geo_country_code"
  , date_min="1900-01-01"
  , date_max="2100-01-01"
  , selected_countries
  , alpha = 0.025
  , no_historic = 7 
  ){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  dfs <- get_aggregates("country_counts")
  colnames(dfs)[colnames(dfs)== geo_country_code] <- "geo_country_code"
  fig <- data_treatment(dfs,s_topic,s_country, type_date, geo_country_code, date_min,date_max, alpha, no_historic)
  plot_trendline(fig,s_country,s_topic,date_min,date_max,selected_countries)
}
    
#######################################MAP#####################################
#' Title
#'
#' @param s_topic 
#' @param geo_code 
#' @param type_date 
#' @param date_min 
#' @param date_max 
#'
#' @return
#' @export
#'
#' @examples
create_map <- function(s_topic=c(),s_country=c(),geo_code = "tweet",type_date="created_date",date_min="1900-01-01",date_max="2100-01-01"){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  df <- get_aggregates("country_counts")
  colnames(df)[colnames(df)== "tweet_geo_country_code"] <- "country_code"
  # aggregating by country
  df <- (df %>% 
    dplyr::filter(
        topic==s_topic 
        & !is.na(country_code)
        & created_date >= date_min 
        & created_date <= date_max
        & (if(length(s_country)==0) TRUE else country_code %in% s_country )
    ) %>% 
    dplyr::group_by(country_code) %>% 
    dplyr::summarize(count = sum(tweets)) %>% 
    dplyr::ungroup() 
  )

  # Addinf country properties
  regions <- get_country_items()
  map <- get_country_index_map()
  df$Country <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$name else NA)
  df$Long <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) mean(c(regions[[i]]$minLong,regions[[i]]$maxLong)) else NA)
  df$Lat <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) mean(c(regions[[i]]$minLat,regions[[i]]$maxLat)) else NA)
  df$MinLat <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$minLat else NA)
  df$MaxLat <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$maxLat else NA)
  df$MinLong <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$minLong else NA)
  df$MaxLong <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$maxLong else NA)

  minLong <- min(df$MinLong, na.rm = TRUE)
  maxLong <- max(df$MaxLong, na.rm = TRUE)
  minLat <- min(df$MinLat, na.rm = TRUE)
  maxLat <- max(df$MaxLat, na.rm = TRUE)
  maxCount <- max(df$count)
  mymap <- maps::map(
    "world"
    , fill=TRUE
    , col=rgb(red =210/255, green = 230/255, blue = 230/255)
    , bg="white"
    , ylim=c(if(minLat>-60) minLat else -60, if(maxLat< 90) maxLat else 90)
    , xlim = c(minLong, maxLong)
    , border = "gray"
  ) #, ylim=c(-60, 90), mar=c(0,0,0,0)
  
  fig <- points(df$Long, df$Lat, col = rgb(red = 1, green = 102/255, blue = 102/255), cex = 4 * sqrt(df$count)/sqrt(maxCount),lwd=.4, pch = 21) #circle line
  fig <- points(df$Long, df$Lat, col = rgb(red = 1, green = 102/255, blue = 102/255, alpha = 0.5), cex = 4 * sqrt(df$count)/sqrt(maxCount),lwd=.4, pch = 19) #filled circlee
  list("chart" = fig, "data" = df) 
}


create_topwords <- function(s_topic=c(),s_country=c(),date_min="1900-01-01",date_max="2100-01-01") {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`

  df <- (get_aggregates("topwords"))
  df <- (df
      %>% dplyr::filter(
        topic==s_topic 
        & created_date >= date_min 
        & created_date <= date_max
        & (if(length(s_country)==0) TRUE else tweet_geo_country_code %in% s_country )
      )
      %>% dplyr::group_by(tokens)
      %>% dplyr::summarize(frequency = sum(frequency))
      %>% dplyr::ungroup() 
      %>% dplyr::arrange(-frequency) 
      %>% head(25)
      %>% dplyr::mutate(tokens = reorder(tokens, frequency))
  )
  fig <- (
      df %>% ggplot2::ggplot(ggplot2::aes(x = tokens, y = frequency)) +
           ggplot2::geom_col(fill = "#7EB750") +
           ggplot2::xlab(NULL) +
           ggplot2::coord_flip() +
           ggplot2::labs(
              x = "Unique words",
              y = "Count",
              #title = "Top words words in period"
              title = paste0("Top words of tweets mentioning ",s_topic,"\n from ",date_min, " to ",date_max)
           )+
           ggplot2::theme_classic(base_family = get_font_family(font)) +
           ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold"),
                       axis.text = ggplot2::element_text(colour = "black", size = 8),
                       axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                           margin = ggplot2::margin(-15, 0, 0, 0)),
                       axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
                       axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 10))
    )
  list("chart" = fig, "data" = df) 
}
