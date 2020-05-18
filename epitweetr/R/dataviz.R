
#' Generates the trend line report with alerts
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
  topic
  , countries=c(1)
  , type_date="created_date"
  , date_min="1900-01-01"
  , date_max="2100-01-01"
  , with_retweets = FALSE
  , location_type = "tweet"
  , alpha = 0.025
  , no_historic = 7 
  ){
  #Importing pipe operator
  df <- get_aggregates("country_counts")
  df <- trend_line_prepare(df,topic,countries, type_date, date_min, date_max, with_retweets, location_type, alpha, no_historic)
  plot_trendline(df,countries,topic,date_min,date_max)
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
trend_line_prepare <- function(
    df
    , topic
    , countries = c(1)
    , date_type = c("creted_date")
    , date_min = as.Date("1900-01-01")
    , date_max = as.Date("2100-01-01")
    , with_retweets = FALSE
    , location_type = "tweet" 
    , alpha = 0.025
    , no_historic = 7 )
{
  #Importing pipe operator
  `%>%` <- magrittr::`%>%` 

  # Getting regios details
  regions <- get_country_items()
  
  df$known_users = df$known_original
  # Adding retwets if requested
  if(with_retweets){
    df$tweets <- df$retweets + df$tweets
    df$known_users <- df$known_retweets + df$known_original
  }


  if(length(countries)==0) countries = c(1)
  # Setting country codes as requested location types as requested
  df <- 
    Reduce(
      x = c(
         if(location_type %in% c("tweet", "both"))
	   list(df %>% dplyr::rename(geo_country_code = tweet_geo_country_code) %>% dplyr::select(-user_geo_country_code))
	 else list()
         , if(location_type %in% c("user", "both"))
	   list(df %>% dplyr::rename(geo_country_code = user_geo_country_code) %>% dplyr::select(-tweet_geo_country_code))
	 else list()
        )
      , f = function(df1, df2) {dplyr::bind_rows(df1, df2)}
    )

  series <- lapply(1:length(countries), function(i) {
    alerts <- 
      get_alerts(
	df = df,
        topic = topic, 
        country_codes = regions[[countries[[i]]]]$codes,
	country_code_col = "geo_country_code",
        known_user_col = "known_users",
        start = as.Date(date_min), 
        end = as.Date(date_max), 
        no_historic=no_historic, 
        alpha=alpha
      )
    alerts$topic <- firstup(stringr::str_replace_all(topic, "%20", " "))
    alerts <- dplyr::rename(alerts, date = reporting_date) 
    alerts <- dplyr::rename(alerts, number_of_tweets = count) 
    alerts$date <- as.Date(alerts$date, origin = '1970-01-01')
    alerts$country <- regions[[countries[[i]]]]$name
    alerts$known_ratio = alerts$known_users / alerts$number_of_tweets
    alerts
  })

  df <- Reduce(x = series, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
    
  if(date_type =="created_weeknum"){
    df <- df %>% 
      dplyr::group_by(week = strftime(date, "%G%V"), topic, country) %>% 
      dplyr::summarise(c = sum(number_of_tweets), a = max(alert), d = min(date)) %>% 
      dplyr::select(d , c, a, topic, country) %>% 
      dplyr::rename(date = d, number_of_tweets = c, alert = a)
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

plot_trendline <- function(df,countries,topic,date_min,date_max){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  regions <- get_country_items()

  time_alarm <- data.frame(date = df$date[which(df$alert == 1)], country = df$country[which(df$alert == 3)], y = vapply(which(df$alert == 1), function(i) 0, double(1)))
  df$tooltip = paste("\nKnown users tweets: ", df$known_users, "\nKnown users ratio: ", df$known_ratio,"\nAlert: ", df$alert, "\nThreshold: ", df$limit, sep = "")

  fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets, label = tooltip)) +
    ggplot2::geom_line(ggplot2::aes(colour=country)) + {
      if(nrow(time_alarm) > 0) ggplot2::geom_point(data = time_alarm, mapping = ggplot2::aes(x = date, y = y, colour = country), shape = 2) 
    } +
    ggplot2::labs(
      title=ifelse(length(countries)==1,
        paste0("Number of tweets mentioning ",topic,"\n from ",date_min, " to ",date_max," in ", regions[[as.integer(countries)]]$name),
        paste0("Number of tweets mentioning ",topic,"\n from ",date_min, " to ",date_max," in multiples regions")
      )
    ) +
    ggplot2::xlab('Day and month') +
    ggplot2::ylab('Number of tweets') +
    ggplot2::scale_y_continuous(breaks = function(x) unique(floor(pretty(seq(0, (max(x) + 1) * 1.1)))))+
    ggplot2::scale_x_date(date_labels = "%Y-%m-%d",
                          expand = c(0, 0),
                          breaks = function(x) {
			    days <- max(x) - min(x)
			    if(days < 7) seq.Date(from = min(x), to = max(x), by = "1 days")
			    else if(days %% 7 == 0) seq.Date(from = min(x), to = max(x), by = "7 days")
			    else c(seq.Date(from = min(x), to = max(x), by = "7 days"), max(x))
			  }
    ) +
    ggplot2::theme_classic(base_family = get_font_family()) +
    ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold",lineheight = 0.9),
                   axis.text = ggplot2::element_text(colour = "black", size = 8),
                   axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                       margin = ggplot2::margin(-15, 0, 0, 0)),
                   axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
                   axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 10),
                   legend.position=ifelse(length(countries)<2,"none","top")
    )
  
  df <- dplyr::rename(df,"Country" = country)
  

  df <- dplyr::rename(df,"Number of tweets" = number_of_tweets, "Tweet date" = date,"Topic"= topic)
  list("chart" = fig_line, "data" = df) 
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
create_map <- function(topic=c(),countries=c(),type_date="created_date",date_min="1900-01-01",date_max="2100-01-01", with_retweets = FALSE, location_type = "tweet"){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  df <- get_aggregates("country_counts")
  regions <- get_country_items()
  country_codes <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(countries), function(i) unlist(regions[[i]]$codes)))

  # Adding retwets if requested
  if(with_retweets)
    df$tweets <- df$retweets + df$tweets

  # Setting country codes as requested location types as requested
  df <-
    Reduce(
      x = c(
         if(location_type %in% c("tweet", "both"))
           list(df %>% dplyr::rename(country_code = tweet_geo_country_code) %>% dplyr::select(-user_geo_country_code))
         else list()
         , if(location_type %in% c("user", "both"))
           list(df %>% dplyr::rename(country_code = user_geo_country_code) %>% dplyr::select(-tweet_geo_country_code))
         else list()
        )
      , f = function(df1, df2) {dplyr::bind_rows(df1, df2)}
    )


  # aggregating by country
  df <- (df %>% 
    dplyr::filter(
        topic==topic 
        & !is.na(country_code)
        & created_date >= date_min 
        & created_date <= date_max
        & (if(length(countries)==0) TRUE else country_code %in% country_codes )
    ) %>% 
    dplyr::group_by(country_code) %>% 
    dplyr::summarize(count = sum(tweets)) %>% 
    dplyr::ungroup() 
  )

  # Adding country properties
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


create_topwords <- function(topic=c(),country_codes=c(),date_min=as.Date("1900-01-01"),date_max=as.Date("2100-01-01"), with_retweets = FALSE, location_type = "tweet") {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`

  df <- (get_aggregates("topwords"))
  df <- (df
      %>% dplyr::filter(
        topic == topic 
        & created_date >= date_min 
        & created_date <= date_max
        & (if(length(country_codes)==0) TRUE else tweet_geo_country_code %in% country_codes )
      ))
  if(!with_retweets) df$frequency <- df$original

  df <- (df
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
              title = paste0("Top words of tweets mentioning ",topic,"\n from ",date_min, " to ",date_max)
           )+
           ggplot2::theme_classic(base_family = get_font_family()) +
           ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold"),
                       axis.text = ggplot2::element_text(colour = "black", size = 8),
                       axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                           margin = ggplot2::margin(-15, 0, 0, 0)),
                       axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
                       axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 10))
    )
  list("chart" = fig, "data" = df) 
}



#Function to get the font family
get_font_family <- function(){
  if(.Platform$OS.type == "windows") 
    "sans"
  else "Helvetica"
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
