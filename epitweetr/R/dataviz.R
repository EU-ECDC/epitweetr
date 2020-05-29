
#' Generates the trend line report with alerts
#'
#' @param s_topic 
#' @param s_country 
#' @param type_date 
#' @param geo_country_code 
#' @param date_min 
#' @param date_max 
#' @export
#'
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
  , bonferroni_correction = FALSE
  ){
  #Importing pipe operator
  df <- get_aggregates("country_counts")
  df <- trend_line_prepare(df,topic,countries, type_date, date_min, date_max, with_retweets, location_type, alpha, no_historic, bonferroni_correction)
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
#' @export
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
    , no_historic = 7 
    , bonferroni_correction = FALSE
    )
{
  #Importing pipe operator
  `%>%` <- magrittr::`%>%` 

  # Getting regios details
  regions <- get_country_items()
  
  df$known_users <- df$known_original
  # Adding retwets if requested
  if(with_retweets){
    df$tweets <- ifelse(is.na(df$retweets), 0, df$retweets) + ifelse(is.na(df$tweets), 0, df$tweets)
    df$known_users <- df$known_retweets + df$known_original
  }
  #Setting world as region if no region is selected
  if(length(countries)==0) countries = c(1)

  series <- lapply(1:length(countries), function(i) {
    bonferroni_m <- 
      if(bonferroni_correction) {
        length(regions[unlist(lapply(regions, function(r) r$level == regions[[ countries[[i]] ]]$level))])
      } else 1
    alerts <- 
      get_alerts(
	      df = df,
        topic = topic, 
        country_codes = regions[[countries[[i]]]]$codes,
	      country_code_cols = if(location_type == "tweet") "tweet_geo_country_code" else if(location_type == "user") "user_geo_country_code" else c("tweet_geo_country_code", "user_geo_country_code"),
        known_user_col = "known_users",
        start = as.Date(date_min), 
        end = as.Date(date_max), 
        no_historic=no_historic, 
        alpha=alpha,
        bonferroni_m = bonferroni_m
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
#' @export
plot_trendline <- function(df,countries,topic,date_min,date_max){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  regions <- get_country_items()

  time_alarm <- data.frame(
    date = df$date[which(df$alert == 1)], 
    country = df$country[which(df$alert == 1)], 
    y = vapply(which(df$alert == 1), function(i) 0, double(1)), 
    Details =  vapply(which(df$alert == 1), function(i) "", character(1))
  )
  df$Details <- 
    paste(
      "\nRegion:",df$country,
      "\nAlert: ", ifelse(df$alert==1, "yes", "no"), 
      "\nNumber of tweets: ", df$number_of_tweets, 
      "\nThreshold: ", round(df$limit), 
      "\nDate:",df$date, 
      "\nKnown users tweets: ", df$known_users, 
      "\nKnown users ratio: ", df$known_ratio,
      sep = "")

  fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets, label = Details)) +
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
			    days <- as.numeric(max(x) - min(x))
			    if(days < 15) seq.Date(from = min(x), to = max(x), by = "1 days")
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
#' @export
create_map <- function(topic=c(),countries=c(1),type_date="created_date",date_min="1900-01-01",date_max="2100-01-01", with_retweets = FALSE, location_type = "tweet"){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  df <- get_aggregates("country_counts")
  regions <- get_country_items()
  country_codes <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(countries), function(i) unlist(regions[[i]]$codes)))

  # Adding retwets if requested
  if(with_retweets)
    df$tweets <- ifelse(is.na(df$retweets), 0, df$retweets) + ifelse(is.na(df$tweets), 0, df$tweets)
	
  #Setting country cols as requested
  country_code_cols = if(location_type == "tweet") "tweet_geo_country_code" else if(location_type == "user") "user_geo_country_code" else c("tweet_geo_country_code", "user_geo_country_code")
  # Setting country codes as requested location types as requested
  df <- (
         if(location_type =="tweet")
           df %>% dplyr::rename(country_code = tweet_geo_country_code) %>% dplyr::select(-user_geo_country_code)
         else if(location_type == "user")
           df %>% dplyr::rename(country_code = user_geo_country_code) %>% dplyr::select(-tweet_geo_country_code)
         else dplyr::bind_rows(
           df %>% dplyr::rename(country_code = tweet_geo_country_code) %>% dplyr::filter(!is.na(country_code)) %>% dplyr::select(-user_geo_country_code),
           df %>% dplyr::rename(country_code = user_geo_country_code) %>% dplyr::filter(!is.na(country_code) & country_code != tweet_geo_country_code ) %>% dplyr::select(-tweet_geo_country_code)
         )  
    )
  f_topic <- topic
  # aggregating by country
  df <- (df %>% 
    dplyr::filter(
        topic==f_topic
        & !is.na(country_code)
        & created_date >= date_min 
        & created_date <= date_max
        & (
          if(length(country_codes) == 0) TRUE 
          else country_code %in% country_codes
        ) 
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
  ) 
  
  fig <- points(df$Long, df$Lat, col = rgb(red = 1, green = 102/255, blue = 102/255), cex = 3 * sqrt(df$count)/sqrt(maxCount),lwd=.4, pch = 21) #circle line
  fig <- points(df$Long, df$Lat, col = rgb(red = 1, green = 102/255, blue = 102/255, alpha = 0.5), cex = 3 * sqrt(df$count)/sqrt(maxCount),lwd=.4, pch = 19) #filled circlee
  fig <- title(
   main = paste("Tweets by country mentioning", topic, "\nfrom", date_min, "to", date_max)
  )
  #Generating legend
  cuts <- sapply(c(maxCount/50, maxCount/20, maxCount/5, maxCount), function(v) round(v, digits = - floor(log10(v))))
  fcuts <- sapply(cuts, function(c) if(c < 1000) paste(c) else if (c< 1000000) sprintf("%.0fk",c/1000) else sprintf("%.0fM",c/1000000)) 
  par(xpd=TRUE)
  fig <- legend("bottom", ncol = 4,# position
    legend = fcuts, 
    pt.cex = 3 * sqrt(cuts)/sqrt(maxCount),
    col = rgb(red = 1, green = 102/255, blue = 102/255),
    pt.bg = rgb(red = 1, green = 102/255, blue = 102/255, alpha = 0.5),
    pch = 21,
    bty = "n", 
    inset=c(0,-0.2)
  ) # border
  list("chart" = fig, "data" = df) 
}

#' Create topwords chart
#' @export
create_topwords <- function(topic=c(),country_codes=c(),date_min=as.Date("1900-01-01"),date_max=as.Date("2100-01-01"), with_retweets = FALSE, location_type = "tweet", top = 25) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  f_topic <- topic
  df <- (get_aggregates("topwords"))
  df <- (df
      %>% dplyr::filter(
        topic == f_topic 
        & created_date >= date_min 
        & created_date <= date_max
        & (if(length(country_codes)==0) TRUE else tweet_geo_country_code %in% country_codes )
      ))
  if(!with_retweets) df$frequency <- df$original

  df <- (df
      %>% dplyr::filter(!is.na(frequency))
      %>% dplyr::group_by(tokens)
      %>% dplyr::summarize(frequency = sum(frequency))
      %>% dplyr::ungroup() 
      %>% dplyr::arrange(-frequency) 
      %>% head(top)
      %>% dplyr::mutate(tokens = reorder(tokens, frequency))
  )
  fig <- (
      df %>% ggplot2::ggplot(ggplot2::aes(x = tokens, y = frequency)) +
           ggplot2::geom_col(fill = "#7EB750") +
           ggplot2::xlab(NULL) +
           ggplot2::coord_flip(expand = FALSE) +
           ggplot2::labs(
              x = "Unique words",
              y = "Count",
              title = paste("Top words of tweets mentioning", topic),
              subtitle = paste("from", date_min, "to", date_max)
           )+
           ggplot2::theme_classic(base_family = get_font_family()) +
           ggplot2::theme(
	     plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold"),
	     plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 12),
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

  if(.Platform$OS.type == "windows" && is.null(windowsFonts("Helvetica")[[1]]))
    windowsFonts(Helvetica = windowsFont("Helvetica"))
  "Helvetica"
}

#Capitalize first letter of a string
#' Title
#'
#' @param x 
firstup <- function(x) {
  x <- tolower(x)
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  x
}
