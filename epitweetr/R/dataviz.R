
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
  , date_min=as.Date("1900-01-01")
  , date_max=as.Date("2100-01-01")
  , with_retweets = FALSE
  , location_type = "tweet"
  , alpha = 0.025
  , no_historic = 7 
  , bonferroni_correction = FALSE
  , same_weekday_baseline = FALSE
  ){
  df <- 
    calculate_regions_alerts(
      topic = topic,
      regions = countries, 
      date_type = type_date, 
      date_min = date_min, 
      date_max = date_max, 
      with_retweets = with_retweets, 
      location_type = location_type, 
      alpha = alpha, 
      no_historic = no_historic, 
      bonferroni_correction = bonferroni_correction,
      same_weekday_baseline = same_weekday_baseline
    )
  if(nrow(df)>0) df$topic <- firstup(stringr::str_replace_all(topic, "%20", " "))
  plot_trendline(df,countries,topic,date_min,date_max)
}



#' Plot trend_line
#'
#' @param df 
#' @export
plot_trendline <- function(df,countries,topic,date_min,date_max){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  regions <- get_country_items()

  # Calculate alert ranking to avoid alert symbol overlapping 
  df <- df %>% 
    dplyr::arrange(alert, date, country) %>% 
    dplyr::group_by(alert, date) %>% 
    dplyr::mutate(rank = rank(country, ties.method = "first")) %>% 
    dplyr::ungroup()
  # Getting alert dataset 
  time_alarm <- data.frame(
    date = df$date[which(df$alert == 1)], 
    country = df$country[which(df$alert == 1)], 
    y = vapply(which(df$alert == 1), function(i) - df$rank[[i]] * (max(df$number_of_tweets))/30, double(1)), 
    # adding hover text for alerts
    Details = vapply(which(df$alert == 1), function(i) {
      paste(
        "\nRegion:",df$country[[i]],
        "\nNumber of tweets: ", df$number_of_tweets[[i]], 
        "\nThreshold: ", round(df$limit[[i]]), 
        "\nDate:",df$date[[i]], 
        "\nKnown users tweets: ", df$known_users[[i]], 
        "\nKnown users ratio: ", round(df$known_ratio[[i]]*100, 2), "%",
        sep = ""
      )}, 
    character(1))
  )
  
  # adding hover text for alerts
  df$Details <- 
    paste(
      "\nRegion:",df$country,
      "\nAlert: ", ifelse(df$alert==1, "yes", "no"), 
      "\nNumber of tweets: ", df$number_of_tweets, 
      "\nThreshold: ", round(df$limit), 
      "\nDate:",df$date, 
      "\nKnown users tweets: ", df$known_users, 
      "\nKnown users ratio: ", round(df$known_ratio*100, 2), "%",
      sep = "")

  fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets, label = Details)) +
    ggplot2::geom_line(ggplot2::aes(colour=country)) + {
      if(nrow(time_alarm) > 0) ggplot2::geom_point(data = time_alarm, mapping = ggplot2::aes(x = date, y = y, colour = country), shape = 2, size = 2) 
    } +
    #ggplot2::geom_ribbon(ggplot2::aes(ymin=y, ymax=limit), linetype=2, alpha=0.1)
    ggplot2::labs(
      title=ifelse(length(countries)==1,
        paste0("Number of tweets mentioning ",topic,"\n from ",date_min, " to ",date_max," in ", regions[[as.integer(countries)]]$name),
        paste0("Number of tweets mentioning ",topic,"\n from ",date_min, " to ",date_max," in multiples regions")
      )
    ) +
    ggplot2::xlab('Day and month') +
    ggplot2::ylab('Number of tweets') +
    ggplot2::scale_y_continuous(breaks = function(x) unique(floor(pretty(seq(0, (max(x) + 1) * 1.1)))))+
    ggplot2::expand_limits(y = 0) +
    ggplot2::scale_x_date(
      date_labels = "%Y-%m-%d",
      expand = c(0, 0),
      breaks = function(x) {
			    # custom logic for x axis
          days <- as.numeric(max(x) - min(x))
          weeks <- days / 7
          years <- days / 365
          # One label per day if period is less or equal then 15 days
			    if(days < 15) {
            seq.Date(from = min(x), to = max(x), by = "1 days")
          # One label per day of week of first day in period and one for last day in period if period is between 16 days and 20 weeks
			    #  - Case for period ending on same day of week than period start 
          } else if(days %% 7 == 0 && weeks <= 10) {
            seq.Date(from = min(x), to = max(x), by = "7 days")
			    #  - Case for period ending on same different of week than period start (last day should be added
			    } else if(weeks <= 20) {
            c(seq.Date(from = min(x), to = max(x), by = "7 days"), max(x))
          # One label per month for day of month in period start if period is less or equal to 2 years but more than 20 weeks
			    #  - Case for period ending on different day of month than period start 
          } else if(strftime(min(x), format= "%d") != strftime(max(x), format= "%d") && years <=2) {
            c(seq.Date(from = min(x), to = max(x), by = "1 month"), max(x))
          } else if(years <=2) {
			    #  - Case for period ending on same day of month than period start (last day should be added
            seq.Date(from = min(x), to = max(x), by = "1 month")
          # One label per year for day of year in period start if period is more than 2 years
			    #  - Case for period ending on different day of year than period start 
          } else if(strftime(min(x), format= "%m-%d") != strftime(max(x), format= "%m-%d")) {
            c(seq.Date(from = min(x), to = max(x), by = "1 year"), max(x))
          } else { 
			    #  - Case for period ending on same day of month than period start (last day should be added
            seq.Date(from = min(x), to = max(x), by = "1 year")
          }
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
create_map <- function(topic=c(),countries=c(1),type_date="created_date",date_min="1900-01-01",date_max="2100-01-01", with_retweets = FALSE, location_type = "tweet", caption = ""){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  df <- get_aggregates(dataset = "country_counts", filter = list(topic = topic, period = list(date_min, date_max)))
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
   main = paste("Tweets by country mentioning", topic, "\nfrom", date_min, "to", date_max),
   sub = caption
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
create_topwords <- function(topic,country_codes=c(),date_min=as.Date("1900-01-01"),date_max=as.Date("2100-01-01"), with_retweets = FALSE, location_type = "tweet", top = 25) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  f_topic <- topic
  df <- get_aggregates(dataset = "topwords", filter = list(topic = f_topic, period = list(date_min, date_max)))
  df <- (df
      %>% dplyr::filter(
        topic == f_topic 
        & created_date >= date_min 
        & created_date <= date_max
        & (if(length(country_codes)==0) TRUE else tweet_geo_country_code %in% country_codes )
        & tokens != "via" & nchar(tokens) > 1
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
              subtitle = paste("from", date_min, "to", date_max),
              caption = "Top words is the only figure in the dashboard only considering tweet location (ignores the location type parameter)"
           ) +
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
