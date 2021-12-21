
#' @title Plot the trendline report of epitweetr dashboard
#' @description Generates a trendline chart of number of tweets by region, for one topic, including alerts using the reweighted version of the EARS algorithm
#' @param topic Character(1) containing the topic to use for the report  
#' @param countries Character vector containing the name of the countries and regions to plot or their respective indexes on the Shiny app select, default: c(1)
#' @param date_type Character vector specifying the time granularity of the report either 'created_weeknum' or 'created_date', default: 'created_date'
#' @param date_min Date indicating start of the reporting period, default: "1900-01-01"
#' @param date_max Date indicating end of the reporting period, default: "2100-01-01"
#' @param with_retweets Logical value indicating whether to include retweets in the time series, default: FALSE
#' @param location_type Character(1) vector indicating the location type. Possible values 'tweet', 'user' or 'both', default: 'tweet'
#' @param alpha Numeric(1) value indicating the alert detection confidence, default: 0.025
#' @param alpha_outlier Numeric(1) value indicating the outliers detection confidence for downweighting, default: 0.05
#' @param k_decay Strength of outliers downweighting, default: 4
#' @param no_historic Number of observations to build the baseline for signal detection, default: 7
#' @param bonferroni_correction Logical value indicating whether to apply the Bonferroni correction for signal detection, default: FALSE
#' @param same_weekday_baseline Logical value indicating whether to use same day of weeks for building the baseline or consecutive days, default: FALSE
#' @return A named list containing two elements: 'chart' with the ggplot2 figure and 'data' containing the data frame that was used to build the chart.
#' @details Produces a multi-region line chart for a particular topic of number of tweets collected based on the provided parameters. 
#' Alerts will be calculated using a modified version of the EARS algorithm that applies a Farrington inspired downweighting of previous outliers.
#' 
#' Days in this function are considered as contiguous blocks of 24 hours starting for the previous hour of the last collected tweet.
#'
#' This function requires \code{\link{search_loop}} and \code{\link{detect_loop}} to have already run successfully to show results.
#' @examples 
#' if(FALSE){
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    #Getting trendline for dengue for South America for the last 30 days
#'    trend_line(
#'      topic = "dengue", 
#'      countries = "South America", 
#'      date_min = as.Date(Sys.time())-30, 
#'      date_max=as.Date(Sys.time())
#'    ) 
#' }
#' @seealso 
#'  \code{\link{create_map}}
#'  \code{\link{create_topwords}}
#'  \code{\link{generate_alerts}}
#'  \code{\link{detect_loop}}
#'  \code{\link{search_loop}}
#' @rdname trend_line
#' @export 
#' @importFrom stringr str_replace_all
trend_line <- function(
  topic
  , countries=c(1)
  , date_type="created_date"
  , date_min="1900-01-01"
  , date_max="2100-01-01"
  , with_retweets = FALSE
  , location_type = "tweet"
  , alpha = 0.025
  , alpha_outlier = 0.05
  , k_decay = 4
  , no_historic = 7 
  , bonferroni_correction = FALSE
  , same_weekday_baseline = FALSE
  ){

  `%>%` <- magrittr::`%>%`
  # If countries are names they have to be changes to region indexes
  if(is.character(countries) && length(countries) > 0) {
    reg <- get_country_items()
    countries = (1:length(reg))[sapply(1:length(reg), function(i) reg[[i]]$name %in% countries)]
  }
  # defining the environment variable for returning complementary data 
  logenv <- new.env()

  # getting the data with counts and alerts from country counts  
  df <- 
    calculate_regions_alerts(
      topic = topic,
      regions = countries, 
      date_type = date_type, 
      date_min = date_min, 
      date_max = date_max, 
      with_retweets = with_retweets, 
      location_type = location_type, 
      alpha = alpha,
      alpha_outlier = alpha_outlier, 
      k_decay = k_decay,
      no_historic = no_historic, 
      bonferroni_correction = bonferroni_correction,
      same_weekday_baseline = same_weekday_baseline,
      logenv = logenv
    )
  # checking if some data points have been returned or return empty char
  if(nrow(df %>% dplyr::filter(.data$number_of_tweets > 0)) >0) {
    df$topic <- unname(get_topics_labels()[stringr::str_replace_all(topic, "%20", " ")])
    plot_trendline(
      df = df,
      countries = countries,
      topic = topic,
      date_min = date_min,
      date_max = date_max, 
      date_type = date_type, 
      alpha = alpha,
      alpha_outlier = alpha_outlier,
      k_decay = k_decay,
      location_type = location_type, 
      total_count = logenv$total_count
    )
  } else {
    get_empty_chart("No data found for the selected topic, region and period")  
  }
}



# Plot the trend_line chart for shiny app
plot_trendline <- function(df,countries,topic,date_min,date_max, date_type, alpha, alpha_outlier, k_decay, location_type = "tweets", total_count= NA){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  # getting regions and countries 
  regions <- get_country_items()

  #turning off the scientific pen
  old <- options()
  on.exit(options(old))
  options(scipen=999)

  # Calculate alert ranking to avoid alert symbol overlapping 
  # this will be used for stacking points 
  df <- df %>% 
    dplyr::arrange(.data$alert, .data$date, .data$country) %>% 
    dplyr::group_by(.data$alert, .data$date) %>% 
    dplyr::mutate(rank = rank(.data$country, ties.method = "first")) %>% 
    dplyr::ungroup()

  # Getting an alert only dataset for alert markers
  time_alarm <- data.frame(
    date = df$date[which(df$alert == 1)], 
    country = df$country[which(df$alert == 1)], 
    y = vapply(which(df$alert == 1), function(i) df$rank[[i]] * (max(df$limit))/30, double(1)), 
    # adding hover text for alerts
    Details = vapply(which(df$alert == 1), function(i) {
      paste(
        "\nAlert detected", 
        "\nRegion:",df$country[[i]],
        "\nNumber of tweets: ", df$number_of_tweets[[i]], 
        "\nBaseline: ", round(df$baseline[[i]]), 
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
      "\nBaseline: ", round(df$baseline), 
      "\nThreshold: ", round(df$limit), 
      "\nDate:",df$date, 
      "\nKnown users tweets: ", df$known_users, 
      "\nKnown users ratio: ", round(df$known_ratio*100, 2), "%",
      "\nAlpha: ", alpha,
      "\nAlpha outliers: ", alpha_outlier,
      "\nK decay: ", k_decay,
      sep = "")

  # Calculating minimum limit boundary #TODO: changing this behaviour for avoiding shadow reversing
  df$lim_start <- 2* df$baseline - df$limit
  df$lim_start <- ifelse(df$lim_start < 0, 0, df$lim_start)

  # Calculating breaks of y axis
  y_breaks <- unique(floor(pretty(seq(0, (max(df$limit, df$number_of_tweets, na.rm = TRUE, 0) + 1) * 1.1))))

  # Calculating tweet location scope count message
  scope_count <- format(sum(df$number_of_tweets), big.mark = " ", scientific=FALSE)
  total_count <- if(is.na(total_count)) NA else format(total_count, big.mark = " ", scientific=FALSE)
  location_message <- (paste("(n=",scope_count,")", sep = "")) 
  
  # plotting
  fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = .data$date, y = .data$number_of_tweets, label = .data$Details)) +
    # Line
    ggplot2::geom_line(ggplot2::aes(colour=.data$country)) + {
    # Alert Points
      if(nrow(time_alarm) > 0) ggplot2::geom_point(data = time_alarm, mapping = ggplot2::aes(x = .data$date, y = .data$y, colour = .data$country), shape = 2, size = 2) 
    } + {
    # Line shadow
      if(length(df$lim_start[!is.na(df$lim_start)])> 0) ggplot2::geom_ribbon(ggplot2::aes(ymin=.data$lim_start, ymax=.data$limit, colour=.data$country, fill = .data$country), linetype=2, alpha=0.1) 
    } +
    # Title
    ggplot2::labs(
      title=ifelse(length(countries)==1,
        paste0("Number of tweets mentioning ",topic," from ",date_min, " to ",date_max,"\n in ", if(as.integer(countries) == 1) "the world" else regions[[as.integer(countries)]]$name," ", location_message),
        paste0("Number of tweets mentioning ",topic," from ",date_min, " to ",date_max,"\n in multiples regions ", location_message)
      ),
      fill="Countries / Regions",
      color="Countries / Regions"
    ) +
    ggplot2::xlab(paste(if(date_type =="created_weeknum") "Posted week" else "Posted date")) + #, "(days are 24 hour blocks ening on last aggregated tweet in period)")) +
    ggplot2::ylab('Number of tweets') +
    ggplot2::scale_y_continuous(breaks = y_breaks, limits = c(0, max(y_breaks)), expand=c(0 ,0))+
    ggplot2::scale_x_date(
      date_labels = {
          x = df$date
			    # custom logic for x axis
          days <- as.numeric(max(x) - min(x))
          weeks <- days / 7
          years <- days / 365
          # Date format if less or equal then 15 days
			    if(days < 15 && date_type !="created_weeknum") {
            "%Y-%m-%d"
          # Week format if period is between 16 days and 20 weeks
			    #  - Case for period ending on same day of week than period start 
			    } else if(weeks <= 20) {
            "%G-w%V"
          # Month format day of month in period start if period is less or equal to 2 years but more than 20 weeks
          } else if(years <=2) {
            "%Y-%b"
          # Year format if more than 2 years
          } else { 
            "%Y"
          }
			  },
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
    {if(length(unique(df$country))==1) 
      ggplot2::scale_color_manual(values=c("#65B32E"))
    } + 
    {if(length(unique(df$country))==1) 
      ggplot2::scale_fill_manual(values=c("#65B32E"))
    } +
    ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold",lineheight = 0.9),
      axis.text = ggplot2::element_text(colour = "black", size = 8),
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                          margin = ggplot2::margin(-15, 0, 0, 0)),
      axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
      axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 10),
      legend.position=ifelse(length(countries)<2,"none","right")
    )
  
  df <- dplyr::rename(df,"Country" = .data$country)
  

  df <- dplyr::rename(df,"Number of tweets" = .data$number_of_tweets, "Tweet date" = .data$date,"Topic"= .data$topic)
  # returning data and chart
  list("chart" = fig_line, "data" = df) 
}

#' @title Plot the map report on the epitweetr dashboard
#' @description Generates a bubble map plot of number of tweets by countries, for one topic
#' @param topic Character(1) containing the topic to use for the report  
#' @param countries Character vector containing the name of the countries and regions to plot or their respective indexes on the Shiny app, default: c(1)
#' @param date_min Date indicating start of the reporting period, default: "1900-01-01"
#' @param date_max Date indicating end of the reporting period, default: "2100-01-01"
#' @param with_retweets Logical value indicating whether to include retweets in the time series, default: FALSE
#' @param location_type Character(1) vector indicating the location type. Possible values 'tweet', 'user' or 'both', default: 'tweet'
#' @param caption Character(1) vector indicating a caption to print at the bottom of the chart, default: ""
#' @param proj Parameter indicating the CRS (Coordinate Reference System) to use on PROJ4 format \code{\link[sp]{CRS-class}}?
#' If null and all countries are selected +proj=robin is used (Robinson projection) otherwise the Lambert azimuthal equal-area projection will be chosen, default: NULL
#' @param forplotly Logical(1) parameter indicating whether some hacks are activated to improve plotly rendering, default: FALSE
#' @return A named list containing two elements: 'chart' with the ggplot2 figure and 'data' containing the dataframe that was used to build the map.
#' @details Produces a bubble chart map for a particular topic on number of tweets collected based on the provided parameters.
#' The map will display information at country level if more than one country is selected, otherwise it will display bubbles at the smallest possible location identified for each tweet within the period
#' which could be any administrative level or city level.
#' 
#' Tweets associated with a country but with no finer granularity are omitted when displaying a single country.
#'
#' When an aggregated zone is requested, all countries in that zone are included.
#'
#' This functions requires that \code{\link{search_loop}} and \code{\link{detect_loop}} have already been run successfully to show results.
#' @examples 
#' if(FALSE){
#'    #Getting bubble chart for dengue for South America for last 30 days
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    create_map(
#'      topic = "dengue", 
#'      countries = "South America", 
#'      date_min = as.Date(Sys.time())-30, 
#'      date_max=as.Date(Sys.time())
#'    ) 
#' }
#' @seealso 
#'  \code{\link{trend_line}}
#'  \code{\link{create_topwords}}
#'  \code{\link{detect_loop}}
#'  \code{\link{search_loop}}
#'  \code{\link[sp]{spTransform}},\code{\link[sp]{coordinates}},\code{\link[sp]{is.projected}},\code{\link[sp]{CRS-class}}
#'  \code{\link[ggplot2]{fortify}},\code{\link[ggplot2]{geom_polygon}},\code{\link[ggplot2]{geom_point}}
#' @rdname create_map
#' @export 
#' @importFrom magrittr `%>%`
#' @importFrom dplyr filter rename select bind_rows group_by summarize ungroup mutate dense_rank
#' @importFrom sp spTransform coordinates proj4string CRS
#' @importFrom ggplot2 fortify theme element_text element_blank element_rect ggplot geom_polygon aes geom_point scale_size_continuous scale_fill_manual coord_fixed labs theme_classic
#' @importFrom stats setNames 
create_map <- function(topic=c(),countries=c(1), date_min="1900-01-01",date_max="2100-01-01", with_retweets = FALSE, location_type = "tweet", caption = "", proj = NULL, forplotly=FALSE){
  # Importing pipe operator
  `%>%` <- magrittr::`%>%`
  # Setting the scientific pen off
  old <- options()
  on.exit(options(old))
  options(scipen=999)

  # getting all regiond
  regions <- get_country_items()
  
  # If countries are names they have to be changes to region indexes
  if(is.character(countries) && length(countries) > 0) {
    countries = (1:length(regions))[sapply(1:length(regions), function(i) regions[[i]]$name %in% countries)]
  }
  country_codes <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(countries), function(i) unlist(regions[[i]]$codes)))
  
  # Setting a variable for choosing if going to subnational level when only one country is required
  detailed <- length(country_codes) == 1

  # Getting the aggregated data based on the national or subnational level
  df <- (
    if(!detailed) 
      get_aggregates(dataset = "country_counts", filter = list(topic = topic, period = list(date_min, date_max)))
    else 
      get_aggregates(dataset = "geolocated", filter = list(topic = topic, period = list(date_min, date_max)))
  )

  # retunrning empty chart if no data is found
  if(nrow(df)==0) {
    return(get_empty_chart("No data found for the selected topic, region and period"))
  }
  
  #filtering data by topic and date and country_codes
  f_topic <- topic
  df <- (df %>% 
    dplyr::filter(
        .data$topic==f_topic
        & (!is.na(.data$tweet_geo_country_code)
           |  !is.na(.data$user_geo_country_code)
          )
        & .data$created_date >= date_min 
        & .data$created_date <= date_max
        & (
            (
              if(length(country_codes) == 0) TRUE 
              else .data$tweet_geo_country_code %in% country_codes
            )
          |
            (
              if(length(country_codes) == 0) TRUE 
              else .data$user_geo_country_code %in% country_codes
            )
          )
    )
  )

  # Adding retweets if requested
  if(with_retweets)
    df$tweets <- ifelse(is.na(df$retweets), 0, df$retweets) + ifelse(is.na(df$tweets), 0, df$tweets)
  
  # Ensuring geo_name column exists for tooltips and setting default value (necessary for data created before version < 0.1.7)
  if(detailed && !("user_geo_name" %in% colnames(df))) {
     df$user_geo_name <- df$user_geo_code 
     df$tweet_geo_name <- df$tweet_geo_code 
  }	else if(detailed) {
     df$tweet_geo_name <- ifelse(df$tweet_geo_name == "" | is.na(df$tweet_geo_name), df$tweet_geo_code, df$tweet_geo_name) 
     df$user_geo_name <- ifelse(df$user_geo_name == "" | is.na(df$user_geo_name), df$user_geo_code, df$user_geo_name) 
  }
  # Getting global tweet count for title 
  scope_count <- (
     if(location_type =="tweet" && !detailed) 
       sum(
         (df %>% 
            dplyr::filter(
              !is.na(.data$tweet_geo_country_code) 
              &(
                .data$tweet_geo_country_code %in% country_codes 
                | length(country_codes) == 0
                )
            )
         )$tweets
       )
     else if(location_type == "user" && !detailed) 
       sum(
         (df %>% 
            dplyr::filter(
              !is.na(.data$user_geo_country_code) 
              &( 
               .data$user_geo_country_code %in% country_codes 
               | length(country_codes) == 0
              )
            )
          )$tweets
        )
     else if(!detailed) NA
     else if(location_type =="tweet") sum((df %>% dplyr::filter((!(.data$tweet_geo_code %in% country_codes )) & (.data$tweet_geo_country_code %in% country_codes | length(country_codes) == 0)))$tweets)
     else if(location_type == "user") sum((df %>% dplyr::filter((!(.data$user_geo_code %in% country_codes )) & (.data$user_geo_country_code %in% country_codes | length(country_codes) == 0)))$tweets)
     else NA 
  )

  total_count <- (
     if(!detailed) sum(df$tweets)
     else sum((df %>% dplyr::filter((!(.data$tweet_geo_code %in% country_codes )) & !(.data$user_geo_code %in% country_codes )))$tweets)
  )

  # Setting country codes as requested location types as requested
  # this is to deal with location type and aggregation level (national vs subnational) 
  df <- (
         if(location_type =="tweet" && !detailed)
           df %>% dplyr::rename(country_code = .data$tweet_geo_country_code) %>% dplyr::select(-.data$user_geo_country_code)
         else if(location_type == "user" && !detailed)
           df %>% dplyr::rename(country_code = .data$user_geo_country_code) %>% dplyr::select(-.data$tweet_geo_country_code)
         else if(!detailed) dplyr::bind_rows( #Dealuing with avoiduing dupplication when requeting both user and tweet location
           df %>% dplyr::rename(country_code = .data$tweet_geo_country_code) %>% dplyr::filter(!is.na(.data$country_code)) %>% dplyr::select(-.data$user_geo_country_code),
           df %>% 
             dplyr::rename(country_code = .data$user_geo_country_code) %>% 
             dplyr::filter(!is.na(.data$country_code) & .data$country_code != .data$tweet_geo_country_code ) %>% 
             dplyr::select(-.data$tweet_geo_country_code)
         )
         else if(location_type =="tweet")
           df %>% 
             dplyr::rename(country_code = .data$tweet_geo_country_code, geo_code = .data$tweet_geo_code, geo_name = .data$tweet_geo_name, longitude = .data$tweet_longitude, latitude = .data$tweet_latitude) %>% 
             dplyr::select(-.data$user_geo_country_code, -.data$user_geo_code, -.data$user_geo_name, -.data$user_longitude, -.data$user_latitude)
         else if(location_type == "user")
           df %>% 
             dplyr::rename(country_code = .data$user_geo_country_code, geo_code = .data$user_geo_code, geo_name = .data$user_geo_name, longitude = .data$user_longitude, latitude = .data$user_latitude) %>% 
             dplyr::select(-.data$tweet_geo_country_code, -.data$tweet_geo_code, -.data$tweet_geo_name, -.data$tweet_longitude, -.data$tweet_latitude)
         else dplyr::bind_rows( #Dealuing with avoiduing dupplication when requeting both user and tweet location
           df %>% 
             dplyr::rename(country_code = .data$tweet_geo_country_code, geo_code = .data$tweet_geo_code, geo_name = .data$tweet_geo_name, longitude = .data$tweet_longitude, latitude = .data$tweet_latitude) %>% 
             dplyr::select(-.data$user_geo_country_code, -.data$user_geo_code, -.data$user_geo_name, -.data$user_longitude, -.data$user_latitude),
           df %>%
             dplyr::rename(country_code = .data$user_geo_country_code, geo_code = .data$user_geo_code, geo_name = .data$user_geo_name, longitude = .data$user_longitude, latitude = .data$user_latitude) %>% 
             dplyr::filter(!is.na(.data$country_code) & is.na(.data$tweet_geo_country_code )) %>%
             dplyr::select(-.data$tweet_geo_country_code, -.data$tweet_geo_code, -.data$tweet_geo_name, -.data$tweet_longitude, -.data$tweet_latitude)
         )     
    )

  #Applying country filter after country type
  df <- (df %>% 
    dplyr::filter(
        !is.na(.data$country_code)
        & (
          if(length(country_codes) == 0) TRUE 
          else .data$country_code %in% country_codes
        )
        & .data$tweets > 0 
        & (
          if(detailed) !(.data$geo_code %in% country_codes)
          else TRUE
        ) 
    )
  )
  # aggregating by country or geo code depending on national or subnational level
  df <- (
    if(detailed) 
      df %>% 
        dplyr::group_by(.data$country_code, .data$geo_code) %>%
        dplyr::summarize(count = sum(.data$tweets), Long = mean(.data$longitude), Lat = mean(.data$latitude), geo_name = max(.data$geo_name, na.rm = TRUE)) %>%
        dplyr::ungroup() 
    else 
      df %>% 
        dplyr::group_by(.data$country_code) %>%
        dplyr::summarize(count = sum(.data$tweets)) %>%
        dplyr::ungroup() 
  )
  # returning an empty chart if no rows are found
  if(nrow(df)==0) {
    return(get_empty_chart("No data found for the selected topic, region and period"))
  }

  # Adding country properties (bounding boxes and country names)
  regions <- get_country_items()
  map <- get_country_index_map()
  df$Country <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$name else NA)
  if(!detailed) {
    df$Long <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) mean(c(regions[[i]]$minLong,regions[[i]]$maxLong)) else NA)
    df$Lat <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) mean(c(regions[[i]]$minLat,regions[[i]]$maxLat)) else NA)
  }
  df$MinLat <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$minLat else NA)
  df$MaxLat <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$maxLat else NA)
  df$MinLong <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$minLong else NA)
  df$MaxLong <- sapply(unname(map[df$country_code]), function(i) if(!is.na(i)) regions[[i]]$maxLong else NA)
  
  #Calculating the center of the map
  min_long <- min(df$MinLong, na.rm = TRUE)
  max_long <- max(df$MaxLong, na.rm = TRUE)
  min_lat <- min(df$MinLat, na.rm = TRUE)
  max_lat <- max(df$MaxLat, na.rm = TRUE)
  lat_center <- mean(c(min_lat, max_lat))
  long_center <- mean(c(min_long, max_long))

  # Getting the projection to use which will center the global bounding box
  full_world <- (1 %in% countries || 2 %in% countries)
  proj <- (
    if(!is.null(proj)) 
      proj
    else if(full_world) 
      # Using Robinson projection for world map  
      "+proj=robin" 
    else
      # Using projection Lambert Azimuthal Equal Area for partial maps
      paste("+proj=laea", " +lon_0=", long_center, " +lat_0=", lat_center ,sep = "") 
  )
  # Projecting the country counts data frame on the target coordinate system this projected data frame contains the bubble X,Y coordinates
  proj_df <- as.data.frame(
    sp::spTransform(
      {
        x <- df %>% dplyr::filter(!is.na(.data$Long) & !is.na(.data$Lat))
        sp::coordinates(x)<-~Long+Lat
        sp::proj4string(x) <- sp::CRS("+proj=longlat +datum=WGS84")
        x
      }, 
      sp::CRS(proj)
    )
  )
  # Extracting country polygons from naturalraearth data
  countries_geo <- rnaturalearthdata::countries50 
  
  # Projecting country polygons on target coordinate system
  countries_proj <- as.data.frame(
    sp::spTransform(
      {
        x <- ggplot2::fortify(countries_geo)
        sp::coordinates(x)<-~long+lat
        sp::proj4string(x) <- sp::CRS("+proj=longlat +datum=WGS84")
        x
      }, 
      sp::CRS(proj)
    )
  )
  #countries_proj = rgeos::gBuffer(countries_proj, width=0, byid=TRUE)
  
  # Extracting ISO codes for joining with country codes
  codemap <- setNames(countries_geo$iso_a2, as.character(1:nrow(countries_geo) - 1))
  # Extracting Country names for joining with country codes
  namemap <- setNames(countries_geo$name, as.character(1:nrow(countries_geo) - 1))
  # Getting original coordinate system for filtering points
  countries_non_proj <-  ggplot2::fortify(countries_geo)

  # Joining projects map data frame with codes and names
  countries_proj_df <- ggplot2::fortify(countries_proj) %>%
    # Renaming projected long lat tp x y
    dplyr::rename(x = .data$long, y = .data$lat) %>%
    # Adding original coordinates
    dplyr::mutate(long = countries_non_proj$long, lat = countries_non_proj$lat) %>%
    # Adding country codes
    dplyr::mutate(ISO_A2 = codemap[.data$id], name = namemap[.data$id]) %>%
    # Getting colors of selected regions
    dplyr::mutate(selected = ifelse(.data$ISO_A2 %in% country_codes, "a. Selected",ifelse(!.data$hole,  "b. Excluded",  "c. Lakes"))) %>%
    # Filtering out elements out of drawing area
    dplyr::filter(
      .data$long >= min_long -20 
      & .data$long <= max_long + 20 
      & .data$lat >= min_lat -20 
      & .data$lat <= max_lat + 20
    ) 
  
  # Getting selected countries projected bounding boxes
  map_limits <- countries_proj_df %>% 
    dplyr::filter(
      .data$long >= min_long  
      & .data$long <= max_long 
      & .data$lat >= min_lat 
      & .data$lat <= max_lat
    ) 

  minX <- min(map_limits$x)
  maxX <- max(map_limits$x)
  minY <- min(map_limits$y)
  maxY <- max(map_limits$y)
  
  # Calculating counts groups for Legend
  maxCount <- max(df$count)
  cutsCandidates <- unique(sapply(c(maxCount/50, maxCount/20, maxCount/5, maxCount), function(v) max(1, ceiling((v/(10 ^ floor(log10(v)))))* (10 ^ floor(log10(v))))))
  proj_df$countGroup <- sapply(proj_df$count, function(c) min(cutsCandidates[c <= cutsCandidates]))
  proj_df$plotlycuts <- paste(letters[dplyr::dense_rank(proj_df$countGroup)+3], ". " ,proj_df$countGroup, sep = "")
  cuts <- sort(unique(proj_df$countGroup))
  plotlycuts <- sort(unique(proj_df$plotlycuts))
  #Creating tooltip 
  countries_proj_df$Details <- 
    paste(
      "\nRegion:",countries_proj_df$name,
      sep = ""
    )

  proj_df$Details <- 
    paste(
      "\nRegion:", proj_df$Country,
      "\nNumber of Tweets:", proj_df$count,
      if(detailed) "\nLocation: " else "",
      if(detailed) proj_df$geo_name else "",
      sep = ""
    )

  
  # Defining the chart theme
  theme_opts <- list(ggplot2::theme(
	  plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold"),
	  plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 12),
    axis.text = ggplot2::element_text(colour = "black", size = 8),
    panel.grid.minor = ggplot2::element_blank(),
    panel.grid.major = ggplot2::element_blank(),
    panel.background = ggplot2::element_blank(),
    plot.background = ggplot2::element_rect(fill="white"),
    panel.border = ggplot2::element_blank(),
    axis.line = ggplot2::element_blank(),
    axis.text.x = ggplot2::element_blank(),
    axis.text.y = ggplot2::element_blank(),
    axis.ticks = ggplot2::element_blank(),
    axis.title.x = ggplot2::element_blank(),
    axis.title.y = ggplot2::element_blank(),
    legend.direction="horizontal",
    legend.position = "bottom"
  ))

  # creating the plot
  fig <- ggplot2::ggplot(df) + 
    ggplot2::geom_polygon(data=countries_proj_df, ggplot2::aes(.data$x,.data$y, group=.data$group, fill=.data$selected, label = .data$Details)) + # background
    ggplot2::geom_polygon(data=countries_proj_df, ggplot2::aes(.data$x,.data$y, group=.data$group, fill=.data$selected, label = .data$Details), color ="#3f3f3f", size=0.3) + # lines
    (if(forplotly) # customistion for plotly legend
      ggplot2::geom_point(data=proj_df, ggplot2::aes(.data$Long, .data$Lat, size=.data$count, fill=.data$plotlycuts, label = .data$Details), color="#65B32E", alpha=I(8/10))
     else
      ggplot2::geom_point(data=proj_df, ggplot2::aes(.data$Long, .data$Lat, size=.data$count), fill="#65B32E", color="#65B32E", alpha=I(8/10))
    ) + 
    ggplot2::scale_size_continuous(
      name = "Number of tweets", 
      breaks = {x = cuts; x[length(x)]=maxCount;x},
      labels = cuts
    ) +
    ggplot2::scale_fill_manual(
      values = c("#C7C7C7", "#E5E5E5" , "white", sapply(cuts, function(c) "#65B32E")), 
      breaks = c("a. Selected", "b. Excluded", "c. Lakes", plotlycuts),
      guide = "none"
    ) +
    ggplot2::coord_fixed(ratio = 1, ylim=if(full_world) NULL else c(minY, maxY), xlim=if(full_world) NULL else c(minX, maxX)) +
    ggplot2::labs(
       title = (
         if(location_type == "both")
           paste(
             "Geographical distribution of tweets mentioning ", 
             topic,
             "\nfrom ",date_min, " to ",date_max, 
             "\nwith user and tweet location (n=",
             format(total_count, big.mark = " ", scientific=FALSE),
             ")",
             sep = ""
           )
         else
           paste(
             "Geographical distribution of tweets mentioning ", 
             topic, 
             "\nfrom ",date_min, " to ",date_max, 
             "\nwith ", 
             location_type,
             " location (n=" ,
             format(scope_count, big.mark = " ", scientific=FALSE),
             ")",
             sep = ""
           )
       ),
       caption = paste(caption, ". Projection: ", proj, sep = "")
    ) +
    ggplot2::theme_classic(base_family = get_font_family()) +
    theme_opts

  # returning the chart and the data
  list("chart" = fig, "data" = df) 
}

#' @title Plot the top words report on the epitweetr dashboard
#' @description Generates a bar plot of most popular words in tweets, for one topic
#' @param topic Character(1) containing the topic to use for the report
#' @param country_codes Character vector containing the ISO 3166-1 alpha-2 countries to plot, default: c()
#' @param date_min Date indicating start of the reporting period, default: "1900-01-01"
#' @param date_max Date indicating end of the reporting period, default: "2100-01-01"
#' @param with_retweets Logical value indicating whether to include retweets in the time series, default: FALSE
#' @param location_type Character(1) this parameter is currently being IGNORED since this report shows only tweet location and cannot show user or both locations for performance reasons, default: 'tweet'
#' @param top numeric(1) Parameter indicating the number of words to show, default: 25
#' @return A named list containing two elements: 'chart' with the ggplot2 figure and 'data' containing the data frame that was used to build the map.
#' @details Produces a bar chart showing the occurrences of the most popular words in the collected tweets based on the provided parameters.
#' For performance reasons on tweet aggregation this report only shows tweet location and ignores the location_type parameter
#' 
#' This report may be empty for combinations of countries and topics with very few tweets since for performance reasons, the calculation of top words is an approximation using chunks of 10.000 tweets.
#'
#' This functions requires that \code{\link{search_loop}} and \code{\link{detect_loop}} have already been run successfully to show results.
#' @examples 
#' if(FALSE){
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    #Getting topword chart for dengue for France, Chile, Australia for last 30 days
#'    create_topwords(
#'      topic = "dengue", 
#'      country_codes = c("FR", "CL", "AU"),
#'      date_min = as.Date(Sys.time())-30, 
#'      date_max=as.Date(Sys.time())
#'    ) 
#'  }
#' @seealso 
#'  \code{\link{trend_line}}
#'  \code{\link{create_map}}
#'  \code{\link{detect_loop}}
#'  \code{\link{search_loop}}
#' @export 
#' @importFrom magrittr `%>%`
#' @importFrom dplyr filter group_by summarize ungroup arrange mutate
#' @importFrom ggplot2 ggplot aes geom_col xlab coord_flip labs scale_y_continuous theme_classic theme element_text margin element_blank
#' @importFrom stats reorder
#' @importFrom utils head
#' 
create_topwords <- function(topic,country_codes=c(),date_min="1900-01-01",date_max="2100-01-01", with_retweets = FALSE, location_type = "tweet", top = 25) {
  create_topchart(topic = topic, serie = "topwords", country_codes = country_codes, date_min = date_min, date_max = date_max, location_type = location_type, top = top)
}

#' @title Plot the top elements for a specific series on the epitweetr dashboard
#' @description Generates a bar plot of most popular elements in tweets, for one topic. Top elements among ("topwords", "hashtags", "entities", "contexts", "urls")
#' @param topic Character(1) containing the topic to use for the report
#' @param serie Character(1) name of the series to be used for the report. It should be one of ("topwords", "hashtags", "entities", "contexts", "urls")
#' @param country_codes Character vector containing the ISO 3166-1 alpha-2 countries to plot, default: c()
#' @param date_min Date indicating start of the reporting period, default: "1900-01-01"
#' @param date_max Date indicating end of the reporting period, default: "2100-01-01"
#' @param with_retweets Logical value indicating whether to include retweets in the time series, default: FALSE
#' @param location_type Character(1) this parameter is currently being IGNORED since this report shows only tweet location and cannot show user or both locations for performance reasons, default: 'tweet'
#' @param top numeric(1) Parameter indicating the number of words to show, default: 25
#' @return A named list containing two elements: 'chart' with the ggplot2 figure and 'data' containing the data frame that was used to build the map.
#' @details Produces a bar chart showing the occurrences of the most popular words in the collected tweets based on the provided parameters.
#' For performance reasons on tweet aggregation, this report only shows tweet location and ignores the location_type parameter
#' 
#' This report may be empty for combinations of countries and topics with very few tweets since for performance reasons, the calculation of top words is an approximation using chunks of 10.000 tweets.
#'
#' This functions requires that \code{\link{search_loop}} and \code{\link{detect_loop}} have already been run successfully to show results.
#' @examples 
#' if(FALSE){
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    #Getting topword chart for dengue for France, Chile, Australia for last 30 days
#'    create_topchart(
#'      topic = "dengue", 
#'      serie = "topwords", 
#'      country_codes = c("FR", "CL", "AU"),
#'      date_min = as.Date(Sys.time())-30, 
#'      date_max=as.Date(Sys.time())
#'    ) 
#'  }
#' @seealso 
#'  \code{\link{trend_line}}
#'  \code{\link{create_map}}
#'  \code{\link{detect_loop}}
#'  \code{\link{search_loop}}
#' @export 
#' @importFrom magrittr `%>%`
#' @importFrom dplyr filter group_by summarize ungroup arrange mutate
#' @importFrom ggplot2 ggplot aes geom_col xlab coord_flip labs scale_y_continuous theme_classic theme element_text margin element_blank
#' @importFrom stats reorder
#' @importFrom utils head
#' 
create_topchart <- function(topic, serie, country_codes=c(),date_min="1900-01-01",date_max="2100-01-01", with_retweets = FALSE, location_type = "tweet", top = 25) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  f_topic <- topic
  dataset <- serie

  # Getting the right top field depending on the series 
  top_field <- (
    if(serie == "topwords") "token"
    else if(serie == "entities") "entity"
    else if(serie == "hashtags") "hashtag"
    else if(serie == "contexts") "context"
    else if(serie == "urls") "url"
    else "top"
  )

  # getting the data from topwords series
  filter <- (
    if(length(country_codes) > 0)
      list(topic = f_topic, period = list(date_min, date_max), tweet_geo_country_code = country_codes)
    else 
      list(topic = f_topic, period = list(date_min, date_max))
  )
  df <- get_aggregates(dataset = dataset, filter =filter , top_field = top_field, top_freq = "frequency")

  # renaming the series depending column to "top"  
  df <- df %>% dplyr::rename(top = !!as.symbol(top_field))

  # getting the series dependant title part  
  serie_title <- (
    if(serie == "topwords") "words"
    else serie
  )
  if(nrow(df)==0 || is.null(df)) {
    return(get_empty_chart("No data found for the selected topic, region and period"))
  }
  #filtering data by countries
  df <- (df
      %>% dplyr::filter(
        .data$topic == f_topic 
        & .data$created_date >= date_min 
        & .data$created_date <= date_max
        & (if(length(country_codes)==0) TRUE else .data$tweet_geo_country_code %in% country_codes )
      ))
 
  # dealing with retweets if requested
  if(!with_retweets) df$frequency <- df$original

  # grouping by top and limiting as requested
  df <- (df
      %>% dplyr::filter(!is.na(.data$frequency))
      %>% dplyr::group_by(.data$top)
      %>% dplyr::summarize(frequency = sum(.data$frequency))
      %>% dplyr::ungroup() 
      %>% dplyr::arrange(-.data$frequency) 
      %>% head(top)
      %>% dplyr::mutate(top = reorder(.data$top, .data$frequency))
  )
  if(nrow(df)==0) {
    return(get_empty_chart("No data found for the selected topic, region and period"))
  }
  # Calculating breaks for y axis
  y_breaks <- unique(floor(pretty(seq(0, (max(df$frequency) + 1) * 1.1))))
  
  # removing scientific pen
  old <- options()
  on.exit(options(old))
  options(scipen=999)

  # plotting
  fig <- (
      df %>% ggplot2::ggplot(ggplot2::aes(x = .data$top, y = .data$frequency)) +
           ggplot2::geom_col(fill = "#65B32E") +
           ggplot2::xlab(NULL) +
           ggplot2::coord_flip(expand = FALSE) +
           ggplot2::labs(
              y = "Count",
              title = paste("Top ", serie_title, " of tweets mentioning", topic),
              subtitle = paste("from", date_min, "to", date_max),
              caption = "Top ", serie_title, " figure only considers tweet location, ignoring the location type parameter"
           ) +
           ggplot2::scale_y_continuous(labels = function(x) format(x, scientific = FALSE), breaks = y_breaks, limits = c(0, max(y_breaks)), expand=c(0 ,0))+
           ggplot2::theme_classic(base_family = get_font_family()) +
           ggplot2::theme(
	           plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold"),
	           plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 12),
             axis.text = ggplot2::element_text(colour = "black", size = 8),
             axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, margin = ggplot2::margin(0, 0, 0, 0)),
             axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
             axis.line.y = ggplot2::element_blank(),
             axis.ticks.y = ggplot2::element_blank()
           )
    )
  # returning chart and data
  list("chart" = fig, "data" = df) 
}



# Helper function to get the font family
get_font_family <- function(){
  if(.Platform$OS.type == "windows" && is.null(grDevices::windowsFonts("Helvetica")[[1]]))
    grDevices::windowsFonts(Helvetica = grDevices::windowsFont("Helvetica"))
  "Helvetica"
}

# Returns an empty chart with provided message on title
get_empty_chart <- function(title) {
  chart <- ggplot2::ggplot() + ggplot2::theme_minimal(base_family = get_font_family() ) + ggplot2::labs(title = title)  
  df <- chart$data
  list("chart" = chart, "data" = df) 

}
