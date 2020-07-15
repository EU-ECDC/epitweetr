
#' Generates the trend line report with alerts
#'
#' @param s_topic 
#' @param s_country 
#' @param date_type 
#' @param geo_country_code 
#' @param date_min 
#' @param date_max 
#' @export
#'
trend_line <- function(
  topic
  , countries=c(1)
  , date_type="created_date"
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
      date_type = date_type, 
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
  plot_trendline(df,countries,topic,date_min,date_max, date_type)
}



#' Plot trend_line
#'
#' @param df 
#' @export
plot_trendline <- function(df,countries,topic,date_min,date_max, date_type){
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
      sep = "")
  # Calculating minimum limit boundary 
  df$lim_start <- 2* df$baseline - df$limit
  df$lim_start <- ifelse(df$lim_start < 0, 0, df$lim_start)

  # Calculating breaks
  y_breaks <- unique(floor(pretty(seq(0, (max(df$limit) + 1) * 1.1))))

  fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets, label = Details)) +
    # Line
    ggplot2::geom_line(ggplot2::aes(colour=country)) + {
    # Alert Points
      if(nrow(time_alarm) > 0) ggplot2::geom_point(data = time_alarm, mapping = ggplot2::aes(x = date, y = y, colour = country), shape = 2, size = 2) 
    } +
    # Line shadow
    ggplot2::geom_ribbon(ggplot2::aes(ymin=lim_start, ymax=limit, colour=country, fill = country), linetype=2, alpha=0.1) +
    # Title
    ggplot2::labs(
      title=ifelse(length(countries)==1,
        paste0("Number of tweets mentioning ",topic,"\n from ",date_min, " to ",date_max," in ", regions[[as.integer(countries)]]$name),
        paste0("Number of tweets mentioning ",topic,"\n from ",date_min, " to ",date_max," in multiples regions")
      )
    ) +
    ggplot2::xlab(paste(if(date_type =="created_weeknum") "Posted week" else "Posted date", "(days are 24 hour blocks ening on last aggregated tweet in period)")) +
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
          # Year formar if more than 2 years
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
#' @param date_type 
#' @param date_min 
#' @param date_max 
#'
#' @export
create_map <- function(topic=c(),countries=c(1),date_type="created_date",date_min="1900-01-01",date_max="2100-01-01", with_retweets = FALSE, location_type = "tweet", caption = "", proj = NULL, forplotly=F){
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
        & tweets > 0 
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
  
  #Calculating the center of the map
  min_long <- min(df$MinLong, na.rm = TRUE)
  max_long <- max(df$MaxLong, na.rm = TRUE)
  min_lat <- min(df$MinLat, na.rm = TRUE)
  max_lat <- max(df$MaxLat, na.rm = TRUE)
  lat_center <- mean(c(min_lat, max_lat))
  long_center <- mean(c(min_long, max_long))

  # Getting the projection to use which will be centered the global bounding box
  full_world <- (1 %in% countries)
  proj <- (
    if(!is.null(proj)) 
      proj
    else if(full_world) 
      # Using Robinson projection for world map  
      "+proj=robin" 
    else
      # Using projection Lambert Azimuthal Equal Area for partial maps
      paste("+proj=laea", " +lon_0=", long_center, " +lat_0=", lat_center ,sep = "") 
      #paste("+proj=laea", " +lon_0=", long_center, " +lat_0=", lat_center, " +x0=4321000", " +y0=3210000 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs", sep = "") 
  )
  # Projecting the country counts dataframe on the target coordinate system this projected dataframe contains the bubble X,Y coordinates
  proj_df <- as.data.frame(
    sp::spTransform(
      {
        x <- df %>% dplyr::filter(!is.na(Long) & !is.na(Lat))
        sp::coordinates(x)<-~Long+Lat
        sp::proj4string(x) <- sp::CRS("+proj=longlat +datum=WGS84")
        x
      }, 
      sp::CRS(proj)
    )
  )
  # Extracting country polygones from naturalraearth dat
  countries_geo <- rnaturalearthdata::countries50 
  
  # Projecting country polygones on target coordinate system
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
  message(proj)
  #countries_proj = rgeos::gBuffer(countries_proj, width=0, byid=TRUE)
  
  # Extracting ISO codes for joining with country codes
  codemap <- setNames(countries_geo$iso_a2, as.character(1:nrow(countries_geo) - 1))
  # Extracting Country names for joining with country codes
  namemap <- setNames(countries_geo$name, as.character(1:nrow(countries_geo) - 1))
  # Getting original coordinate system for filtering points
  countries_non_proj <-  ggplot2::fortify(countries_geo)

  # Joining projectes map dataframe with codes and names
  countries_proj_df <- ggplot2::fortify(countries_proj) %>%
    # Renaming projected long lat tp x y
    dplyr::rename(x = long, y = lat) %>%
    # Adding original coordinates
    dplyr::mutate(long = countries_non_proj$long, lat = countries_non_proj$lat) %>%
    # Adding country codes
    dplyr::mutate(ISO_A2 = codemap[id], name = namemap[id]) %>%
    # Getting colors of selected regions
    dplyr::mutate(selected = ifelse(ISO_A2 %in% country_codes, "a. Selected",ifelse(!hole,  "b. Excluded",  "c. Lakes"))) %>%
    # Filtering out elements out of drawing area
    dplyr::filter(long >= min_long -20 & long <= max_long + 20 & lat >= min_lat -20 & lat <= max_lat + 20) 
  
  message(paste(min_long, max_long, min_lat, max_lat))
  # Getting selected countries projected bounding boxes
  map_limits <- countries_proj_df %>% dplyr::filter(long >= min_long  & long <= max_long & lat >= min_lat & lat <= max_lat) 
  minX <- min(map_limits$x)
  maxX <- max(map_limits$x)
  minY <- min(map_limits$y)
  maxY <- max(map_limits$y)
  
  message(paste(minX, maxX, minY, maxY))
 
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
    axis.title.y = ggplot2::element_blank()
  ))
  fig <- ggplot2::ggplot() + 
    ggplot2::geom_polygon(data=countries_proj_df, ggplot2::aes(x,y, group=group, fill=selected, label = Details)) + 
    ggplot2::geom_polygon(data=countries_proj_df, ggplot2::aes(x,y, group=group, fill=selected, label = Details), color ="#3f3f3f", size=0.3) + 
    (if(forplotly) 
      ggplot2::geom_point(data=proj_df, ggplot2::aes(Long, Lat, size=count, fill=plotlycuts, label = Details), color="#65B32E", alpha=I(8/10))
     else
      ggplot2::geom_point(data=proj_df, ggplot2::aes(Long, Lat, size=count), fill="#65B32E", color="#65B32E", alpha=I(8/10))
    ) + 
    ggplot2::scale_size_continuous(
      name = "Number of tweets", 
      breaks = {x = cuts; x[length(x)]=maxCount;x},
      labels = cuts
    ) +
    ggplot2::scale_fill_manual(
      values = c("#C7C7C7", "#E5E5E5" , "white", sapply(cuts, function(c) "#65B32E")), 
      breaks = c("a. Selected", "b. Excluded", "c. Lakes", plotlycuts),
      guide = FALSE
    ) +
    (if(!full_world) ggplot2::coord_fixed(ratio = 1, ylim=c(minY, maxY), xlim=c(minX, maxX)) ) +
    ggplot2::labs(
       title = paste("Tweets by territory mentioning", topic, "\nfrom", date_min, "to", date_max),
       caption = paste(caption, ". Projection: ", proj, sep = "")
    ) +
    ggplot2::theme_classic(base_family = get_font_family()) +
    theme_opts

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
  # Calculating breaks
  y_breaks <- unique(floor(pretty(seq(0, (max(df$frequency) + 1) * 1.1))))
  
  fig <- (
      df %>% ggplot2::ggplot(ggplot2::aes(x = tokens, y = frequency)) +
           ggplot2::geom_col(fill = "#65B32E") +
           ggplot2::xlab(NULL) +
           ggplot2::coord_flip(expand = FALSE) +
           ggplot2::labs(
              y = "Count",
              title = paste("Top words of tweets mentioning", topic),
              subtitle = paste("from", date_min, "to", date_max),
              caption = "Top words is the only figure in the dashboard only considering tweet location (ignores the location type parameter)"
           ) +
           ggplot2::scale_y_continuous(labels = function(x) format(x, scientific = FALSE), breaks = y_breaks, limits = c(0, max(y_breaks)), expand=c(0 ,0))+
           ggplot2::theme_classic(base_family = get_font_family()) +
           ggplot2::theme(
	           plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold"),
	           plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 12),
             axis.text = ggplot2::element_text(colour = "black", size = 8),
             axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, margin = ggplot2::margin(-15, 0, 0, 0)),
             axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
             axis.line.y = ggplot2::element_blank(),
             axis.ticks.y = ggplot2::element_blank()
           )
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
