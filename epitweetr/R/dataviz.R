
#' Search for all tweets on topics defined on configuration
#' @export
#colors to improve readability for those who are colour-blind
cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")




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
data_treatment <- function(df,s_topic,s_country, date, geo_country_code, date_min="1900-01-01",date_max="2100-01-01" ){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%` 
   #Select grouping variables
  if(length(s_country) > 0){
    to_group <- c("date","geo_country_code","topic")}
  else{
    to_group <-c("date","topic")}
  #Renaming date and geo_country_code (tweet or user) ! This must be changed, if user
  colnames(df)[colnames(df)== geo_country_code] <- "geo_country_code"
  colnames(df)[colnames(df)== date] <- "date"
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
  date_df <- tibble::tibble()
  if(length(s_country)>0){
  for(i in 1:length(s_country)){
    date_df <- rbind(date_df,cbind.data.frame("dates" = dates,"geo_country_code"= rep(s_country[i],length(dates)),"topic" = rep(s_topic,length(dates))))
  }
  
  df <- (df
         %>% dplyr::full_join(date_df, by = c("date"="dates","geo_country_code"="geo_country_code","topic"="topic")))
  df$tweets <- replace(df$tweets, is.na(df$tweets),0)
  
  df <- (df
         %>% dplyr::group_by_at(to_group)
         %>% dplyr::filter(!is.na(geo_country_code) && !is.na(date))
         %>% dplyr::filter(date >= date_min && date <= date_max)
         %>% dplyr::summarise(number_of_tweets = sum(tweets)) 
         %>% dplyr::arrange(desc(number_of_tweets)) 
         %>% dplyr::filter(topic==s_topic )
         %>% dplyr::filter(geo_country_code %in% s_country )
         %>% dplyr::ungroup()
  )
  } else {
    date_df <- rbind(date_df,cbind.data.frame("dates" = dates,"topic" = rep(s_topic,length(dates))))
  
    df <- (df
           %>% dplyr::full_join(date_df, by = c("date"="dates","topic"="topic")))
    df$tweets <- replace(df$tweets, is.na(df$tweets),0)
    df <- (df
           %>% dplyr::group_by_at(to_group)
           %>% dplyr::filter(!is.na(date))
           %>% dplyr::filter(date >= date_min && date <= date_max)
           %>% dplyr::summarise(number_of_tweets = sum(tweets)) 
           %>% dplyr::arrange(desc(number_of_tweets)) 
           %>% dplyr::filter(topic==s_topic )
           %>% dplyr::ungroup()
    )
  }
  if(date=="created_weeknum"){
    df$date <-as.Date(paste(df$date,"1"),"%Y%U %u")
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
plot_trendline <- function(df,s_country,s_topic){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  if(length(s_country)>0){ #if several countries selected, aggregation by country
    df <- dplyr::rename(df,country = geo_country_code)
    fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets)) +
      ggplot2::geom_line(ggplot2::aes(colour=country, group=country)) +
      ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      ggplot2::xlab('Day and month') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_colour_manual(values=cbPalette)+
      ggplot2::scale_y_continuous(limits = c(0, max(df$number_of_tweets)), expand = c(0,0)) +
      ggplot2::scale_x_date(date_labels = "%d %b",
                            expand = c(0, 0),
                            breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic(base_family = "Arial") +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 16, face = "bold"),
                     axis.text = ggplot2::element_text(colour = "black", size = 16),
                     axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                         margin = ggplot2::margin(-15, 0, 0, 0)),
                     axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
                     axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
    df <- dplyr::rename(df,"Country" = country)
  } else{ #no countries selected, without aggregation by country #has to be factorized and need to generate all dates
    fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = date, y = number_of_tweets)) +
      ggplot2::geom_line(colour = "#65b32e") +
      ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      ggplot2::xlab('Day and month') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_y_continuous(limits = c(0, max(df$number_of_tweets)), expand = c(0,0)) +
      ggplot2::scale_x_date(date_labels = "%d %b",
                            expand = c(0, 0),
                            breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic(base_family = "Arial") +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 16, face = "bold"),
                     axis.text = ggplot2::element_text(colour = "black", size = 16),
                     axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                         margin = ggplot2::margin(-15, 0, 0, 0)),
                     axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
                     axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
    
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
trend_line <- function(s_topic=c(),s_country=c(),type_date="created_date",geo_country_code="tweet_geo_country_code",date_min="1900-01-01",date_max="2100-01-01"){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  dfs <- get_aggregates()
  colnames(dfs)[colnames(dfs)== geo_country_code] <- "geo_country_code"
  fig <- data_treatment(dfs,s_topic,s_country, type_date, geo_country_code, date_min,date_max)
  plot_trendline(fig,s_country,s_topic)
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
create_map <- function(s_topic=c(),geo_code = "tweet",type_date="days",date_min="1900-01-01",date_max="2100-01-01"){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  type_date <- ifelse(type_date=="days","created_date","created_weeknum")
  longitude <- ifelse(geo_code=="tweet","tweet_longitude","user_longitude")
  latitude <- ifelse(geo_code=="tweet","tweet_latitude","user_latitude")
  dfs <- get_aggregates()
  colnames(dfs)[colnames(dfs)== type_date] <- "date"
  colnames(dfs)[colnames(dfs)== longitude] <- "longitude"
  colnames(dfs)[colnames(dfs)== latitude] <- "latitude"
  fig_map <- (dfs
              # keep records with latitude and longitude
              %>% dplyr::filter(date >= date_min && date <= date_max)
              %>% dplyr::filter(!is.na(longitude))
              %>% dplyr::filter(!is.na(latitude)))
  #only selected topic
  if(length(s_topic>0)){ 
    fig_map <- (fig_map %>% dplyr::filter(topic==s_topic ))}
  mymap <- maps::map("world", fill=TRUE, col="white", bg="lightblue", ylim=c(-60, 90), mar=c(0,0,0,0))
  fig <- points(fig_map$longitude, fig_map$latitude, col = "red", cex = 1)
  list("chart" = fig, "data" = fig_map) 
}
