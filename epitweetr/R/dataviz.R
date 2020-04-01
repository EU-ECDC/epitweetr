
#' Search for all tweets on topics defined on configuration
#' @export
#colors o improve readability for those who are colour-blind
cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
#trendline on dates, countries and topic selected
trend_line <- function(s_topic,s_country,date_min,date_max) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  
  dfs <- get_aggregates()
  if(length(s_country)>0){ #if several countries selected, aggregation by country
    fig <- (dfs
          %>% dplyr::group_by(created_date,tweet_geo_country_code, topic) 
          %>% dplyr::filter(!is.na(tweet_geo_country_code) &&!is.na(created_date) && created_date >= date_min && created_date <= date_max)
          %>% dplyr::summarise(t = sum(tweets)) 
          %>% dplyr::arrange(desc(t)) 
          #%>% dplyr::top_n(100)
          %>% dplyr::filter(topic==s_topic )
          %>% dplyr::filter(tweet_geo_country_code %in% s_country ))
    
    fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_date, y = t)) +
      ggplot2::geom_line(ggplot2::aes(colour=tweet_geo_country_code, group=tweet_geo_country_code)) +
      ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      ggplot2::xlab('Date') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_colour_manual(values=cbPalette)+
      ggplot2::scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      ggplot2::scale_x_date(date_labels = "%d %b",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic() +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = ggplot2::element_text(colour = "black", size = 16),
            axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = ggplot2::margin(-15, 0, 0, 0)),
            axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
            axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
  } else{ #no countries selected, without aggregation by country
    fig <- (dfs
            %>% dplyr::group_by(created_date, topic) 
            %>% dplyr::filter(!is.na(tweet_geo_country_code) &&!is.na(created_date) && created_date >= date_min && created_date <= date_max)
            %>% dplyr::summarise(t = sum(tweets)) 
            %>% dplyr::arrange(desc(t)) 
            #%>% dplyr::top_n(100)
            %>% dplyr::filter(topic==s_topic ))
    fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_date, y = t)) +
      ggplot2::geom_line(colour = "#65b32e") +
      ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      ggplot2::xlab('Date') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      ggplot2::scale_x_date(date_labels = "%d %b",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic() +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = ggplot2::element_text(colour = "black", size = 16),
            axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = ggplot2::margin(-15, 0, 0, 0)),
            axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
            axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
    
  }
  # Show figure in 'plots' pannel
  fig_line
}

#trendline by weeknum on dates, countries and topic selected
trend_line_weeknum <- function(s_topic,s_country,date_min,date_max) {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  
  dfs <- get_aggregates()
  if(length(s_country)>0){ #if several countries selected, aggregation by country
    fig <- (dfs
            %>% dplyr::group_by(created_weeknum,tweet_geo_country_code, topic) 
            %>% dplyr::filter(!is.na(tweet_geo_country_code) &&!is.na(created_weeknum) && created_weeknum >= date_min && created_weeknum <= date_max )
            %>% dplyr::summarise(t = sum(tweets)) 
            %>% dplyr::arrange(desc(t)) 
            #%>% dplyr::top_n(100)
            %>% dplyr::filter(topic==s_topic )
            %>% dplyr::filter(tweet_geo_country_code %in% s_country ))
    fig$created_weeknum <-as.Date(as.character(fig$created_weeknum),"%Y%W")
    fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_weeknum, y = t)) +
      ggplot2::geom_line(ggplot2::aes(colour=tweet_geo_country_code, group=tweet_geo_country_code)) +
      ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      ggplot2::xlab('Date') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_colour_manual(values=cbPalette)+
      ggplot2::scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      ggplot2::scale_x_date(date_labels = "%V%G",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic() +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = ggplot2::element_text(colour = "black", size = 16),
            axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = ggplot2::margin(-15, 0, 0, 0)),
            axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
            axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
  }else{ #no countries selected, without aggregation by country
    fig <- (dfs
            %>% dplyr::group_by(created_weeknum, topic) 
            %>% dplyr::filter(!is.na(tweet_geo_country_code) &&!is.na(created_weeknum) && created_weeknum >= date_min && created_weeknum <= date_max)
            %>% dplyr::summarise(t = sum(tweets)) 
            %>% dplyr::arrange(desc(t)) 
            #%>% dplyr::top_n(100)
            %>% dplyr::filter(topic==s_topic ))
    fig$created_weeknum <-as.Date(as.character(fig$created_weeknum),"%Y%W")
    fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_weeknum, y = t)) +
      ggplot2::geom_line(colour = "#65b32e") +
      ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      ggplot2::xlab('Date') +
      ggplot2::ylab('Number of tweets') +
      ggplot2::scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      ggplot2::scale_x_date(date_labels = "%V%G",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      ggplot2::theme_classic() +
      ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = ggplot2::element_text(colour = "black", size = 16),
            axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = ggplot2::margin(-15, 0, 0, 0)),
            axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
            axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
    
  }
  # Show figure in 'plots' pannel
  fig_line
}

#######################################MAP#####################################
create_map <- function(topic, date_min,date_max){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`

  dfs <- get_aggregates()
  fig_map <- (dfs
            # keep records with latitude and longitude
          %>% dplyr::filter(!is.na(tweet_latitude) && !is.na(tweet_longitude))
          #only selected topic
          %>% dplyr::filter(topic==s_topic ))
          #if(!is.na(date_min)){fig_map <- dplyr::filter(fig_map,created_date >= date_min )}
          #if(!is.na(date_max)){fig_map <- dplyr::filter(fig_map,created_date <= date_max )}
  mymap <- maps::map("world", fill=TRUE, col="white", bg="lightblue", ylim=c(-60, 90), mar=c(0,0,0,0))
  points(fig_map$tweet_longitude, fig_map$tweet_latitude, col = "red", cex = 1)
  }

