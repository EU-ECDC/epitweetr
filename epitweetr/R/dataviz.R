
#' Search for all tweets on topics defined on configuration
#' @export
# plotting and pipes - tidyverse!
library(tidyverse) 
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
    
    fig_line <- ggplot(fig, aes(x = created_date, y = t,fill=factor(tweet_geo_country_code),color=tweet_geo_country_code)) +
      geom_line() +
      ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      xlab('Date') +
      ylab('Number of tweets') +
      scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      scale_x_date(date_labels = "%d %b",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      theme_classic() +
      theme(plot.title = element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = element_text(colour = "black", size = 16),
            axis.text.x = element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = margin(-15, 0, 0, 0)),
            axis.title.x = element_text(margin = margin(30, 0, 0, 0), size = 16),
            axis.title.y = element_text(margin = margin(-25, 0, 0, 0), size = 16))
  } else{ #no countries selected, without aggregation by country
    fig <- (dfs
            %>% dplyr::group_by(created_date, topic) 
            %>% dplyr::filter(!is.na(tweet_geo_country_code) &&!is.na(created_date) && created_date >= date_min && created_date <= date_max)
            %>% dplyr::summarise(t = sum(tweets)) 
            %>% dplyr::arrange(desc(t)) 
            #%>% dplyr::top_n(100)
            %>% dplyr::filter(topic==s_topic ))
    fig_line <- ggplot(fig, aes(x = created_date, y = t)) +
      geom_line(colour = "green") +
      ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      xlab('Date') +
      ylab('Number of tweets') +
      scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      scale_x_date(date_labels = "%d %b",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      theme_classic() +
      theme(plot.title = element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = element_text(colour = "black", size = 16),
            axis.text.x = element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = margin(-15, 0, 0, 0)),
            axis.title.x = element_text(margin = margin(30, 0, 0, 0), size = 16),
            axis.title.y = element_text(margin = margin(-25, 0, 0, 0), size = 16))
    
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
    fig_line <- ggplot(fig, aes(x = created_weeknum, y = t,fill=factor(tweet_geo_country_code),color=tweet_geo_country_code)) +
      geom_line() +
      ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      xlab('Date') +
      ylab('Number of tweets') +
      scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      scale_x_date(date_labels = "%V%G",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      theme_classic() +
      theme(plot.title = element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = element_text(colour = "black", size = 16),
            axis.text.x = element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = margin(-15, 0, 0, 0)),
            axis.title.x = element_text(margin = margin(30, 0, 0, 0), size = 16),
            axis.title.y = element_text(margin = margin(-25, 0, 0, 0), size = 16))
  }else{ #no countries selected, without aggregation by country
    fig <- (dfs
            %>% dplyr::group_by(created_weeknum, topic) 
            %>% dplyr::filter(!is.na(tweet_geo_country_code) &&!is.na(created_weeknum) && created_weeknum >= date_min && created_weeknum <= date_max)
            %>% dplyr::summarise(t = sum(tweets)) 
            %>% dplyr::arrange(desc(t)) 
            #%>% dplyr::top_n(100)
            %>% dplyr::filter(topic==s_topic ))
    fig$created_weeknum <-as.Date(as.character(fig$created_weeknum),"%Y%W")
    fig_line <- ggplot(fig, aes(x = created_weeknum, y = t)) +
      geom_line(colour = "green") +
      ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
      xlab('Date') +
      ylab('Number of tweets') +
      scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
      scale_x_date(date_labels = "%V%G",
                   expand = c(0, 0),
                   breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
      theme_classic() +
      theme(plot.title = element_text(hjust = 0.5, size = 18, face = "bold"),
            axis.text = element_text(colour = "black", size = 16),
            axis.text.x = element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                       margin = margin(-15, 0, 0, 0)),
            axis.title.x = element_text(margin = margin(30, 0, 0, 0), size = 16),
            axis.title.y = element_text(margin = margin(-25, 0, 0, 0), size = 16))
    
  }
  # Show figure in 'plots' pannel
  fig_line
}
#trend_line('Ebola',c("BE","MX","US"), '2020-03-10','2020-03-26')
#trend_line('Ebola',c(), '2020-03-10','2020-03-26')
#To do : allow both types of data (weekly or daily)
