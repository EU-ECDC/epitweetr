
#' Search for all tweets on topics defined on configuration
#' @export
#colors to improve readability for those who are colour-blind
cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

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
trend_line <- function(s_topic=c(),s_country=c(),type_date="days",geo_country_code="tweet_geo_country_code",date_min="1900-01-01",date_max="2100-01-01"){
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  dfs <- get_aggregates()
  colnames(dfs)[colnames(dfs)== geo_country_code] <- "geo_country_code"
  
  if(length(s_topic)==0){
    if(type_date =="days"){
      if(length(s_country)>0){ #if several countries selected, aggregation by country
        fig <- (dfs
                %>% dplyr::group_by(created_date,geo_country_code) 
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_date)))
        if(!is.na(date_min)){fig <- dplyr::filter(fig,created_date >= date_min)}
        if(!is.na(date_max)){fig <- dplyr::filter(fig,created_date <= date_max)}
        fig <- (fig
                %>% dplyr::summarise(t = sum(tweets)) 
                %>% dplyr::arrange(desc(t)) 
                #%>% dplyr::top_n(100)
                %>% dplyr::filter(geo_country_code %in% s_country ))
        
        fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_date, y = t)) +
          ggplot2::geom_line(ggplot2::aes(colour=geo_country_code, group=geo_country_code)) +
          ggplot2::ggtitle("Number of tweets ") +
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
                %>% dplyr::group_by(created_date) 
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_date)))
        if(!is.na(date_min)){fig <- dplyr::filter(fig,created_date >= date_min)}
        if(!is.na(date_max)){fig <- dplyr::filter(fig,created_date <= date_max)}
        fig <- (fig
                %>% dplyr::summarise(t = sum(tweets)) 
                %>% dplyr::arrange(desc(t))) 
        #%>% dplyr::top_n(100)
        fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_date, y = t)) +
          ggplot2::geom_line(colour = "#65b32e") +
          ggplot2::ggtitle("Number of tweets") +
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
      ##Case B
      #Date in format weeknum
    } else { 
      if(length(s_country)>0){ #if several countries selected, aggregation by country
        fig <- (dfs
                %>% dplyr::group_by(created_weeknum,geo_country_code) 
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_weeknum) && created_weeknum >= date_min && created_weeknum <= date_max )
                %>% dplyr::summarise(t = sum(tweets)) 
                %>% dplyr::arrange(desc(t)) 
                #%>% dplyr::top_n(100)
                %>% dplyr::filter(geo_country_code %in% s_country ))
        fig$created_weeknum <-as.Date(paste(fig$created_weeknum,"1"),"%Y%U %u")
        fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_weeknum, y = t)) +
          ggplot2::geom_line(ggplot2::aes(colour=geo_country_code, group=geo_country_code)) +
          ggplot2::ggtitle("Number of tweets") +
          ggplot2::xlab('Date') +
          ggplot2::ylab('Number of tweets') +
          ggplot2::scale_colour_manual(values=cbPalette)+
          ggplot2:: scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
          ggplot2::scale_x_date(date_labels = "%d %b",
                                expand = c(0, 0),
                                breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
          ggplot2:: theme_classic() +
          ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
                         axis.text = ggplot2::element_text(colour = "black", size = 16),
                         axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                             margin = ggplot2::margin(-15, 0, 0, 0)),
                         axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
                         axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
      }else{ #no countries selected, without aggregation by country
        fig <- (dfs
                %>% dplyr::group_by(created_weeknum) 
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_weeknum) && created_weeknum >= date_min && created_weeknum <= date_max)
                %>% dplyr::summarise(t = sum(tweets)) 
                %>% dplyr::arrange(desc(t))) 
        #%>% dplyr::top_n(100)
        fig$created_weeknum <-as.Date(paste(fig$created_weeknum,"1"),"%Y%U %u")
        fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_weeknum, y = t)) +
          ggplot2::geom_line(colour = "#65b32e") +
          ggplot2::ggtitle("Number of tweets") +
          ggplot2::xlab('Date') +
          ggplot2::ylab('Number of tweets') +
          ggplot2::scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
          ggplot2::scale_x_date(date_labels = "%d %b",
                                expand = c(0, 0),
                                breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
          ggplot2:: theme_classic() +
          ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
                         axis.text = ggplot2::element_text(colour = "black", size = 16),
                         axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                             margin = ggplot2::margin(-15, 0, 0, 0)),
                         axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
                         axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
        
        
      }
    }
  } else {
    
    
    
    ##Case A
    #Date in format "yyyy-mm-dd"
    if(type_date =="days"){
      if(length(s_country)>0){ #if several countries selected, aggregation by country
        fig <- (dfs
                %>% dplyr::group_by(created_date,geo_country_code, topic) 
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_date)))
        if(!is.na(date_min)){fig <- dplyr::filter(fig,created_date >= date_min)}
        if(!is.na(date_max)){fig <- dplyr::filter(fig,created_date <= date_max)}
        fig <- (fig
                %>% dplyr::summarise(t = sum(tweets)) 
                %>% dplyr::arrange(desc(t)) 
                #%>% dplyr::top_n(100)
                %>% dplyr::filter(topic==s_topic )
                %>% dplyr::filter(geo_country_code %in% s_country ))
        
        fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_date, y = t)) +
          ggplot2::geom_line(ggplot2::aes(colour=geo_country_code, group=geo_country_code)) +
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
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_date)))
        if(!is.na(date_min)){fig <- dplyr::filter(fig,created_date >= date_min)}
        if(!is.na(date_max)){fig <- dplyr::filter(fig,created_date <= date_max)}
        fig <- (fig
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
      ##Case B
      #Date in format weeknum
    } else { 
      if(length(s_country)>0){ #if several countries selected, aggregation by country
        fig <- (dfs
                %>% dplyr::group_by(created_weeknum,geo_country_code, topic) 
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_weeknum) && created_weeknum >= date_min && created_weeknum <= date_max )
                %>% dplyr::summarise(t = sum(tweets)) 
                %>% dplyr::arrange(desc(t)) 
                #%>% dplyr::top_n(100)
                %>% dplyr::filter(topic==s_topic )
                %>% dplyr::filter(geo_country_code %in% s_country ))
        fig$created_weeknum <-as.Date(paste(fig$created_weeknum,"1"),"%Y%U %u")
        fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_weeknum, y = t)) +
          ggplot2::geom_line(ggplot2::aes(colour=geo_country_code, group=geo_country_code)) +
          ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
          ggplot2::xlab('Date') +
          ggplot2::ylab('Number of tweets') +
          ggplot2::scale_colour_manual(values=cbPalette)+
          ggplot2:: scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
          ggplot2::scale_x_date(date_labels = "%d %b",
                                expand = c(0, 0),
                                breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
          ggplot2:: theme_classic() +
          ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
                         axis.text = ggplot2::element_text(colour = "black", size = 16),
                         axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                             margin = ggplot2::margin(-15, 0, 0, 0)),
                         axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
                         axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
      }else{ #no countries selected, without aggregation by country
        fig <- (dfs
                %>% dplyr::group_by(created_weeknum, topic) 
                %>% dplyr::filter(!is.na(geo_country_code) &&!is.na(created_weeknum) && created_weeknum >= date_min && created_weeknum <= date_max)
                %>% dplyr::summarise(t = sum(tweets)) 
                %>% dplyr::arrange(desc(t)) 
                #%>% dplyr::top_n(100)
                %>% dplyr::filter(topic==s_topic ))
        fig$created_weeknum <-as.Date(paste(fig$created_weeknum,"1"),"%Y%U %u")
        fig_line <- ggplot2::ggplot(fig, ggplot2::aes(x = created_weeknum, y = t)) +
          ggplot2::geom_line(colour = "#65b32e") +
          ggplot2::ggtitle(paste0("Number of tweets mentioning ",s_topic)) +
          ggplot2::xlab('Date') +
          ggplot2::ylab('Number of tweets') +
          ggplot2::scale_y_continuous(limits = c(0, max(fig$t)), expand = c(0,0)) +
          ggplot2::scale_x_date(date_labels = "%d %b",
                                expand = c(0, 0),
                                breaks = function(x) seq.Date(from = min(x), to = max(x), by = "7 days")) +
          ggplot2:: theme_classic() +
          ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 18, face = "bold"),
                         axis.text = ggplot2::element_text(colour = "black", size = 16),
                         axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                                             margin = ggplot2::margin(-15, 0, 0, 0)),
                         axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 16),
                         axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 16))
        
        
      }
    }
  }
  list("chart" = fig_line, "data" = fig) 
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

