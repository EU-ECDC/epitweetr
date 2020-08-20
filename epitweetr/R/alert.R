#' @title Execute the alert task  
#' @description Evaluate alerts for last collected day for all topics and regions and send email alerts to subscribers
#' @param tasks current tasks for reporting purposes, Default: get_tasks()
#' @return The list of tasks updated with produced messages
#' @details This function calculates for last aggregated day and then send emails to subscriptors
#'
#' The alert calculation is based on the country_counts time series which stores alerts by country, hour and topics.
#'
#' For each country and region the process starts by aggregating the last N days. A day a block of contiguous 24 hours ending before the hour of the collected last tweet. 
#' N is defined by the alert base line parameter of the shiny application.
#' 
#' An alert will be produced when the number of tweets observates is under the threshold calculated by the modified version of the EARS algorithm (for more details see the package vignette) 
#' The behaviour of the alert detection algoritm is mofied by the alert confidence level,  previous alert downgrade and weekly or daily baseline parameters 
#' as defined on the shiny application and the topics file 
#'
#' A prerequisite to this funtion is that the search_loop must have already collected tweets on the search folder and that geotag and aggregate tasks have already run.
#' Normally this function is not called directly by the user but from the \code{\link{detect_loop}} function.
#' @examples 
#' \dontrun{
#' if(interactive()){
#'    library(epitweetr)
#'    # setting up the data folder
#'    setup_config("/home/epitweetr/data")
#'
#'    # calculating alerts for last day tweets and sending emails to subscriptors
#'    generate_alerts()
#'  }
#' }
#' @seealso 
#'  \code{\link{detect_loop}}
#'  \code{\link{geotag_tweets}}
#'  \code{\link{aggregate_tweets}}
#' @rdname generate_alerts
#' @export
generate_alerts <- function(tasks = get_tasks()) {
  tasks <- tryCatch({
    tasks <- update_alerts_task(tasks, "running", "processing", start = TRUE)
    tasks <- do_next_alerts(tasks)

    tasks <- update_alerts_task(tasks, "running", "sending emails")
    tasks <- send_alert_emails(tasks)
    # Setting status to succes
    tasks <- update_alerts_task(tasks, "success", "", end = TRUE)
    tasks
  }, error = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$alerts$message," :", error_condition))
    tasks <- update_alerts_task(tasks, "failed", paste("failed while", tasks$alerts$message," alerts ", error_condition))
    tasks
  }, warning = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$alerts$message," ", error_condition))
    tasks <- update_alerts_task(tasks, "failed", paste("failed while", tasks$alerts$message," alerts ", error_condition))
    tasks
  })
  return(tasks)
}


#' @title algorithm for outbreak detection, extends the ears algorithm
#' @author Michael Höhle <https://www.math.su.se/~hoehle>
#' @description The simple 7 day running mean version of the EARS 
#' algorithm is extended as follows:
#' \itemize{
#'   \item{proper computation of the prediction interval}
#'   \item{re-weighted version inspired by Farrington (1996)}
#' }
#' @param ts A numeric vector containing the counts of the univariate
#' time series to monitor. The last time point in ts is
#' investigated
#' @param alpha The upper limit is computed as the limit of a one-sided
#' (1-alpha) times 100prc prediction interval, Default: 0.025
#' @param alpha_outlier Residuals beyond 1-alpha_outlier quantile of the 
#'              the t(n-k-1) distribution are downweighted, Default: 0.05
#' @param k_decay Power k in the expression (r_star/r_threshold)^k determining the weight, Default: 4
#' @param no_historic no_historic Number of previous values i.e -1, -2, ..., no_historic
#' to include when computing baseline parameters, Default: 7
#' @param same_weekday_baseline whether to calculate baseline using same weekdays or any day, Default: FALSE
#' @return A dataframe containing the monitored time point,
#'         the upper limit and whether an alarm is sounded or not.
#' @details for algorithm details see package vignette.
#' @examples
#' \dontrun{
#' if(interactive()){
#'    library(epitweetr)
#'    #Running the modifies version of the ears algorithm for a particular data series
#'     ts <- c(150, 130, 122, 160, 155, 128, 144, 125, 300, 319, 289, 277, 500)
#'     show(ears_t_reweighted(ts))
#'  }
#' }
#' @rdname ears_t_reweighted
#' @export
#' @importFrom magrittr `%>%`
ears_t_reweighted <- function(ts, alpha = 0.025, alpha_outlier=0.05, k_decay = 4, no_historic = 7L, same_weekday_baseline = FALSE) {
  `%>%` <- magrittr::`%>%`
  # Times 7 on no_historic if baseline considers only same day of week
  step <- (
    if(same_weekday_baseline) 
      7
    else 
      1
  )
  
  t(sapply(1:length(ts), function(t) {
    if(t <  no_historic * step + 1)
      c(time_monitored=t, y0=ts[[t]], U0=NA, alarm0=NA, y0bar=NA, U0_dw=NA, alarm0_dw=NA)
    else {
      ## Extract current value and historic values 
      ## Taking in consideration the step size which will be seven for same day of week comparison
      y_historic <- sapply(no_historic:1, function(i) ts[[t - i*step]])
      ## Calculate empirical mean and standard deviation
      y0 <- ts[[t]]
      
      # Number of observations
      n <- no_historic
      ## Rank of design matrix - it's 1 for the intercept-only-model
      k <- 1   # qr(matrix(rep(1, no_historic)))$rank
      
      #Helper function to compute threshold based on weights 
      compute_threshold <- function(w) {
        # Ensure weights sum to n
        w <- w/sum(w) * n
        # Compute reweighted estimates
        y0bar <- mean( w * y_historic)
        resid <- (y_historic - y0bar)
        sd0 <- sqrt(sum( w * resid^2)/(n-k))
        
        ## Upper threshold
        U0 <- y0bar + qt(1-alpha,  df=no_historic - 1) * sd0 * sqrt(1 + 1/no_historic)
        ## Enough evidence to sound an alarm?
        alarm0 <- y0 > U0
        #Return results
        return(list(y0bar=y0bar, sd0=sd0, U0=U0, alarm0=alarm0))
      }      
      
      #Results without reweighting
      res_noreweight <- compute_threshold( w=rep(1,n))
    
      #Extract fit from inversion  initial w=1 version
      y0bar <- res_noreweight$y0bar
      
      ## Calculate the raw residuals
      e <- (y_historic - y0bar)
      #Diagonal of hat matrix - analytic
      pii <- rep(1/no_historic, no_historic)
      # Hat matrix from regression with i'th observation removed is always they same for intercept only model
      Xi <- matrix(rep(1, n-1))
      #P_noi <- Xi %*% solve( t(Xi) %*% Xi) %*% t(Xi)
      #same for the intercept only model, but probably faster: 
      # P_noi <- matrix(1/(n-1), ncol=n-1, nrow=n-1)
      # # Compute \hat{sigma}_{(i)} as in Chatterjee and Hadi (1988), eqn 4.7 on p. 74
      # sigmai <- sqrt(sapply(1:n, function(i) {
      #   Y_noi <- matrix(y_historic[-i]) 
      #   t(Y_noi) %*% (diag(n-1) - P_noi) %*% Y_noi / (n-k-1)
      # }))

      # Fast version for the intercept-only model
      sigmai <- sapply(1:n, function(i) { sd(y_historic[-i])})
      
      # Externally Studentized residuals, see Chaterjee and Hadi (1988), p. 74
      rstar <- e / (sigmai * sqrt(1 - pii))
      
      # Define treshold for outliers
      reweight_threshold <- qt(1-alpha_outlier, df=n - k - 1)
      
      # Weights 1: Drop outliers
      #w_drop <- ifelse( rstar > reweight_threshold, 0, 1)
      #res_drop <- compute_threshold(w_drop)
      
      # Weights 2: Farrington procedure, downweight
      w_dw <- ifelse( rstar > reweight_threshold, (reweight_threshold/rstar)^k_decay, 1)
      res_dw <- compute_threshold(w_dw)
      
      ## Return a vector with the components of the computation
      c(time_monitored=t, y0=y0, U0=res_noreweight$U0, alarm0=res_noreweight$alarm0, y0bar = res_noreweight$y0bar, U0_dw=res_dw$U0, alarm0_dw=res_dw$alarm0)
    }})) %>% as.data.frame()
}


# Getting alert daily counts taking in consideration a 24 hour sliding window since last full hour
get_reporting_date_counts <- function(
    df = get_aggregates("country_counts")
    , topic
    , start = NA
    , end = NA
  ) {
  `%>%` <- magrittr::`%>%`
  if(nrow(df)>0) {
    last_day <- max(as.Date(df$created_date))
    # Calculating end day hour which is going to be the last fully collected hour when 
    # requesting the last colleted dates or 23h if it is a fully collected day 
    last_full_hour <- (
      if(!is.na(end) && end < last_day) 
        23
      else
        as.integer(strftime(max(as.POSIXlt(df$created_date) + (as.integer(df$created_hour) - 1) * 3600), "%H"))
    )
    #Handling going to previous day if lastèfull hour is negative
    if(last_full_hour < 0) {
      last_full_hour = 23
      last_day <- last_day - 1  
    }
    # Setting the reporting date based on the cutoff hour
    df$reporting_date <- ifelse(df$created_hour <= last_full_hour, df$created_date, df$created_date + 1)

    # filtering by start date
    df <- (
      if(is.na(start)) df 
      else dplyr::filter(df, reporting_date >= start)
    )
    # filtering by end date
    df <- (
      if(is.na(end)) df 
      else dplyr::filter(df, reporting_date <= end)
    )
    
    # group by reporting date
    df %>%
      dplyr::group_by(reporting_date) %>% 
      dplyr::summarise(count = sum(tweets), known_users = sum(known_users)) %>% 
      dplyr::ungroup()
  } else {
    data.frame(reporting_date=as.Date(character()),count=numeric(), known_users=numeric(), stringsAsFactors=FALSE)   
  }
}

# Calculating alerts for a particular period and a set of period 
calculate_region_alerts <- function(
    topic
    , country_codes = list()
    , country_code_cols = "tweet_geo_country_code"
    , start = NA
    , end = NA
    , with_retweets = FALSE
    , alpha = 0.025
    , alpha_outlier = 0.05
    , k_decay = 4
    , no_historic = 7
    , bonferroni_m = 1
    , same_weekday_baseline = FALSE
    , logenv = NULL
  ) {
  `%>%` <- magrittr::`%>%`
  read_from_date <- get_alert_count_from(date = start, baseline_size = no_historic, same_weekday_baseline = same_weekday_baseline) 
  f_topic <- topic
  # Getting data from country counts to perform alert calculation 
  df <- get_aggregates(dataset = "country_counts", filter = list(
    topic = topic, 
    period = list(read_from_date, end)
  )) %>% dplyr::filter(topic == f_topic)

  # Adding retwets on count if requested
  df <- if(with_retweets){
    df %>% dplyr::mutate(
      tweets = ifelse(is.na(retweets), 0, retweets) + ifelse(is.na(tweets), 0, tweets),
      known_users = known_retweets + known_original
    )
  } else {
    df %>% dplyr::rename(
      known_users = known_original
    ) 
  }
  if(!is.null(logenv)) {
    # Setting global variable for storing total number of tweets concerned including all country_cols
    total_df <- df %>% dplyr::filter(
      (
       length(country_codes) == 0 
       |  (tweet_geo_country_code %in% country_codes) 
       |  (user_geo_country_code %in% country_codes) 
      )
    )
    total_count <- sum((get_reporting_date_counts(total_df, topic, read_from_date, end) %>% dplyr::filter(reporting_date >= start))$count)
    logenv$total_count <- if(exists("total_count", logenv)) logenv$total_count + total_count else total_count
  }
  # filtering by country codes
  df <- (
    if(length(country_codes) == 0) df 
    else if(length(country_code_cols) == 1) dplyr::filter(df, topic == f_topic & (!!as.symbol(country_code_cols[[1]])) %in% country_codes) 
    else if(length(country_code_cols) == 2) dplyr::filter(df, topic == f_topic & (!!as.symbol(country_code_cols[[1]])) %in% country_codes | (!!as.symbol(country_code_cols[[2]])) %in% country_codes)
    else stop("get geo count does not support more than two country code columns") 
  )
  # Getting univariate time series aggregatin by day
  counts <- get_reporting_date_counts(df, topic, read_from_date, end)
  if(nrow(counts)>0) {
    # filling missing values with zeros if any
    date_range <- as.numeric(read_from_date):as.numeric(end)
    missing_dates <- date_range[sapply(date_range, function(d) !(d %in% counts$reporting_date))]
    if(length(missing_dates > 0)) {
      missing_dates <- data.frame(reporting_date = missing_dates, count = 0)
      counts <- dplyr::bind_rows(counts, missing_dates) %>% dplyr::arrange(reporting_date) 
    }
    #Calculating alerts
    alerts <- ears_t_reweighted(counts$count, alpha=alpha/bonferroni_m, alpha_outlier = alpha_outlier, k_decay = k_decay, no_historic = no_historic, same_weekday_baseline)
    counts$alert <- alerts$alarm0_dw
    counts$limit <- alerts$U0_dw
    counts$baseline <- alerts$y0bar
    counts <- if(is.na(start))
      counts
    else
      counts %>% dplyr::filter(reporting_date >= start)
    counts
  } else {
    counts$alert <- logical()
    counts$limit <- numeric()
    counts$baseline <- numeric()
    counts
  }
}

# Calculating alerts for a set of regions and an specific period
calculate_regions_alerts <- function(
    topic
    , regions = c(1)
    , date_type = c("created_date")
    , date_min = as.Date("1900-01-01")
    , date_max = as.Date("2100-01-01")
    , with_retweets = FALSE
    , location_type = "tweet" 
    , alpha = 0.025
    , alpha_outlier = 0.05
    , k_decay = 4
    , no_historic = 7 
    , bonferroni_correction = FALSE
    , same_weekday_baseline = FALSE
    , logenv = NULL
    )
{
  #Importing pipe operator
  `%>%` <- magrittr::`%>%` 

  # Getting regions details
  all_regions <- get_country_items()
  # Setting world as region if no region is selected
  if(length(regions)==0) regions = c(1)

  # Calculating alerts for each provided region
  series <- lapply(1:length(regions), function(i) {
    #message(paste("calculating for ", topic , all_regions[[regions[[i]]]]$name, i))
    bonferroni_m <- 
      if(bonferroni_correction) {
        length(all_regions[unlist(lapply(all_regions, function(r) r$level == all_regions[[ regions[[i]] ]]$level))])
      } else 1
    alerts <- 
      calculate_region_alerts(
        topic = topic, 
        country_codes = all_regions[[regions[[i]]]]$codes,
	      country_code_cols = if(location_type == "tweet") "tweet_geo_country_code" else if(location_type == "user") "user_geo_country_code" else c("tweet_geo_country_code", "user_geo_country_code"),
        start = as.Date(date_min), 
        end = as.Date(date_max), 
        with_retweets = with_retweets, 
        no_historic=no_historic, 
        alpha=alpha,
        alpha_outlier = alpha_outlier,
        k_decay = k_decay,
        bonferroni_m = bonferroni_m,
        same_weekday_baseline = same_weekday_baseline,
        logenv = logenv
      )
    alerts <- dplyr::rename(alerts, date = reporting_date) 
    alerts <- dplyr::rename(alerts, number_of_tweets = count) 
    alerts$date <- as.Date(alerts$date, origin = '1970-01-01')
    if(nrow(alerts)>0) alerts$country <- all_regions[[regions[[i]]]]$name
    alerts$known_ratio = alerts$known_users / alerts$number_of_tweets
    alerts
  })

  # Joinig alerts generated for each region
  df <- Reduce(x = series, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
  if(date_type =="created_weeknum"){
    df <- df %>% 
      dplyr::group_by(week = strftime(date, "%G%V"), topic, country) %>% 
      dplyr::summarise(c = sum(number_of_tweets), a = max(alert), d = min(date), ku = sum(known_users), kr = sum(known_users)/sum(number_of_tweets), l = 0, b = 0) %>% 
      dplyr::ungroup() %>% 
      dplyr::select(d , c, ku, a, l, topic, country, kr) %>% 
      dplyr::rename(date = d, number_of_tweets = c, alert = a, known_users = ku, known_ratio = kr, limit = l, baseline = b)
  }
  return(df)
}


# get alert count start depending on the baseline type: any day ort same weekday
get_alert_count_from <- function(date, baseline_size, same_weekday_baseline) {
  if(!same_weekday_baseline)
    date - (baseline_size + 2)
  else
    date - (7 * baseline_size + 2)
}

# getting paramerters for current alert generation
# This will be based on last successfully aggregated date and it will only be generated if once each scheduling span
do_next_alerts <- function(tasks = get_tasks()) {
  `%>%` <- magrittr::`%>%`
  # Getting period for last alerts
  last_agg <- get_aggregated_period("country_counts")
  alert_to <- last_agg$last
  alert_to_hour <- last_agg$last_hour

  # Determining whether we should produce alerts for current hour (if aggregation has produced new records since last alert generation
  do_alerts <- FALSE
  alert_file <-get_alert_file(alert_to)
  if(!file.exists(alert_file)) { 
    do_alerts <- TRUE
  } else {
    f <- file(alert_file, "rb")
    existing_alerts <- jsonlite::stream_in(f)
    close(f)
    if(nrow(existing_alerts)==0 || nrow(existing_alerts %>% dplyr::filter(date == alert_to & hour == alert_to_hour)) == 0)
      do_alerts <- TRUE
  }
  if(do_alerts) {
    # Getting region details
    regions <- get_country_items()
    # Caching counts for all topics for the selected period the period to consider on cache will depend on  alert_same_weekday_baseline
    cc <- get_aggregates(
      dataset = "country_counts", 
      filter = list(
        period = list(
          get_alert_count_from(date = alert_to, baseline_size = conf$alert_history, same_weekday_baseline = conf$alert_same_weekday_baseline), 
          alert_to
        )
      )
    )
    # reassigning NULL to cc for allow garbage collecting
    cc <- NULL
    #TODO: Using parallel package to calculate alerts on all available cores
    #  cl <- parallel::makePSOCKcluster(as.numeric(conf$spark_cores), outfile="")
    #  conf <- conf
    topics <- unique(lapply(conf$topics, function(t) t$topic))
    #  parallel::clusterExport(cl, list("conf", "topics", "regions", "alert_to", "calculate_regions_alerts", "get_country_items", "get_country_items", "calculate_alerts"), envir=environment())
    #  alerts <- parallel::parLapply(cl, topics, function(topic) {
    
    # calculating aletrts per topic
    alerts <- lapply(topics, function(topic) {
      m <- paste("Getting alerts for",topic, alert_to) 
      message(m)  
      tasks <- update_alerts_task(tasks, "running", m , start = TRUE)
      calculate_regions_alerts(
        topic = topic,
        regions = 1:length(regions), 
        date_type = "created_date", 
        date_min = alert_to, 
        date_max = alert_to, 
        with_retweets = conf$alert_with_retweets, 
        location_type = "tweet" , 
        alpha = as.numeric(get_topics_alphas()[[topic]]), 
        alpha_outlier = as.numeric(get_topics_alpha_outliers()[[topic]]), 
        k_decay = as.numeric(get_topics_k_decays()[[topic]]), 
        no_historic = as.numeric(conf$alert_history), 
        bonferroni_correction = conf$alert_with_bonferroni_correction,
        same_weekday_baseline =  conf$alert_same_weekday_baseline
      ) %>%
      dplyr::mutate(topic = topic) %>% 
      dplyr::filter(!is.na(alert) & alert == 1)
    })
    #  parallel::stopCluster(cl)
    
    # Joining all day alerts on a single dataframe
    alerts <- Reduce(x = alerts, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
    
    if(nrow(alerts)>0) {
      # Adding top words
      m <- paste("Adding topwords") 
      message(m)  
      tasks <- update_alerts_task(tasks, "running", m , start = TRUE)
      codeMap <- get_country_codes_by_name()
      ts <- unique(alerts$topic)
      topwords <- get_aggregates("topwords", filter = list(topic = ts , period = c(alert_to, alert_to)))
      topwords$frequency <- topwords$original
      topwords <- mapply(
        function(t, d, r) {
          codes <- codeMap[[r]]
          if(is.null(codes)) {
            "" 
          } else {
            tws <- (topwords %>%
              dplyr::filter(
                topic == t
                & created_date == d
                & (if(length(codes)==0) TRUE else tweet_geo_country_code %in% codes )
              ) %>% 
            dplyr::filter(!is.na(frequency)) %>% 
            dplyr::group_by(tokens) %>%
            dplyr::summarize(frequency = sum(frequency)) %>%
            dplyr::ungroup() %>%
            dplyr::arrange(-frequency) %>%
            head(10) %>%
            dplyr::mutate(tokens = reorder(tokens, frequency)))
            if(nrow(tws) == 0) 
              ""
            else 
              paste(paste(tws$tokens, " (", tws$frequency, ")", collapse = ", ", sep = ""))
          }
        }, 
        alerts$topic, 
        alerts$date,
        alerts$country
      )
      alerts$topwords <- topwords 
      # Adding used parameters
      alerts <- alerts %>% 
        dplyr::mutate(
        hour = alert_to_hour, 
        location_type = "tweet", 
        with_retweets = conf$alert_with_retweets, 
        alpha = as.numeric(get_topics_alphas()[[topic]]), 
        alpha_outlier = as.numeric(get_topics_alpha_outliers()[[topic]]), 
        k_decay = as.numeric(get_topics_k_decays()[[topic]]), 
        no_historic = as.numeric(conf$alert_history), 
        no_historic = as.numeric(conf$alert_history),
        bonferroni_correction = conf$alert_with_bonferroni_correction,
        same_weekday_baseline = conf$same_weekday_baseline
      )
      f <- file(alert_file, open = "ab")
      jsonlite::stream_out(alerts, f, auto_unbox = TRUE)
      close(f)
    }
  } else {
    message("no alerts to recalculate")
  }
  tasks
}

 
#' @title Getting alerts produced by the \code{\link{detect_loop}}
#' @description Returns a dataframe of alerts produced by the detect loop, which are stored on the alert folder.
#' @param topic Character vector. When it is not emptu it will limit the returned alerts to the provided topics , Default: character()
#' @param countries Character vector containing the names of countries or regions or a numeric vector containing the indexes of countries 
#' to filter the alerts to return. The indexes can be obtained by the calling \code{\link{get_country_items}}, Default: numeric()
#' @param from date defining the beggining of the period of alerts to return, Default: '1900-01-01'
#' @param until date defining the end of the period of alerts to return, Default: '2100-01-01'
#' @return 
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'    library(epitweetr)
#'    # setting up the data folder
#'    setup_config("/home/epitweetr/data")
#'
#'    #Getting alert produced for last 30 days for a particular country
#'    get_alerts(countries = c("Chile", "Australia", "France"), from = as.Date(Sys.time())-30, until = as.Date(Sys.time()))
#'  
#'  }
#' }
#' @seealso
#' \code{\link{get_country_items}}
#' @rdname get_alerts
#' @export
#' @importFrom magrittr `%>%`
#' @importFrom jsonlite stream_in
#' @importFrom dplyr filter arrange group_by mutate ungroup bind_rows
get_alerts <- function(topic=character(), countries=numeric(), from="1900-01-01", until="2100-01-01") {
  `%>%` <- magrittr::`%>%`
  # preparing filers dealing with possible names collitions with dataframe
  regions <- get_country_items()
  t <- topic
  c <- ( 
    if(class(countries) %in% c("numeric", "integer"))
      lapply(countries, function(i) regions[[i]]$name)
    else 
      countries
  )
 
  alert_files <- list.files(file.path(conf$data_dir, "alerts"), recursive=TRUE, full.names =TRUE)  
  file_dates <- lapply(alert_files, function(f) {list(path = f, date = as.Date(substr(tail(strsplit(f, "/")[[1]], n = 1), 1, 10), format="%Y.%m.%d"))})
  files_in_period <- lapply(file_dates[sapply(file_dates, function(fd) fd$date >= as.Date(from) && fd$date <= as.Date(until))], function(fd) fd$path)
  alerts <- lapply(files_in_period, function(alert_file) {
    f <- file(alert_file, "rb")
    df <- jsonlite::stream_in(f, verbose = FALSE)
    close(f)
    # Adding default valur for same_weekday_baseline if does not exists
    if(!("same_weekday_baseline" %in% colnames(df)))
      df$same_weekday_baseline <- sapply(df$topic, function(t) FALSE)
    # Adding default valur for alpha_outlier if does not exists
    if(!("alpha_outlier" %in% colnames(df)))
      df$alpha_outlier <- as.numeric(sapply(df$topic, function(t) NA))
    # Adding default valur for k_decay if does not exists
    if(!("k_decay" %in% colnames(df)))
      df$k_decay <- as.numeric(sapply(df$topic, function(t) NA))


    df %>% dplyr::filter(
      (if(length(c)==0) TRUE else country %in% c) &
      (if(length(t)==0) TRUE else topic %in% t)
    ) %>%
     dplyr::arrange(topic, country, hour) %>%
     dplyr::group_by(topic, country) %>%
     dplyr::mutate(rank = rank(hour, ties.method = "first")) %>%
     dplyr::ungroup()
  })
  Reduce(x = alerts, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
}


# get the default or user defined subscribed user list
get_subscribers <-function() {
  df <- readxl::read_excel(get_subscribers_path())  
  df$Topics <- as.character(df$Topics)
  df$User <- as.character(df$User)
  df$Email <- as.character(df$Email)
  df$Regions <- as.character(df$Regions)
  df$`Excluded Topics` <- as.character(df$`Excluded Topics`)
  df$`Real time Topics` <- as.character(df$`Real time Topics`)
  df$`Alert Slots` <- as.character(df$`Alert Slots`)
  df
}

# Send email alerts to subscribers based on newly geberated alerts and subscribers configuration
send_alert_emails <- function(tasks = get_tasks()) {
  `%>%` <- magrittr::`%>%`
  task <- tasks$alerts 
  # Creating sending email statistics if any
  if(!exists("sent", tasks$alerts)) tasks$alerts$sent <- list()
  # Getting subscriber users
  subscribers <- get_subscribers()
  if(nrow(subscribers)>0) {
    for(i in 1:nrow(subscribers)) {
      user <- subscribers$User[[i]]
      topics <- if(is.na(subscribers$Topics[[i]])) NA else strsplit(subscribers$Topics[[i]],";")[[1]]
      dest <- if(is.na(subscribers$Email[[i]])) NA else strsplit(subscribers$Email[[i]],";")[[1]]
      regions <- if(is.na(subscribers$Regions[[i]])) NA else strsplit(subscribers$Regions[[i]],";")[[1]]
      excluded <- if(is.na(subscribers$`Excluded Topics`[[i]])) NA else strsplit(subscribers$`Excluded Topics`[[i]], ";")[[1]]         
      realtime_topics <- if(is.na(subscribers$`Real time Topics`[[i]])) NA else strsplit(subscribers$`Real time Topics`[[i]], ";")[[1]]         
      realtime_regions <- if(is.na(subscribers$`Real time Regions`[[i]])) NA else strsplit(subscribers$`Real time Regions`[[i]], ";")[[1]]         
      slots <- as.integer(if(is.na(subscribers$`Alert Slots`[[i]])) NA else strsplit(subscribers$`Alert Slots`[[i]], ";")[[1]])
      # Adding users statistics if does not existd already
      if(!exists(user, where=tasks$alerts$sent)) 
        tasks$alerts$sent[[user]] <- list()
      # Getting last day alerts date period
      agg_period <- get_aggregated_period("country_counts")
      alert_date <- agg_period$last
      alert_hour <- agg_period$last_hour

      # Getting last day alerts for subscribed user and regions
      user_alerts <- get_alerts(
        topic = if(all(is.na(topics))) NULL else if(!all(is.na(realtime_topics))) unique(c(topics, realtime_topics)) else topics,
        countries = if(all(is.na(regions))) NULL else if(!all(is.na(realtime_regions))) unique(c(regions, realtime_regions)) else regions,
        from = alert_date,
        until = alert_date
      )

      if(!is.null(user_alerts)) {
        # Excluding alerts for topics ignored for this user
        user_alerts <- (
          if(!all(is.na(excluded)))
            user_alerts %>% dplyr::filter(!(topic %in% excluded))
          else 
            user_alerts
        )
        # Excluding alerts that are not the first in the day for the specific topic and region
        user_alerts <- user_alerts %>% dplyr::filter(rank == 1)


        # Defining if this slot corresponds to a user slot
        current_hour <- as.integer(strftime(Sys.time(), format="%H"))
        current_slot <- (
          if(all(is.na(slots))) 
            current_hour
          else if(length(slots[slots <= current_hour])==0) 
            tail(slots, 1) 
          else 
            max(slots[slots <= current_hour])
        )
        
        send_slot_alerts <- (
          all(is.na(slots)) ||  
          (length(slots[slots <= current_hour])>0 && # After first slot in day AND
            (length(slots) == 1 || #Either there is only one slot for the user 
              ! exists("last_slot", where = tasks$alerts$sent[[user]]) || #or there is no register of previous slot executed
              tasks$alerts$sent[[user]]$last_slot != current_slot # Or there are multiple slots and the current slot is different than the previus one
            )
          )
        ) 
            
        # Filtering out alerts that are not instant if  not respect the defined slots
        instant_alerts <- user_alerts %>% dplyr::filter(
          (!all(is.na(realtime_topics)) | !all(is.na(realtime_regions))) & 
          (
            (all(is.na(realtime_topics)) | topic %in% realtime_topics) |
            (all(is.na(realtime_regions)) | country %in% realtime_regions)
          )
        )
        
        # Excluding instant alerts produced before the last alert sent to user
        instant_alerts <- (
          if(exists("date", where = tasks$alerts$sent[[user]]) && 
            exists("hour_instant", where = tasks$alerts$sent[[user]]) &&
            as.Date(tasks$alerts$sent[[user]]$date, format="%Y-%m-%d") == alert_date
          )
            instant_alerts %>% dplyr::filter(hour > tasks$alerts$sent[[user]]$hour_instant)
          else 
            instant_alerts
        )
        
        slot_alerts <- user_alerts %>% dplyr::filter(
         ( (all(is.na(realtime_topics)) & all(is.na(realtime_regions))) |
          (
            (!all(is.na(realtime_topics)) & !(topic %in% realtime_topics)) &
            (!all(is.na(realtime_regions)) & !(country %in% realtime_regions))
          )
         )
          & send_slot_alerts 
        )
        
        # Excluding instant alerts produced before the last alert sent to user
        slot_alerts <- (
          if(exists("date", where = tasks$alerts$sent[[user]]) && 
            exists("hour_slot", where = tasks$alerts$sent[[user]]) &&
            as.Date(tasks$alerts$sent[[user]]$date, format="%Y-%m-%d") == alert_date
            )
            slot_alerts %>% dplyr::filter(hour > tasks$alerts$sent[[user]]$hour_slot)
          else 
            slot_alerts
        )
        # Joining back instant and slot alert for email send

        user_alerts <- dplyr::union_all(slot_alerts, instant_alerts) %>% dplyr::mutate(topic = get_topics_labels()[topic])
        # Sending alert email & registering last sent dates and hour to user
        if(nrow(user_alerts) > 0) {
          # Calculating top alerts for title
          top_alerts <- head(
            user_alerts %>%
              dplyr::arrange(topic, dplyr::desc(number_of_tweets)) %>% 
              dplyr::group_by(topic) %>% 
              dplyr::mutate(rank = rank(topic, ties.method = "first")) %>% 
              dplyr::filter(rank < 3) %>% 
              dplyr::summarize(top = paste(country, collapse = ", "), max_tweets=max(number_of_tweets)) %>%
              dplyr::arrange(dplyr::desc(max_tweets)), 
            3)


          tasks <- update_alerts_task(tasks, "running", paste("sending alert to", dest))
          message(paste("Sending alert to ", dest))

          # Getting the title
          title <- paste(
            "[epitweetr] found", 
            nrow(user_alerts), 
            "signals. Top alerts: ", 
            paste(
              top_alerts$topic, 
              " (", 
              top_alerts$top, 
              ")", 
              collapse = ", ", 
              sep = ""
            )
          )
          # Creating email body
          # Getting an array of countries with alerts sorted on descending ordrt by the total number of tweets
          topics_with_alerts <- (
            user_alerts %>% 
            dplyr::group_by(topic) %>% 
            dplyr::summarize(tweets = sum(number_of_tweets)) %>% 
            dplyr::ungroup() %>%
            dplyr::arrange(dplyr::desc(tweets))
            )$topic
          
          # Getting the html table for each region
          alert_tables <- sapply(topics_with_alerts, function(t) {
            paste(
              "<h5>",
              t,
              "</h5>",
              user_alerts %>% 
                dplyr::filter(topic == t) %>%
                dplyr::arrange(dplyr::desc(number_of_tweets)) %>%
                dplyr::select(
                 `Date` = `date`, 
                 `Hour` = `hour`, 
                 `Topic` = `topic`,  
                 `Region` = `country`,
                 `Top words` = `topwords`, 
                 `Tweets` = `number_of_tweets`, 
                 `% important user` = `known_ratio`,
                 `Threshold` = `limit`,
                 `Baseline` = `no_historic`, 
                 `Alert confidence` = `alpha`,
                 `Outliers confidence` = `alpha_outlier`,
                 `Downweight strength` = `k_decay`,
                 `Bonf. corr.` = `bonferroni_correction`,
                 `Same weekday baseline` = `same_weekday_baseline`,
                 `Day_rank` = `rank`,
                 `With retweets` = `with_retweets`,
                 `Location` = `location_type`
                ) %>%
                xtable::xtable() %>%
                print(type="html", file=tempfile())
            )
          })
          # Applying table inline format 
          alert_tables <- gsub("<table ", "<table style=\"border: 1px solid black;width: 100%;border-collapse: collapse;\" ", alert_tables) 
          alert_tables <- gsub("<th ", "<th style=\"border: 1px solid black;background-color: #3d6806;color: #ffffff;text-align: center;\" ", alert_tables) 
          alert_tables <- gsub("<th> ", "<th style=\"border: 1px solid black;background-color: #3d6806;color: #ffffff;text-align: center;\">", alert_tables) 
          alert_tables <- gsub("<td ", "<td style=\"border: 1px solid black;\" ", alert_tables) 
          alert_tables <- gsub("<td> ", "<td style=\"border: 1px solid black;\">", alert_tables) 
          
          # Applying email template
          t_con <- file(get_email_alert_template_path(), open = "rb", encoding = "UTF-8")
          html <- paste(readLines(t_con, encoding = "UTF-8"), collapse = "\n")
          html <- gsub("@title", title, html)
          html <- gsub("@alerts", paste(alert_tables, collapse="\n"), html)
          close(t_con)

          msg <- ( 
            emayili::envelope() %>% 
            emayili::from(conf$smtp_from) %>% 
            emayili::to(dest) %>% 
            emayili::subject(title) %>% 
            emayili::html(html)
          )
          smtp <- ( 
            if(is.na(conf$smtp_password) || is.null(conf$smtp_password) ||  conf$smtp_password == "") 
              emayili::server(host = conf$smtp_host, port=conf$smtp_port, insecure=conf$smtp_insecure, reuse = FALSE)
            else 
              emayili::server(host = conf$smtp_host, port=conf$smtp_port, username=conf$smtp_login, insecure=conf$smtp_insecure, password=conf$smtp_password, reuse = FALSE)
          )
          smtp(msg)
          
          # Storing last day sendung alerts for current user
          # If new dates resetting hours to 0
          if(exists("date", where = tasks$alerts$sent[[user]]) || tasks$alerts$sent[[user]]$date != alert_date ) {
            tasks$alerts$sent[[user]]$hour_slot <- 0
            tasks$alerts$sent[[user]]$hour_instant <- 0
          }

          # Updating date tu current date
          tasks$alerts$sent[[user]]$date <- alert_date
          
          # Updating slot hour if slots alerts are sent
          if(nrow(slot_alerts)>0) {
            tasks$alerts$sent[[user]]$hour_slot <- alert_hour
            tasks$alerts$sent[[user]]$last_slot <- current_slot
          }
          
          # Updating instant hour if instant alerts are sent
          if(nrow(instant_alerts)>0)
            tasks$alerts$sent[[user]]$hour_instant <- alert_hour

        }
      }
    } 
  }
  tasks
}

