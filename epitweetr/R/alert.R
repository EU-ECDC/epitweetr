#' @title Execute the alert task  
#' @description Evaluate alerts for the last collected day for all topics and regions and send email alerts to subscribers
#' @param tasks Current tasks for reporting purposes, default: get_tasks()
#' @return The list of tasks updated with produced messages
#' @details This function calculates alerts for the last aggregated day and then send emails to subscribers.
#'
#' The alert calculation is based on the country_counts time series which stores alerts by country, hour and topics.
#'
#' For each country and region, the process starts by aggregating the last N days. A day is a block of consecutive 24 hours ending before the hour of the collected last tweet. 
#' N is defined by the alert baseline parameter on the configuration tab of the Shiny application (the default is N=7).
#' 
#' An alert will be produced when the number of tweets observed is above the threshold calculated by the modified version of the EARS algorithm (for more details see the package vignette). 
#' The behaviour of the alert detection algorithm is modified by the signal false positive rate (alpha), downweighting of previous alerts and weekly or daily baseline parameters 
#' as defined on the configuration tab of the Shiny application and the topics file.
#'
#' A prerequisite to this function is that the \code{\link{search_loop}} must already have stored collected tweets in the search folder and that the geotagging and aggregation tasks have already been run.
#' Normally this function is not called directly by the user but from the \code{\link{detect_loop}} function.
#' @examples 
#' if(FALSE){
#'    library(epitweetr)
#'    # setting up the data folder
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'
#'    # calculating alerts for last day tweets and sending emails to subscribers
#'    generate_alerts()
#' }
#' @seealso 
#'  \code{\link{detect_loop}}
#'  
#' @rdname generate_alerts
#' @importFrom stats sd qt
#' @export
generate_alerts <- function(tasks = get_tasks()) {
  tasks <- tryCatch({
    tasks <- update_alerts_task(tasks, "running", "processing", start = TRUE)
    # detecting alerts for the last aggregated date
    tasks <- do_next_alerts(tasks)

    tasks <- update_alerts_task(tasks, "running", "sending emails")
    # Sending email alerts 
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


#' @title algorithm for outbreak detection, extends the EARS algorithm
#' @author Michael Hoehle <https://www.math.su.se/~hoehle>
#' @description The simple 7 day running mean version of the Early Aberration Reporting System (EARS) 
#' algorithm is extended as follows:
#' \itemize{
#'   \item{proper computation of the prediction interval}
#'   \item{downweighting of previous signals, similar to the approach by Farrington (1996)}
#' }
#' @param ts A numeric vector containing the counts of the univariate
#' time series to monitor. The last time point in ts is
#' investigated
#' @param alpha The alpha is used to compute the upper limit of the prediction interval:
#' (1-alpha) * 100\%, default: 0.025
#' @param alpha_outlier Residuals beyond 1-alpha_outlier quantile of the 
#'              the t(n-k-1) distribution are downweighted, default: 0.05
#' @param k_decay Power k in the expression (r_star/r_threshold)^k determining the weight, default: 4
#' @param no_historic Number of previous values i.e -1, -2, ..., no_historic
#' to include when computing baseline parameters, default: 7
#' @param same_weekday_baseline whether to calculate baseline using same weekdays or any day, default: FALSE
#' @return A dataframe containing the monitored time point,
#'         the upper limit and whether a signal is detected or not.
#' @details for algorithm details see package vignette.
#' @examples
#' if(FALSE){
#'    library(epitweetr)
#'    #Running the modifies version of the ears algorithm for a particular data series
#'     ts <- c(150, 130, 122, 160, 155, 128, 144, 125, 300, 319, 289, 277, 500)
#'     show(ears_t_reweighted(ts))
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
      
      # Helper function to compute threshold based on weights 
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
        # Return results
        return(list(y0bar=y0bar, sd0=sd0, U0=U0, alarm0=alarm0))
      }      
      
      # Results without reweighting
      res_noreweight <- compute_threshold( w=rep(1,n))
    
      # Extract fit from inversion  initial w=1 version
      y0bar <- res_noreweight$y0bar
      
      ## Calculate the raw residuals
      e <- (y_historic - y0bar)
      # Diagonal of hat matrix - analytic
      pii <- rep(1/no_historic, no_historic)
      # Hat matrix from regression with i'th observation removed is always the same for intercept only model
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
      
      # Define threshold for outliers
      reweight_threshold <- qt(1-alpha_outlier, df=n - k - 1)
      
      # Weights 1: Drop outliers
      # w_drop <- ifelse( rstar > reweight_threshold, 0, 1)
      # res_drop <- compute_threshold(w_drop)
      
      # Weights 2: Farrington procedure, down weight
      w_dw <- ifelse( rstar > reweight_threshold, (reweight_threshold/rstar)^k_decay, 1)
      res_dw <- compute_threshold(w_dw)
      
      ## Return a vector with the components of the computation
      c(time_monitored=t, y0=y0, U0=res_noreweight$U0, alarm0=res_noreweight$alarm0, y0bar = res_noreweight$y0bar, U0_dw=res_dw$U0, alarm0_dw=res_dw$alarm0)
    }})) %>% as.data.frame()
}


# Getting alert daily counts taking in consideration a 24-hour sliding window since last full hour
# the provided dataset is expected to be an hourly count by date with the following columns: tweets, known_users, created_date, created_hour
get_reporting_date_counts <- function(
    df
    , topic
    , start
    , end
    , last_day
  ) {
  `%>%` <- magrittr::`%>%`
  if(nrow(df)>0) {
    # Calculating end day hour which is going to be the last fully collected hour when 
    # requesting the last collected dates or 23 hours if it is a fully collected day 
    last_full_hour <- (
      if(!is.na(end) && end < last_day) 
        23
      else
        as.integer(strftime(max(as.POSIXlt(df$created_date) + (as.integer(df$created_hour) - 1) * 3600), "%H"))
    )
    #Handling going to previous day if last full hour is negative
    if(last_full_hour < 0) {
      last_full_hour = 23
      last_day <- last_day - 1  
    }
    # Setting the reporting date based on the cut-off hour
    df$reporting_date <- ifelse(df$created_hour <= last_full_hour, df$created_date, df$created_date + 1)

    # filtering by start date
    df <- (
      if(is.na(start)) df 
      else dplyr::filter(df, .data$reporting_date >= start)
    )
    # filtering by end date
    df <- (
      if(is.na(end)) df 
      else dplyr::filter(df, .data$reporting_date <= end)
    )
    
    # group by reporting date
    df %>%
      dplyr::group_by(.data$reporting_date) %>% 
      dplyr::summarise(count = sum(.data$tweets), known_users = sum(.data$known_users)) %>% 
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
  )) %>% dplyr::filter(.data$topic == f_topic)

  # Adding retweets on count if requested
  df <- if(with_retweets){
    df %>% dplyr::mutate(
      tweets = ifelse(is.na(.data$retweets), 0, .data$retweets) + ifelse(is.na(.data$tweets), 0, .data$tweets),
      known_users = .data$known_retweets + .data$known_original
    )
  } else {
    df %>% dplyr::rename(
      known_users = .data$known_original
    ) 
  }
  if(!is.null(logenv)) {
    # Setting global variable for storing total number of tweets concerned including all country_cols
    total_df <- df %>% dplyr::filter(
      (
       length(country_codes) == 0 
       |  (.data$tweet_geo_country_code %in% country_codes) 
       |  (.data$user_geo_country_code %in% country_codes) 
      )
    )
    total_count <- sum((get_reporting_date_counts(total_df, topic, read_from_date, end, end) %>% dplyr::filter(.data$reporting_date >= start))$count)
    logenv$total_count <- if(exists("total_count", logenv)) logenv$total_count + total_count else total_count
  }
  # filtering by country codes
  df <- (
    if(length(country_codes) == 0) df 
    else if(length(country_code_cols) == 1) dplyr::filter(df, .data$topic == f_topic & (!!as.symbol(country_code_cols[[1]])) %in% country_codes) 
    else if(length(country_code_cols) == 2) dplyr::filter(df, .data$topic == f_topic & (!!as.symbol(country_code_cols[[1]])) %in% country_codes | (!!as.symbol(country_code_cols[[2]])) %in% country_codes)
    else stop("get geo count does not support more than two country code columns") 
  )
  # Getting univariate time series aggregating by day
  counts <- get_reporting_date_counts(df, topic, read_from_date, end, end)
  if(nrow(counts)>0) {
    # filling missing values with zeros if any
    date_range <- as.numeric(read_from_date):as.numeric(end)
    missing_dates <- date_range[sapply(date_range, function(d) !(d %in% counts$reporting_date))]
    if(length(missing_dates > 0)) {
      missing_dates <- data.frame(reporting_date = missing_dates, count = 0)
      counts <- dplyr::bind_rows(counts, missing_dates) %>% dplyr::arrange(.data$reporting_date) 
    }
    # Calculating alerts
    alerts <- ears_t_reweighted(counts$count, alpha=alpha/bonferroni_m, alpha_outlier = alpha_outlier, k_decay = k_decay, no_historic = no_historic, same_weekday_baseline)
    counts$alert <- alerts$alarm0_dw
    counts$limit <- alerts$U0_dw
    counts$baseline <- alerts$y0bar
    counts <- if(is.na(start))
      counts
    else
      counts %>% dplyr::filter(.data$reporting_date >= start)
    counts
  } else {
    counts$alert <- logical()
    counts$limit <- numeric()
    counts$baseline <- numeric()
    counts
  }
}

# Calculating alerts for a set of regions and a specific period
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

  # Getting region details
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
    alerts <- dplyr::rename(alerts, date = .data$reporting_date) 
    alerts <- dplyr::rename(alerts, number_of_tweets = .data$count) 
    alerts$date <- as.Date(alerts$date, origin = '1970-01-01')
    if(nrow(alerts)>0) alerts$country <- all_regions[[regions[[i]]]]$name
    alerts$known_ratio = alerts$known_users / alerts$number_of_tweets
    alerts
  })

  # Joining alerts generated for each region
  df <- Reduce(x = series, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
  if(date_type =="created_weeknum"){
    df <- df %>% 
      dplyr::group_by(week = strftime(.data$date, "%G%V"), .data$country) %>% 
      dplyr::summarise(c = sum(.data$number_of_tweets), a = max(.data$alert), d = min(.data$date), ku = sum(.data$known_users), kr = sum(.data$known_users)/sum(.data$number_of_tweets), l = 0, b = 0) %>% 
      dplyr::ungroup() %>% 
      dplyr::select(.data$d , .data$c, .data$ku, .data$a, .data$l, .data$country, .data$kr, .data$l, .data$b) %>% 
      dplyr::rename(date = .data$d, number_of_tweets = .data$c, alert = .data$a, known_users = .data$ku, known_ratio = .data$kr, limit = .data$l, baseline = .data$b)
  }
  return(df)
}


# get alert count start depending on the baseline type: any day or same weekday
get_alert_count_from <- function(date, baseline_size, same_weekday_baseline) {
  if(!same_weekday_baseline)
    date - (baseline_size + 2)
  else
    date - (7 * baseline_size + 2)
}

# getting parameters for current alert generation
# This will be based on last successfully aggregated date and it will only be generated if once each scheduling span
do_next_alerts <- function(tasks = get_tasks()) {
  `%>%` <- magrittr::`%>%`
  # Getting period for last alerts
  alert_to <- NA
  alert_to_hour <- NA
  wait_for <- 0
  while(is.na(alert_to) || is.na(alert_to_hour)) { 
    last_agg <- get_aggregated_period()
    alert_to <- last_agg$last
    alert_to_hour <- last_agg$last_hour
    if(is.na(alert_to) || is.na(alert_to_hour)) {
      message(paste("Cannot determine the last aggregated period for alert detection, tryng again in", wait_for, " seconds"))
      Sys.sleep(wait_for)
      wait_for <- wait_for + 10
    }
    if(wait_for > 30)
      stop("Cannot determine the last aggregated period for alert detection. Please check that series have been aggregated")
  }
  

  # Determining whether we should produce alerts for current hour (if aggregation has produced new records since last alert generation
  do_alerts <- FALSE
  alert_file <-get_alert_file(alert_to)
  if(!file.exists(alert_file)) { 
    do_alerts <- TRUE
  } else {
    f <- file(alert_file, "rb")
    existing_alerts <- jsonlite::stream_in(f, verbose = FALSE)
    close(f)
    if(nrow(existing_alerts)==0 || nrow(existing_alerts %>% dplyr::filter(.data$date == alert_to & .data$hour == alert_to_hour)) == 0)
      do_alerts <- TRUE
  }
  if(do_alerts) {
    # Getting region details
    regions <- get_country_items()
    cores <- as.numeric(conf$spark_cores)
    cl <- parallel::makePSOCKcluster(cores, outfile="")
    on.exit(parallel::stopCluster(cl))
    data_dir <- conf$data_dir
    topics <- unique(lapply(conf$topics, function(t) t$topic))
    parallel::clusterExport(cl, 
      list(
        "setup_config",
        "data_dir", 
        "topics", 
        "regions", 
        "alert_to", 
        "get_country_items", 
        "update_alerts_task",
        "cores",
        "tasks",
        "get_topics_alphas",
        "get_topics_alpha_outliers",
        "get_topics_k_decays",
        "setup_config"
      )
      , envir=rlang::current_env()
    )
    
    # calculating alerts per topic
    alerts <- parallel::parLapply(cl, 1:length(topics), function(i) {
    #alerts <- lapply(1:length(topics), function(i) {
      topic <- topics[[i]]
      setup_config(data_dir)
      m <- paste("Getting alerts for",topic,i,  alert_to, (Sys.time())) 
      message(m) 
      if(i %% cores == 0) {
        tasks <- update_alerts_task(tasks, "running", m)
      }
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
      dplyr::filter(!is.na(.data$alert) & .data$alert == 1)
    })
    
    # Joining all day alerts on a single data frame
    alerts <- Reduce(x = alerts, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
    # If alert prediction model has been trained, alerts can be predicted
    if(nrow(alerts)>0) {
      # Adding top items
      codeMap <- get_country_codes_by_name()
      ts <- unique(alerts$topic)
      for(serie in list(
        list(name = "topwords", col = "token"),
        list(name = "hashtags", col = "hashtag"),
        list(name = "urls", col = "url"),
        list(name = "contexts", col = "context"),
        list(name = "entities", col = "entity")
        )) {
        m <- paste("Adding",serie$name) 
        message(m)  

        data <- get_aggregates(serie$name, filter = list(topic = ts , period = c(alert_to, alert_to)))
        data$frequency <- data$original
        data$item <- data[[serie$col]]
        new_col <- mapply(
          function(t, d, r) {
            codes <- codeMap[[r]]
            if(is.null(codes)) {
              "" 
            } else {
              tps <- (data %>%
                dplyr::filter(
                  .data$topic == t
                  & .data$created_date == d
                  & (if(length(codes)==0) TRUE else .data$tweet_geo_country_code %in% codes )
                ) %>% 
                dplyr::filter(!is.na(.data$frequency)) %>% 
                dplyr::group_by(.data$item) %>%
                dplyr::summarize(frequency = sum(.data$frequency)) %>%
                dplyr::ungroup() %>%
                dplyr::arrange(-.data$frequency) %>%
                head(10) #%>%
                #dplyr::mutate(item = reorder(.data$item, .data$frequency))
              )
              if(nrow(tps) == 0) 
                ""
              else 
                paste(paste(tps$item, " (", tps$frequency, ")", collapse = ", ", sep = ""))
            }
          }, 
          alerts$topic, 
          alerts$date,
          alerts$country
        )
        alerts[[serie$name]] <- new_col
      }
      # Adding used parameters
      alerts <- alerts %>% 
        dplyr::mutate(
          hour = alert_to_hour, 
          location_type = "tweet", 
          with_retweets = conf$alert_with_retweets, 
          alpha = as.numeric(get_topics_alphas()[.data$topic]), 
          alpha_outlier = as.numeric(get_topics_alpha_outliers()[.data$topic]), 
          k_decay = as.numeric(get_topics_k_decays()[.data$topic]), 
          no_historic = as.numeric(conf$alert_history), 
          no_historic = as.numeric(conf$alert_history),
          bonferroni_correction = conf$alert_with_bonferroni_correction,
          same_weekday_baseline = conf$same_weekday_baseline,
          
        )
      alert_training <- get_alert_training_df()
      if(length(unique(alert_training$epitweetr_category))>1) {
        # Adding tweets to alerts
        m <- paste("Adding toptweets") 
        message(m)  
        tasks <- update_alerts_task(tasks, "running", m)
        alerts <- add_toptweets(alerts, 10)
        m <- paste("Classifying alerts toptweets") 
        message(m)  
        tasks <- update_alerts_task(tasks, "running", m)
        alerts <- classify_alerts(alerts %>% dplyr::mutate(test = FALSE, augmented = FALSE, deleted = FALSE), retrain = FALSE)
        # Removing top tweets
        alerts$toptweets <- NULL
        alerts$id <- NULL
      } 
      f <- file(alert_file, open = "ab")
      jsonlite::stream_out(alerts, f, auto_unbox = TRUE)
      close(f)
    }
  } else {
    message("no alerts to recalculate")
  }
  tasks
}

 
#' @title Getting signals produced by the task \code{\link{generate_alerts}} of \code{\link{detect_loop}}
#' @description Returns a data frame of signals produced by the \code{\link{detect_loop}}, which are stored on the signal folder.
#' @param topic Character vector. When it is not empty it will limit the returned signals to the provided topics, default: character()
#' @param countries Character vector containing the names of countries or regions or a numeric vector containing the indexes of countries 
#' as displayed at the Shiny App to filter the signals to return., default: numeric()
#' @param from Date defining the beginning of the period of signals to return, default: '1900-01-01'
#' @param until Date defining the end of the period of signals to return, default: '2100-01-01'
#' @param toptweets Integer number of top tweets to be added to the alert. These are obtained from the tweet index based on topwords and Lucene score, default: 0
#' @param limit Maximum number of alerts returned, default: 0
#' @param duplicates Character, action to decide what to do with alerts generated on the same day. Options are "all" (keep all alerts), "first" get only first alert and "last" for getting only the last alert 
#' @param progress Function, function to report progress it should receive two parameter a progress between 0 and 1 and a message, default: empty function 
#' @return a data frame containing the calculated alerts for the period. If no alerts are found then NULL is returned 
#' @details For more details see the package vignette.
#' @examples
#' if(FALSE){
#'    library(epitweetr)
#'    # setting up the data folder
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose()) 
#'
#'    # Getting signals produced for last 30 days for a particular country
#'    get_alerts(
#'      countries = c("Chile", "Australia", "France"), 
#'      from = as.Date(Sys.time())-30, 
#'      until = as.Date(Sys.time())
#'    )
#' }
#' @seealso
#' \code{\link{generate_alerts}}
#'
#' \code{\link{detect_loop}}
#' @rdname get_alerts
#' @export
#' @importFrom magrittr `%>%`
#' @importFrom jsonlite stream_in
#' @importFrom dplyr filter arrange group_by mutate ungroup bind_rows
#' @importFrom stats reorder sd qt
#' @importFrom utils head
get_alerts <- function(topic=character(), countries=numeric(), from="1900-01-01", until="2100-01-01", toptweets = 0, limit = 0, duplicates = "all", progress = function(a, b) {}) {
  `%>%` <- magrittr::`%>%`
  # preparing files and renaming variables with names as column on data frame to avoid conflicts
  regions <- get_country_items()
  t <- topic
  cnames <- ( 
    if(class(countries) %in% c("numeric", "integer"))
      lapply(countries, function(i) regions[[i]]$name)
    else 
      countries
  )
  alert_files <- list.files(file.path(conf$data_dir, "alerts"), recursive=TRUE, full.names =TRUE)  
  if(length(alert_files)==0) {
    NULL
  } else {
    file_dates <- lapply(alert_files, function(f) {list(path = f, date = as.Date(substr(tail(strsplit(f, "/")[[1]], n = 1), 1, 10), format="%Y.%m.%d"))})
    files_in_period <- lapply(file_dates[sapply(file_dates, function(fd) fd$date >= as.Date(from) && fd$date <= as.Date(until))], function(fd) fd$path)
    alerts <- lapply(files_in_period, function(alert_file) {
      f <- file(alert_file, "rb")
      df <- jsonlite::stream_in(f, verbose = FALSE)
      close(f)
      # Adding default value for same_weekday_baseline if does not exists
      if(!("same_weekday_baseline" %in% colnames(df)))
        df$same_weekday_baseline <- sapply(df$topic, function(t) FALSE)
      # Adding default value for alpha_outlier if does not exists
      if(!("alpha_outlier" %in% colnames(df)))
        df$alpha_outlier <- as.numeric(sapply(df$topic, function(t) NA))
      # Adding default value for k_decay if does not exists
      if(!("k_decay" %in% colnames(df)))
        df$k_decay <- as.numeric(sapply(df$topic, function(t) NA))
      # Adding default hashtags
      if(!("hashtags" %in% colnames(df)))
        df$hashtags <- sapply(df$topic, function(t) NA)
      # Adding default urls
      if(!("urls" %in% colnames(df)))
        df$urls <- sapply(df$topic, function(t) NA)
      # Adding default entities
      if(!("entities" %in% colnames(df)))
        df$entities <- sapply(df$topic, function(t) NA)
      # Adding default contexts
      if(!("contexts" %in% colnames(df)))
        df$contexts <- sapply(df$topic, function(t) NA)

      df <- df %>% dplyr::filter(
        (if(length(cnames)==0) TRUE else .data$country %in% cnames) &
        (if(length(t)==0) TRUE else .data$topic %in% t)
      ) %>%
       dplyr::arrange(.data$topic, .data$country, .data$hour, .data$number_of_tweets) %>%
       dplyr::group_by(.data$topic, .data$country) %>%
       dplyr::mutate(rank = rank(.data$hour, ties.method = "first")) %>%
       dplyr::ungroup()

      if(duplicates == "last") {
        df <- df %>% 
          dplyr::arrange(.data$topic, .data$country, dplyr::desc(.data$hour)) %>%
          dplyr::distinct(.data$topic, .data$country, .keep_all = TRUE)
      } else if(duplicates == "first") {
        df <- df %>% dplyr::filter(.data$rank == 1)
      }
      df
    })
  
    df <- Reduce(x = alerts, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
    if(!is.null(df)) {
      if(limit > 0 && limit < nrow(df)) {
        set.seed(26062012)
        df <- df[sample(nrow(df), limit),]
      }
      # Adding top tweets if required
      if(toptweets > 0) {
        df <- add_toptweets(df, toptweets, progress = progress)
      }
      # Calculating top columns
      df$tops <- paste(
        "<UL>",
        ifelse(!is.na(df$topwords), paste("<li><b>Top Words</b>: ", df$topwords, "</li>\n"), ""),
        ifelse(!is.na(df$hashtags), paste("<li><b>Top Hashtags</b>: ", df$hashtags, "</li>\n"), ""),
        ifelse(!is.na(df$entities), paste("<li><b>Top Entities</b>: ", df$entities, "</li>\n"), ""),
        ifelse(!is.na(df$contexts), paste("<li><b>Top Contexts</b>: ", df$contexts, "</li>\n"), ""),
        ifelse(!is.na(df$urls) & nchar(df$urls) > 0, paste("<li><b>Top Urls</b>: ", sapply(strsplit(df$urls, ", "), function(s) {
            if(!is.null(s) && !is.na(s) && length(s) > 0 && nchar(s) > 0) {
              urls <- sapply(strsplit(s, " \\("), function(f) f[[1]])
              counts <- sapply(strsplit(s, " \\("), function(f) paste(" (", if(length(f) > 1) f[[2]] else "-", sep = ""))
              paste(sapply(1:length(urls), function(i) paste("<a target = \"_blank\" href = \"",urls[[i]], "\">",urls[[i]], counts[[i]],"</a>", sep = "")), collapse = ", ")
            } else ""
          }), "</li>\n"), ""),
        "</UL>",
        sep = ""
      )
      if(nrow(df) > 0 && !"epitweetr_category" %in% colnames(df))
        df$epitweetr_category <- NA
      
      # overriding classification if provided by users$
      alert_training <- get_alert_training_df()
      given <- setNames(alert_training$given_category, tolower(paste(alert_training[["date"]], alert_training[["topic"]], alert_training[["country"]], sep = "@@")))
      given <- given[!is.na(given)]
      ngiven <- names(given)
     
      
      if(nrow(df) > 0) {
        df$epitweetr_category <- sapply(1:nrow(df), function(i) {
          key = tolower(paste(df[["date"]][[i]], df[["topic"]][[i]], df[["country"]][[i]], sep = "@@"))
          if(key %in% ngiven)
            given[[key]]
          else
            df$epitweetr_category[[i]]
        })
      }
      progress(1, "Alerts obtained")
      tibble::as_tibble(df)
    }
    else 
      NULL
  }
}

# Add top tweets from the tweet index to the alert data frame
# A query is done to the index for each topic alert to consider top words detected
add_toptweets <- function(df, toptweets, ignore_place = FALSE, progress = function(a, b) {}) {
  codes <- get_country_codes_by_name()
  topwords <- sapply(strsplit(df$topwords, "\\W+|[0-9]\\W"), function(v) v[nchar(v)>0])

  df$toptweets <- if(nrow(df)==0) character() else { 
    sapply(1:nrow(df), function(i) { 
      created_from = (
        if(df$hour[[i]] == 23) { df$date[[i]]
        } else if(df$hour[[i]] < 9) { paste0(as.character(as.Date(df$date[[i]]) - 1), "T0", df$hour[[i]] + 1)
        } else paste0(as.character(as.Date(df$date[[i]]) - 1), "T", df$hour[[i]] + 1)
      )
      created_to = (
        if(df$hour[[i]] == 23) { paste0(df$date[[i]], "TZ") 
        } else if(df$hour[[i]] < 10) { paste0(df$date[[i]], "T0", df$hour[[i]],"Z")
        } else paste0(df$date[[i]], "T", df$hour[[i]], "Z")
      )
      match_codes <- codes[[df$country[[i]]]]
      if(is.null(match_codes))
        ""
      else {
        progress(i/nrow(df), "Getting alerts tweets")
        tweets <- lapply(conf$languages, function(lang) {
          res <- search_tweets(
            query = paste0(
              "created_at:[",created_from ," TO ", created_to,"] AND ",
              paste0("lang:", lang$code," AND "),
              (if(length(match_codes)==0 || ignore_place) "" else paste0("text_loc.geo_country_code:", paste0(match_codes, collapse=";")," AND " )),
              (if(length(topwords[[i]]) == 0) "" else paste0("(", paste0(paste0("\"", topwords[[i]], "\""), collapse=" OR "),") AND " )),
              "is_retweet:false"
            ), 
            topic = df$topic[[i]], 
            from = as.character(as.Date(df$date[[i]])-1), 
            to = df$date[[i]], 
            max = toptweets,
            by_relevance = TRUE
          )
          if(length(res) == 0)
            character(0)
          else
            res$text
        })
        jsonlite::toJSON(setNames(
          tweets, 
          lapply(conf$languages, function(l) l$code)
        )) 
      }
    })
  }
  df$toptweets <- lapply(df$toptweets, function(json) jsonlite::fromJSON(json))
  df
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
  df$`Alert category` <- if(!"Alert category" %in% colnames(df)) NA else as.character(df$`Alert category`)
  df$`Alert Slots` <- as.character(df$`Alert Slots`)
  df$`One tweet alerts` <- if(!"One tweet alerts" %in% colnames(df)) TRUE else ifelse(as.character(df$`One tweet alerts`) %in% c("0", "no", "No", "FALSE"),FALSE,TRUE)
  df$`Topics ignoring 1 tweet alerts` <- if(!"Topics ignoring 1 tweet alerts" %in% colnames(df)) NA else as.character(df$`Topics ignoring 1 tweet alerts`)
  df$`Regions ignoring 1 tweet alerts`<- if(!"Regions ignoring 1 tweet alerts" %in% colnames(df)) NA else as.character(df$`Regions ignoring 1 tweet alerts`)
  df
}

# Send email alerts to subscribers based on newly generated alerts and subscribers' configuration
send_alert_emails <- function(tasks = get_tasks()) {
  `%>%` <- magrittr::`%>%`
  task <- tasks$alerts 
  # Creating sending email statistics if any
  if(!exists("sent", tasks$alerts)) tasks$alerts$sent <- list()
  # Getting subscriber users
  subscribers <- get_subscribers()
  # iterating for each subscriber
  if(nrow(subscribers)>0 && conf$smtp_host != "") {
    for(i in 1:nrow(subscribers)) {
      # getting values subscriber settings 
      user <- subscribers$User[[i]]
      topics <- if(is.na(subscribers$Topics[[i]])) NA else strsplit(subscribers$Topics[[i]],";")[[1]]
      dest <- if(is.na(subscribers$Email[[i]])) NA else strsplit(subscribers$Email[[i]],";")[[1]]
      regions <- if(is.na(subscribers$Regions[[i]])) NA else strsplit(subscribers$Regions[[i]],";")[[1]]
      excluded <- if(is.na(subscribers$`Excluded Topics`[[i]])) NA else strsplit(subscribers$`Excluded Topics`[[i]], ";")[[1]]         
      realtime_topics <- if(is.na(subscribers$`Real time Topics`[[i]])) NA else strsplit(subscribers$`Real time Topics`[[i]], ";")[[1]]         
      realtime_regions <- if(is.na(subscribers$`Real time Regions`[[i]])) NA else strsplit(subscribers$`Real time Regions`[[i]], ";")[[1]]         
      alert_categories <- if(!"Alert category" %in% colnames(subscribers) || is.na(subscribers$`Alert category`[[i]])) NA else strsplit(subscribers$`Alert category`[[i]], ";")[[1]]         
      slots <- as.integer(
        if(is.na(subscribers$`Alert Slots`[[i]])) 
          NA 
        else { 
          times <- strsplit(subscribers$`Alert Slots`[[i]], ";")[[1]]
          times <- sapply(
            times, 
            function(t) {
              parts <- as.integer(strsplit(t, "[^0-9]")[[1]]);
              if(length(parts)<=1) 
                parts 
              else 
                parts[[1]] + parts[[2]]/60
            }
          )
        }
      )
      one_tweet_alerts <- subscribers$`One tweet alerts`[[i]]
      ignore_one_tweet_topics <- if(is.na(subscribers$`Topics ignoring 1 tweet alerts`[[i]])) NA else strsplit(subscribers$`Topics ignoring 1 tweet alerts`[[i]], ";")[[1]]
      ignore_one_tweet_regions <- if(is.na(subscribers$`Regions ignoring 1 tweet alerts`[[i]])) NA else strsplit(subscribers$`Regions ignoring 1 tweet alerts`[[i]], ";")[[1]]

      # Adding users' statistics if these do not exist already
      if(!exists(user, where=tasks$alerts$sent)) 
        tasks$alerts$sent[[user]] <- list()
      # Getting last day alerts date period
      agg_period <- get_aggregated_period()
      alert_date <- agg_period$last
      alert_hour <- agg_period$last_hour

      # Getting last day alerts for subscribed user and regions
      user_alerts <- get_alerts(
        topic = if(all(is.na(topics))) NULL else if(!all(is.na(realtime_topics))) unique(c(topics, realtime_topics)) else topics,
        countries = if(all(is.na(regions))) NULL else if(!all(is.na(realtime_regions))) unique(c(regions, realtime_regions)) else regions,
        from = alert_date,
        until = alert_date
      )
      
      # Limiting to alerts on selected alert_categories
      if(!is.null(user_alerts) && !all(is.na(alert_categories))) {
        user_alerts <- user_alerts %>% dplyr::filter(.data$epitweetr_category %in% alert_categories) 
      }

      if(!is.null(user_alerts)) {
        # Excluding alerts for topics ignored for this user
        user_alerts <- (
          if(!all(is.na(excluded)))
            user_alerts %>% dplyr::filter(!(.data$topic %in% excluded))
          else 
            user_alerts
        )

        # Getting last metrics per alert by region and topic
        last_metrics <- list()
        if(nrow(user_alerts)>0) {
          user_alerts$key = paste(user_alerts$topic, user_alerts$country, sep = "-")
          for(i in 1:nrow(user_alerts)) {
            key = user_alerts$key[[i]]
            # Getting the maximum values to report for that alert
            if(!exists(paste(key, "number_of_tweets", sep = "-"), last_metrics) || last_metrics[[paste(key, "number_of_tweets", sep = "-")]] < user_alerts$number_of_tweets[[i]]) {
              last_metrics[[paste(key, "number_of_tweets", sep = "-")]] <- user_alerts$number_of_tweets[[i]]
              last_metrics[[paste(key, "baseline", sep = "-")]] <- user_alerts$baseline[[i]]
              last_metrics[[paste(key, "known_users", sep = "-")]] <- user_alerts$known_users[[i]]
            }
            # Getting the last 1 tweet alert per region and topic so the next can be reported as first in day
            if(!exists(paste(key, "last_one_tweet_rank", sep = "-"), last_metrics)) 
              last_metrics[[paste(key, "last_one_tweet_rank", sep = "-")]] <- -1
            if(user_alerts$number_of_tweets[[i]] == 1
              && last_metrics[[paste(key, "last_one_tweet_rank", sep = "-")]] < user_alerts$rank[[i]]
              )
                last_metrics[[paste(key, "last_one_tweet_rank", sep = "-")]] <- user_alerts$rank[[i]]
            
          }
          # Excluding alerts that are not the first in the day for the specific topic and region
          # This is done because only the first alert on the day is reported
          # One exception is done for alerts that have been increased from 1 tweet
          user_alerts <- user_alerts %>% dplyr::filter(.data$rank == 1 | .data$rank == (unlist(last_metrics[paste(.data$key, "last_one_tweet_rank",sep = "-")]) + 1))
          
          # Removing potential duplicates added by the increased from 1 tweet exception
          user_alerts <- user_alerts %>% 
            dplyr::arrange(.data$topic, .data$country, dplyr::desc(.data$hour)) %>%
            dplyr::distinct(.data$topic, .data$country, .keep_all = TRUE)

          # Now metrics are updated to last observed alert
          user_alerts <- user_alerts %>%dplyr::mutate(
            number_of_tweets = unlist(last_metrics[paste(.data$key, "number_of_tweets", sep = "-")]), 
            baseline = unlist(last_metrics[paste(.data$key, "baseline", sep = "-")]), 
            known_users = unlist(last_metrics[paste(.data$key, "known_users", sep = "-")]) 
          )
          #Removing one tweet alerts depending on user options
          user_alerts <- user_alerts %>% dplyr::filter(
            .data$number_of_tweets > 1 | (
              one_tweet_alerts & 
               (all(is.na(ignore_one_tweet_topics)) | !(.data$topic %in% ignore_one_tweet_topics )) &
               (all(is.na(ignore_one_tweet_regions)) | !(.data$country %in% ignore_one_tweet_regions ))
              )
          )
        }

        # Defining if this slot corresponds to a user slot
        current_minutes <- as.numeric(strftime(Sys.time(), format="%M"))
        current_hour <- as.numeric(strftime(Sys.time(), format="%H")) + current_minutes / 60
        
        # Getting the current slot
        current_slot <- (
          if(all(is.na(slots))) 
            as.integer(current_hour)
          else if(length(slots[slots <= current_hour])==0) 
            tail(slots, 1) 
          else 
            max(slots[slots <= current_hour])
        )
        
        # Evaluating if slot alerts should be sent
        send_slot_alerts <- (
          all(is.na(slots)) ||  
          (length(slots[slots <= current_hour])>0 && # After first slot in day AND
            (length(slots) == 1 || #Either there is only one slot for the user 
              ! exists("last_slot", where = tasks$alerts$sent[[user]]) || #or there is no register of previous slot executed
              tasks$alerts$sent[[user]]$last_slot != current_slot # or there are multiple slots and the current slot is different than the previous one
            )
          )
        ) 
            
        # Filtering out alerts that are not instant if these do not respect the defined slots
        instant_alerts <- user_alerts %>% dplyr::filter(
          (!all(is.na(realtime_topics)) | !all(is.na(realtime_regions))) & 
          (
            (all(is.na(realtime_topics)) | .data$topic %in% realtime_topics) |
            (all(is.na(realtime_regions)) | .data$country %in% realtime_regions)
          )
        )
        
        # Excluding instant alerts produced before the last alert sent to user
        instant_alerts <- (
          if(exists("date", where = tasks$alerts$sent[[user]]) && 
            exists("hour_instant", where = tasks$alerts$sent[[user]]) &&
            as.Date(tasks$alerts$sent[[user]]$date, format="%Y-%m-%d") == alert_date
          )
            instant_alerts %>% dplyr::filter(.data$hour > tasks$alerts$sent[[user]]$hour_instant)
          else 
            instant_alerts
        )
        
        # getting slot alerts
        slot_alerts <- user_alerts %>% dplyr::filter(
         ( (all(is.na(realtime_topics)) & all(is.na(realtime_regions))) |
          (
            (!all(is.na(realtime_topics)) & !(.data$topic %in% realtime_topics)) &
            (!all(is.na(realtime_regions)) & !(.data$country %in% realtime_regions))
          )
         )
          & send_slot_alerts 
        )
        
        # Excluding slot alerts produced before the last alert sent to user
        slot_alerts <- (
          if(exists("date", where = tasks$alerts$sent[[user]]) && 
            exists("hour_slot", where = tasks$alerts$sent[[user]]) &&
            as.Date(tasks$alerts$sent[[user]]$date, format="%Y-%m-%d") == alert_date
            )
            slot_alerts %>% dplyr::filter(.data$hour > tasks$alerts$sent[[user]]$hour_slot)
          else 
            slot_alerts
        )
        # Joining back instant and slot alert for email send

        user_alerts <- dplyr::union_all(slot_alerts, instant_alerts) %>% dplyr::mutate(topic = get_topics_labels()[.data$topic])
        # Sending alert email & registering last sent dates and hour to user

        if(nrow(user_alerts) > 0) {
          # Calculating top alerts for title
          top_alerts <- head(
            user_alerts %>%
              dplyr::arrange(.data$topic, dplyr::desc(.data$number_of_tweets)) %>% 
              dplyr::group_by(.data$topic) %>% 
              dplyr::mutate(rank = rank(.data$topic, ties.method = "first")) %>% 
              dplyr::filter(.data$rank < 3) %>% 
              dplyr::summarize(top = paste(.data$country, collapse = ", "), max_tweets=max(.data$number_of_tweets)) %>%
              dplyr::arrange(dplyr::desc(.data$max_tweets)), 
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
          
          # Getting the html table for each region
          non_one_alert_tables <- get_alert_tables(user_alerts %>% dplyr::filter(.data$number_of_tweets > 1), group_topics = TRUE) 
          one_alert_table <- get_alert_tables(user_alerts %>% dplyr::filter(.data$number_of_tweets == 1), group_topics = FALSE, ungrouped_title = "1 Tweet alerts")
          
          # Applying email template
          t_con <- file(get_email_alert_template_path(), open = "rb", encoding = "UTF-8")
          html <- paste(readLines(t_con, encoding = "UTF-8"), collapse = "\n")
          html <- gsub("@title", title, html)
          html <- gsub("@alerts", paste(c(non_one_alert_tables, one_alert_table), collapse="\n"), html)
          close(t_con)

          
          # creating the message to send
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
          tryCatch({
            smtp(msg)
            },
            error = function(e) {
              message(paste("Error when sending email", e))
              stop(e)
            }
          )
          
          # Storing last day sending alerts for current user
          # If new dates resetting hours to 0
          if(exists("date", where = tasks$alerts$sent[[user]]) && tasks$alerts$sent[[user]]$date != alert_date ) {
            tasks$alerts$sent[[user]]$hour_slot <- 0
            tasks$alerts$sent[[user]]$hour_instant <- 0
          }

          # Updating date to current date
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

# Produce html tables for alerts provided. This is used on for email alerts.
# Tables can be grouped by topic or not.
get_alert_tables <- function(alerts, group_topics = TRUE, ungrouped_title = "Alerts") {
  # Getting an array of countries with alerts sorted on descending order by the total number of tweets
  topics <- (
    alerts %>% 
    dplyr::group_by(.data$topic) %>% 
    dplyr::summarize(tweets = sum(.data$number_of_tweets)) %>% 
    dplyr::ungroup() %>%
    dplyr::arrange(dplyr::desc(.data$tweets))
    )$topic
  if(!group_topics)
    topics = NA

  alert_tables <- (
    if(nrow(alerts) > 0) {
      tables <- sapply(topics, function(t) {
        paste(
          "<h5>",
          (if(!group_topics)
            ungrouped_title
          else 
            t
          ),
          "</h5>",
          alerts %>% 
            dplyr::filter(is.na(t) | .data$topic == t) %>%
            dplyr::arrange(dplyr::desc(.data$number_of_tweets)) %>%
            dplyr::select(
             `Date` = .data$`date`, 
             `Hour` = .data$`hour`, 
             `Topic` = .data$`topic`,  
             `Region` = .data$`country`,
             `Tops` = .data$`tops`, 
             `Category` = .data$`epitweetr_category`, 
             `Tweets` = .data$`number_of_tweets`, 
             `% from important user` = .data$`known_ratio`,
             `Threshold` = .data$`limit`,
             `Baseline` = .data$`no_historic`, 
             `Alert confidence` = .data$`alpha`,
             `Outliers confidence` = .data$`alpha_outlier`,
             `Downweight strength` = .data$`k_decay`,
             `Bonf. corr.` = .data$`bonferroni_correction`,
             `Same weekday baseline` = .data$`same_weekday_baseline`,
             `Day_rank` = .data$`rank`,
             `With retweets` = .data$`with_retweets`,
             `Location` = .data$`location_type`
            ) %>%
            xtable::xtable() %>%
            print(type="html", file=tempfile(), sanitize.text.function = function(f) f)
        )
      })
      # Applying table inline format 
      tables <- gsub("<table ", "<table style=\"border: 1px solid black;width: 100%;border-collapse: collapse;\" ", tables) 
      tables <- gsub("<th ", "<th style=\"border: 1px solid black;background-color: #3d6806;color: #ffffff;text-align: center;\" ", tables) 
      tables <- gsub("<th> ", "<th style=\"border: 1px solid black;background-color: #3d6806;color: #ffffff;text-align: center;\">", tables) 
      tables <- gsub("<td ", "<td style=\"border: 1px solid black;\" ", tables) 
      tables <- gsub("<td> ", "<td style=\"border: 1px solid black;\">", tables) 
      tables
    }
    else 
      c("")
  )
  alert_tables
} 

# Read the alert training dataset generated by the system and updated by the user
get_alert_training_df <- function() {
  `%>%` <- magrittr::`%>%`
  data_types<-c("text","text", "text", "text", "numeric", "text", "text", "text")
  current <- readxl::read_excel(get_alert_training_path(), col_types = data_types, sheet = "Alerts")
  current <- current %>% dplyr::transmute(
    date  = .data$`Date`, 
    topic  = .data$`Topic`, 
    country = .data$`Region`, 
    topwords = .data$`Top words`, 
    number_of_tweets = .data$`Tweets`, 
    toptweets = .data$`Top Tweets`, 
    given_category = .data$`Given Category`, 
    epitweetr_category= .data$`Epitweetr Category`
  )
  current$`toptweets` <- lapply(current$`toptweets`, function(json) jsonlite::fromJSON(json))
  current %>% dplyr::arrange(.data$date, .data$topic, .data$country, .data$topwords)
}

get_alert_balanced_df <- function(alert_training_df = get_alert_training_df(), progress = function(value, message) {}) {
  
  allcat_df <- alert_training_df %>% dplyr::filter(!is.na(.data$given_category))%>%dplyr::group_by(.data$given_category) %>% dplyr::count()
  # we choose 25 percent of rows to be used as test set. These rows will not be augmented
  split <- jsonlite::rbind_pages(lapply(1:nrow(allcat_df), function(i) {
    set.seed(20131205)
    x = 1:allcat_df$n[[i]]
    alert_training_df %>% dplyr::filter(.data$given_category == allcat_df$given_category[[i]]) %>% dplyr::mutate(test = sample(x, length(x), replace = FALSE) > length(x) * 0.75)
  }))
  
  test_df <- split %>% dplyr::filter(.data$test) %>% dplyr::mutate(deleted = FALSE, augmented = FALSE)
  training_df <- split %>% dplyr::filter(!.data$test)
  cat_df <- training_df %>% dplyr::filter(!is.na(.data$given_category))%>%dplyr::group_by(.data$given_category) %>% dplyr::count()
  cat_df$missing <- max(cat_df$n) - cat_df$n
  cat_to_augment <- cat_df %>% dplyr::filter(.data$missing > 0)
  na_cat <- alert_training_df %>% dplyr::filter(is.na(.data$given_category)) %>% dplyr::mutate(test = FALSE, deleted = FALSE, augmented = FALSE)
  
  # getting the alerts to augment which are categories with less annotations, we do not consider location for this purpose 
  alerts_to_augment <- training_df %>% dplyr::filter(.data$given_category %in% cat_to_augment$given_category) %>% dplyr::distinct(.data$topic, .data$date, .data$given_category, .keep_all=TRUE)
  alerts_to_augment$hour <- sapply(alerts_to_augment$date, function(v) 23)
  # adding extra tweets ignoring location to build augmented alerts
  alerts_to_augment <- add_toptweets(alerts_to_augment, 1000, ignore_place = TRUE, progress = function(value, message) progress(value*0.9, message))
  
  # Iterating over categories to augment
  # Creating alerts to augment underepresented categories, using same settings than existing alerts but looking for more tweets not limiting by country
  alerts_for_augment <- list()
  smallest_cat <- Inf
  if(nrow(cat_to_augment) > 0) {
    progress(0.9, "Augmenting categories")
    for(icat in 1:nrow(cat_to_augment)) {
       cat <- cat_to_augment$given_category[[icat]]
       to_add <- cat_to_augment$missing[[icat]]
       size_after_augment <- cat_to_augment$n[[icat]]
       tweets_exhausted <- FALSE
       found_tweets <- FALSE
       ialert <- 1
       # Looping over all alerts with extra tweets for this category until no more new tweets are found or the expected number of alerts to add is reach
       while(to_add > 0 && !tweets_exhausted) {
         # looping over alerts to augment which have 
         if(alerts_to_augment$given_category[[ialert]] == cat) {
           toptweets <- alerts_to_augment$toptweets[[ialert]]
           # getting the index of tweets to extract from 5 to five per language, it will start on 10 since these have already been used by original alerts
           if(!exists("itweet", where=toptweets))
             toptweets$itweet <- 7
           # getting tweets to add
           langs <- names(toptweets)
           langs <- langs[langs != "itweet"]
           i0 <- toptweets$itweet + 1
           i1 <- i0 + 5
           # getting new tweets
           new_tweets <- lapply(langs, function(l) {if(length(toptweets[[l]]) >= i1) toptweets[[l]][i0:i1] else if(length(toptweets[[l]])>i0) toptweets[[l]][i0:length(toptweets[[l]])] else list()})
           toptweets$itweet <- i1
           alerts_to_augment$toptweets[[ialert]] <- toptweets
           # counting new tweets
           tcount <- sum(sapply(new_tweets, function(t) length(t)))
           # message(paste("cat", cat, "to_add",to_add, "ialert", ialert, "i0", i0, "i1", i1, "tcount", tcount))
           if(tcount > 0) {
             found_tweets <- TRUE
             new_tweets <- setNames(new_tweets, langs)
             alerts_for_augment[[length(alerts_for_augment) + 1]] <- ( 
               list(
                 date = alerts_to_augment$date[[ialert]],
                 topic = alerts_to_augment$topic[[ialert]],
                 country = alerts_to_augment$country[[ialert]],
                 topwords = alerts_to_augment$topwords[[ialert]],
                 number_of_tweets = alerts_to_augment$number_of_tweets[[ialert]], 
                 toptweets = new_tweets,
                 given_category = alerts_to_augment$given_category[[ialert]],
                 epitweetr_category = NA,
                 augmented = TRUE,
                 test = FALSE
               )
             )
             size_after_augment <- size_after_augment + 1
             to_add <- to_add - 1
           }
         }
         if(ialert == nrow(alerts_to_augment)) {
           ialert <- 1
           if(!found_tweets) 
             tweets_exhausted <- TRUE
           found_tweets <- FALSE
         } else {
           ialert <- ialert + 1
         }
       }
       if(size_after_augment < smallest_cat)
         smallest_cat <- size_after_augment
    }
  }

  # Adding the new alerts to the original set
  training_df$augmented <- FALSE
  if(length(alerts_for_augment) > 0) {
    augmented_alerts <- jsonlite::rbind_pages(list(training_df,
      tibble::tibble(
        date = sapply(alerts_for_augment, function(r) r$date),
        topic = sapply(alerts_for_augment, function(r) r$topic), 
        country = as.character(sapply(alerts_for_augment, function(r) r$country)),
        topwords = sapply(alerts_for_augment, function(r) r$topwords), 
        number_of_tweets = sapply(alerts_for_augment, function(r) r$number_of_tweets), 
        toptweets = lapply(alerts_for_augment, function(r) r$toptweets),
        given_category = sapply(alerts_for_augment, function(r) r$given_category),
        epitweetr_category = as.character(sapply(alerts_for_augment, function(r) r$epitweetr_category)),
        augmented = as.logical(sapply(alerts_for_augment, function(r) r$augmented)),
        test = as.logical(sapply(alerts_for_augment, function(r) r$test))
      )
    ))
  } else
    augmented_alerts = training_df
  balanced_training <- jsonlite::rbind_pages(lapply(1:nrow(cat_df), function(i) {
    augmented_in_cat <- augmented_alerts %>% dplyr::filter(.data$given_category == cat_df$given_category[[i]])
    frac <- smallest_cat / nrow(augmented_in_cat) 
    set.seed(20131205)
    x = 1:nrow(augmented_in_cat)
    augmented_in_cat %>% dplyr::mutate(deleted = sample(x, length(x), replace = FALSE) > length(x) * frac)
  }))

  test_df$augmented <- FALSE
  test_df$deleted <- FALSE
  balanced_alerts <- jsonlite::rbind_pages(list(balanced_training, test_df, na_cat))
  balanced_alerts
}

# Read the alert training runs that will determine the algorithms to test for alert classification
get_alert_training_runs_df <- function() {
  `%>%` <- magrittr::`%>%`
  # Reading runs without type to detect if the balance and force columns are present since added on an intermediate version 
  current <- readxl::read_excel(get_alert_training_path(), sheet = "Runs")
  if("Force to use" %in% colnames(current))
    data_types<-c("numeric","text", "numeric", "numeric", "numeric", "numeric", "text", "text", "text","text", "logical", "logical", "logical", "text", "text")
  else 
    data_types<-c("numeric","text", "numeric", "numeric", "numeric", "numeric", "text", "text", "text","text", "logical", "text", "text")
  
  current <- readxl::read_excel(get_alert_training_path(), col_types = data_types, sheet = "Runs")
  # Adding default values to new columns if not present
  if(!("Force to use" %in% colnames(current))) {
    current$`Force to use` <- FALSE
    current$`Balance classes` <- TRUE
  }
  # giving snake_case format to column names
  current <- current %>% dplyr::transmute(
    ranking  = .data$`Ranking`,
    models  = .data$`Models`, 
    alerts = .data$`Alerts`, 
    runs = .data$`Runs`, 
    f1score = .data$`F1Score`,
    accuracy = .data$`Accuracy`,
    precision_by_class = .data$`Precision by Class`,
    sensitivity_by_class = .data$`Sensitivity by Class`,
    fscore_by_class = .data$`FScore by Class`,
    last_run = .data$`Last run`,
    balance_classes = .data$`Balance classes`,
    force_to_use = .data$`Force to use`,
    active = .data$`Active`,
    documentation = .data$`Documentation`,
    custom_parameters= .data$`Custom Parameters`
  )
  current$`custom_parameters` <- lapply(current$`custom_parameters`, function(json) jsonlite::fromJSON(json))
  current
}

# Classify the provided alerts retraining the models if requested
classify_alerts <- function(alerts, retrain = FALSE, progress = function(value, message) {}) {
  `%>%` <- magrittr::`%>%`
  if(is.null(alerts) || nrow(alerts) == 0)
    alerts
  else if(retrain && min((alerts %>% dplyr::group_by(.data$given_category) %>% dplyr::summarise(n = dplyr::n()) %>% dplyr::filter(!is.na(.data$given_category)))$n) < 10 ) {
    warning("In order to train alerts' classifier all categories must have at least 10 observations")
    alerts
  } else {
    runs <- get_alert_training_runs_df()
    augment_classes <- nrow(runs %>% dplyr::filter(.data$active & .data$balance_classes)) > 0
    if(augment_classes && retrain) {
      progress(value = 0.2, message = "Balancing classes as requested per parameters")
      alerts <- get_alert_balanced_df(alerts, progress = function(value, message) {progress(0.2 + value*0.5, message)})
    } else {
      alerts$augmented <- FALSE
      alerts$deleted <- FALSE
      alerts$test <- FALSE
    }
    alerts <- alerts %>% dplyr::mutate(`id`=as.character(1:nrow(alerts)))
    body = paste("{",
      paste("\"alerts\":", jsonlite::toJSON(alerts, pretty = T, auto_unbox = FALSE)),
      if(retrain)
        paste(",\"runs\":", jsonlite::toJSON(runs, pretty = T, auto_unbox = TRUE))
      else "",
      "}",
      sep = "\n"  
    )
    progress(value = 0.7, message = "Training models")
    post_result <- httr::POST(url=get_scala_alert_training_url(), httr::content_type_json(), body=body, encode = "raw", encoding = "UTF-8")
    if(httr::status_code(post_result) != 200) {
      stop(paste("retrain web service failed with the following output: ", substring(httr::content(post_result, "text", encoding = "UTF-8"), 1, 100), sep  = "\n"))
    } else {
      res <- jsonlite::fromJSON(httr::content(post_result, "text", encoding = "UTF-8"))
      alerts$epitweetr_category <- (alerts %>% dplyr::left_join(res$alerts, by ="id"))[[if("epitweetr_category" %in% colnames(alerts)) "epitweetr_category.y" else "epitweetr_category"]]

      if(retrain) {
        nruns = tibble::as_tibble(res$runs)
        nruns$custom_parameters <- jsonlite::fromJSON(jsonlite::toJSON(nruns$custom_parameters), simplifyVector=F)
        list(alerts=alerts, runs = nruns)
      }
      else
        alerts
    }
  }
}

# Retrain the alert classifiers with current alerts
retrain_alert_classifier <- function(progress = function(value, message) {}) {
  `%>%` <- magrittr::`%>%`
  progress(value = 0.1, message = "Getting alerts to retrain")
  alerts = get_alert_training_df()
  ret <- classify_alerts(alerts, retrain = TRUE, progress = progress)
  if(exists("alerts", where = ret)) {
    progress(value = 0.9, message = "Writing resutls")
    write_alert_training_db(alerts = ret$alerts %>% dplyr::filter(!.data$augmented), runs = ret$runs)
  }
}


# Write the provided alerts and runs to the alert annotation Excel
write_alert_training_db <- function(alerts, runs = get_alert_training_runs_df()) {
  `%>%` <- magrittr::`%>%`
  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "Alerts")
  openxlsx::addWorksheet(wb, "Runs")
  # writing data to the worksheet
  alerts <- alerts %>% dplyr::transmute(
    `Date` = .data$date, 
    `Topic` = .data$topic, 
    `Region` = .data$country, 
    `Top words` = .data$topwords, 
    `Tweets` = .data$number_of_tweets, 
    `Top Tweets` =  lapply(.data$toptweets, function(l) gsub("\", \"", "\",\n        \"", jsonlite::toJSON(l, pretty = TRUE))), 
    `Given Category` = .data$given_category, 
    `Epitweetr Category` = .data$epitweetr_category
  )
  openxlsx::writeDataTable(
    wb, 
    sheet = "Alerts", 
    alerts,
    colNames = TRUE, 
    startRow = 1, 
    startCol = "A"
  )
  runs <- runs %>% dplyr::transmute(
   `Ranking` = .data$ranking,
   `Models` = .data$models, 
   `Alerts` = .data$alerts, 
   `Runs` = .data$runs, 
   `F1Score` = .data$f1score,
   `Accuracy` = .data$accuracy,
   `Precision by Class` = .data$precision_by_class,
   `Sensitivity by Class` = .data$sensitivity_by_class,
   `FScore by Class` = .data$fscore_by_class,
   `Last run` = .data$last_run,
   `Balance classes` = .data$balance_classes,
   `Force to use` = .data$force_to_use,
   `Active` = .data$active,
   `Documentation` = .data$documentation,
   `Custom Parameters` = lapply(.data$custom_parameters, function(l) jsonlite::toJSON(l, auto_unbox = TRUE))
  )
  openxlsx::writeDataTable(
    wb, 
    sheet = "Runs", 
    runs,
    colNames = TRUE, 
    startRow = 1, 
    startCol = "A"
  )

  # setting some minimal formatting
  openxlsx::setColWidths(wb, "Alerts", cols = c(4, 6), widths = c(50))
  openxlsx::setRowHeights(wb, "Alerts", rows = 1, heights = 20)
  openxlsx::setRowHeights(wb, "Runs", rows = 1, heights = 20)
  openxlsx::addStyle(
    wb, 
    sheet = "Alerts", 
    style = openxlsx:: createStyle(fontSize = 10, halign = "center", fgFill = "#ff860d", border = c("top", "bottom", "left", "right"), textDecoration="bold", wrapText = T, fontColour = "#222222"), 
    rows = 1, 
    cols = 1:ncol(alerts), 
    gridExpand = FALSE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "Runs", 
    style = openxlsx:: createStyle(fontSize = 10, halign = "center", fgFill = "#ffde59", border = c("top", "bottom", "left", "right"), textDecoration="bold", wrapText = T, fontColour = "#222222"), 
    rows = 1, 
    cols = 1:ncol(runs), 
    gridExpand = FALSE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "Alerts", 
    style = openxlsx:: createStyle(fontSize = 10, border = c("top", "bottom", "left", "right")), 
    rows = 1:nrow(alerts)+1, 
    cols = 1:ncol(alerts), 
    gridExpand = TRUE
  )
  openxlsx::addStyle(
    wb, 
    sheet = "Runs", 
    style = openxlsx:: createStyle(fontSize = 10, border = c("top", "bottom", "left", "right")), 
    rows = 1:nrow(runs)+1, 
    cols = 1:ncol(runs), 
    gridExpand = TRUE
  )
  openxlsx::freezePane(wb, "Alerts",firstActiveRow = 2)
  openxlsx::freezePane(wb, "Runs",firstActiveRow = 2)
  openxlsx::saveWorkbook(wb, get_user_alert_training_path() , overwrite = TRUE)
}
