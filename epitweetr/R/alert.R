#' Realize alert detection on tweet counts by countries
#' @export
generate_alerts <- function(tasks = get_tasks()) {
  tryCatch({
    tasks <- update_alerts_task(tasks, "running", "processing", start = TRUE)
    
    # Setting status to succès
    tasks <- update_alerts_task(tasks, "success", "", end = TRUE)

  }, error = function(error_condition) {
    # Setting status to failed
    tasks <- update_alerts_task(tasks, "failed", paste("failed while", tasks$alerts$message," alerts ", error_condition))
  })
  return(tasks)
}


#' Simple algorithm for outbreak detection, extends the ears algorithm
#'
#' @param ts A numeric vector containing the counts of the univariate
#'           time series to monitor. The last time point in ts is
#'           investigated
#' @param alpha The upper limit is computed as the limit of a one-sided
#'              (1-alpha) times 100prc prediction interval
#' @param no_historic Number of previous values i.e -1, -2, ..., no_historic
#'                    to include when computing baseline parameters
#' @return A named vector containing the monitored time point,
#'         the upper limit and whether an alarm is sounded or not.
#' Several time points
ears_t <- function(ts, alpha=0.025, no_historic=7L) {
  `%>%` <- magrittr::`%>%`
  if (length(ts) < no_historic) {
    warning("Time series has to be longer than the specified number of historic values to use.")
  }
  t(sapply(1:length(ts), function(t) {
    if(t <  no_historic + 1)
      c(time_monitored=t, y0=ts[[t]], U0=NA, alarm0=NA)
    else {
      ## Extract current value and historic values and calculate empirical
      ## mean and standard deviation
      y_historic <- ts[(t - 1 - no_historic):(t - 1)]
      y0 <- ts[[t]]
      y0bar <- mean(y_historic)
      sd0 <- sd(y_historic)

      ## Upper threshold
      U0 <- y0bar + qt(1-alpha,  df=no_historic - 1) * sd0 * sqrt(1 + 1/no_historic)
      ## Reason to sound an alarm=
      alarm0 <- y0 > U0

      ## Return a vector with the components of the computation
      c(time_monitored=t, y0=y0, U0=U0, alarm0=alarm0)
  }})) %>% as.data.frame()

}

get_geo_counts <- function(df = get_aggregates("country_counts"), topic, country_codes = list(), start = NA, end = NA, country_code_col = "tweet_geo_country_code") {
  `%>%` <- magrittr::`%>%`
  last_day <- max(as.POSIXlt(df$created_date))
  # Calculating end day hour which is going to be the last fully collected hour when 
  # requesting the last colleted dates or 23h if it is a fully collected day 
  last_full_hour <- (
    if(!is.na(end) && end < last_day) 
      23
    else
      as.integer(strftime(max(as.POSIXlt(df$created_date) + (as.integer(df$created_hour) - 1) * 3600), "%H"))
  )
  # Setting the reporting date based on the cutoff hour
  df$reporting_date <- ifelse(df$created_hour <= last_full_hour, df$created_date, df$created_date + 1)
  
  # filtering by topic
  df <- ( df %>% 
    dplyr::rename(col_topic = topic) %>%
    dplyr::filter(col_topic == topic) 
  )
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
  # filtering by country codes
  df <- (
    if(length(country_codes) == 0) df 
    else dplyr::filter(df, (!!as.symbol(country_code_col)) %in% country_codes)
  )
  # group by reporting date
  df %>%
    dplyr::group_by(reporting_date) %>% 
    dplyr::summarise(count = sum(tweets)) %>% 
    dplyr::ungroup()
}

# Getting alerts
get_alerts <- function(df= get_aggregates("country_counts"), topic, country_codes = list(), country_code_col = "tweet_geo_country_code", start = NA, end = NA, alpha = 0.025, no_historic = 7) {
  `%>%` <- magrittr::`%>%`
  # Getting counts from no_historic + 1 days bufore start if available 
  counts <- get_geo_counts(df, topic, country_codes, start - (no_historic - 1) , end, country_code_col)
  if(nrow(counts) > 0) {
    #filling missing values with zeros if any
    date_range <- min(counts$reporting_date):max(counts$reporting_date)
    missing_dates <- date_range[sapply(date_range, function(d) !(d %in% counts$reporting_date))]
    if(length(missing_dates > 0)) {
      missing_dates <- data.frame(reporting_date = missing_dates, count = 0)
      counts <- dplyr::bind_rows(counts, missing_dates) %>% dplyr::arrange(reporting_date) 
    }
 
    #Calculating alerts 
    alerts <- ears_t(counts$count, alpha=alpha, no_historic= no_historic)
    counts$alert <- alerts$alarm0
    counts$limit <- alerts$U0
    counts
  } else {
    counts$alert <- NA
    counts$limit <- NA
    counts
  }
}
