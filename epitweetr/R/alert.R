#' Realize alert detection on tweet counts by countries
#' @export
generate_alerts <- function(tasks = get_tasks()) {
  tryCatch({
    tasks <- update_alerts_task(tasks, "running", "processing", start = TRUE)
    tasks <- do_next_alerts(tasks)

    tasks <- update_alerts_task(tasks, "sending emails", "processing")
    tasks <- send_alert_emails(tasks)
    # Setting status to succes
    tasks <- update_alerts_task(tasks, "success", "", end = TRUE)

  }, error = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$aggregate$message," languages", error_condition))
    tasks <- update_alerts_task(tasks, "failed", paste("failed while", tasks$alerts$message," alerts ", error_condition))
    tasks
  }, warning = function(error_condition) {
    # Setting status to failed
    message("Failed...")
    message(paste("failed while", tasks$aggregate$message," languages", error_condition))
    tasks <- update_alerts_task(tasks, "failed", paste("failed while", tasks$alerts$message," alerts ", error_condition))
    tasks
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
    #warning("Time series has to be longer than the specified number of historic values to use.")
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

get_geo_counts <- function(
    df = get_aggregates("country_counts")
    , topic
    , country_codes = list()
    , start = NA
    , end = NA
    , country_code_cols = "tweet_geo_country_code"
    , known_user_col = "known_original"
    ) {
  `%>%` <- magrittr::`%>%`
  # renaming the topic column
  df <- df %>% dplyr::rename(col_topic = topic)
  # filtering by country codes
  df <- (
    if(length(country_codes) == 0) dplyr::filter(df, col_topic == topic) 
    else if(length(country_code_cols) == 1) dplyr::filter(df, col_topic == topic & (!!as.symbol(country_code_cols[[1]])) %in% country_codes) 
    else if(length(country_code_cols) == 2) dplyr::filter(df, col_topic == topic & (!!as.symbol(country_code_cols[[1]])) %in% country_codes | (!!as.symbol(country_code_cols[[2]])) %in% country_codes)
    else stop("get geo count does not support more than two country code columns") 
  )
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
      dplyr::summarise(count = sum(tweets), known_users = sum((!!as.symbol(known_user_col)))) %>% 
      dplyr::ungroup()
  } else {
    data.frame(reporting_date=as.Date(character()),count=numeric(), known_users=numeric(), stringsAsFactors=FALSE)   
  }
}

# Calculating alerts for a particular period
calculate_alerts <- function(
    df = get_aggregates("country_counts")
    , topic
    , country_codes = list()
    , country_code_cols = "tweet_geo_country_code"
    , known_user_col = "known_original"
    , start = NA
    , end = NA
    , alpha = 0.025
    , no_historic = 7
    , bonferroni_m = 1
  ) {
  `%>%` <- magrittr::`%>%`
  # Getting counts from no_historic + 1 days bufore start if available 
  counts <- get_geo_counts(df, topic, country_codes, start - (no_historic + 1) , end, country_code_cols)
  if(nrow(counts) > 0) {
    #filling missing values with zeros if any
    date_range <- min(counts$reporting_date):max(counts$reporting_date)
    missing_dates <- date_range[sapply(date_range, function(d) !(d %in% counts$reporting_date))]
    if(length(missing_dates > 0)) {
      missing_dates <- data.frame(reporting_date = missing_dates, count = 0)
      counts <- dplyr::bind_rows(counts, missing_dates) %>% dplyr::arrange(reporting_date) 
    }
 
    #Calculating alerts
    alerts <- ears_t(counts$count, alpha=alpha/bonferroni_m, no_historic= no_historic)
    counts$alert <- alerts$alarm0
    counts$limit <- alerts$U0
    if(is.na(start))
      counts
    else
      counts %>% dplyr::filter(reporting_date >= start)
  } else {
    counts$alert <- logical()
    counts$limit <- numeric()
    counts
  }
}

#' Calculating alerts for an specific period
#'
#' @param df 
#' @param s_topic 
#' @param date 
#' @param geo_country_code 
#' @param s_country 
#' @param date_min 
#' @param date_max 
#' @export
calculate_tweet_alerts <- function(
    df
    , topic
    , countries = c(1)
    , date_type = c("created_date")
    , date_min = as.Date("1900-01-01")
    , date_max = as.Date("2100-01-01")
    , with_retweets = FALSE
    , location_type = "tweet" 
    , alpha = 0.025
    , no_historic = 7 
    , bonferroni_correction = FALSE
    )
{
  #Importing pipe operator
  `%>%` <- magrittr::`%>%` 

  # Getting regios details
  regions <- get_country_items()
  df$known_users <- df$known_original
  # Adding retwets if requested
  if(with_retweets){
    df$tweets <- ifelse(is.na(df$retweets), 0, df$retweets) + ifelse(is.na(df$tweets), 0, df$tweets)
    df$known_users <- df$known_retweets + df$known_original
  }
  #Setting world as region if no region is selected
  if(length(countries)==0) countries = c(1)

  series <- lapply(1:length(countries), function(i) {
    #message(paste("calculating for ", topic , regions[[countries[[i]]]]$name, i))
    bonferroni_m <- 
      if(bonferroni_correction) {
        length(regions[unlist(lapply(regions, function(r) r$level == regions[[ countries[[i]] ]]$level))])
      } else 1
    alerts <- 
      calculate_alerts(
	      df = df,
        topic = topic, 
        country_codes = regions[[countries[[i]]]]$codes,
	      country_code_cols = if(location_type == "tweet") "tweet_geo_country_code" else if(location_type == "user") "user_geo_country_code" else c("tweet_geo_country_code", "user_geo_country_code"),
        known_user_col = "known_users",
        start = as.Date(date_min), 
        end = as.Date(date_max), 
        no_historic=no_historic, 
        alpha=alpha,
        bonferroni_m = bonferroni_m
      )
    alerts <- dplyr::rename(alerts, date = reporting_date) 
    alerts <- dplyr::rename(alerts, number_of_tweets = count) 
    alerts$date <- as.Date(alerts$date, origin = '1970-01-01')
    if(nrow(alerts)>0) alerts$country <- regions[[countries[[i]]]]$name
    alerts$known_ratio = alerts$known_users / alerts$number_of_tweets
    alerts
  })

  df <- Reduce(x = series, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
  if(date_type =="created_weeknum"){
    df <- df %>% 
      dplyr::group_by(week = strftime(date, "%G%V"), topic, country) %>% 
      dplyr::summarise(c = sum(number_of_tweets), a = max(alert), d = min(date), ku = sum(known_users), kr = sum(known_users)/sum(number_of_tweets), l = 0) %>% 
      dplyr::ungroup() %>% 
      dplyr::select(d , c, ku, a, l, topic, country, kr) %>% 
      dplyr::rename(date = d, number_of_tweets = c, alert = a, known_users = ku, known_ratio = kr, limit = l)
  }
  return(df)
  
}


#' Get JSON file name for alert on given date
get_alert_file <- function(date) {
  alert_folder <- file.path(conf$data_dir, "alerts")
  if(!file.exists(alert_folder)) dir.create(alert_folder)
  alert_folder <- file.path(alert_folder, strftime(date, format="%Y"))
  if(!file.exists(alert_folder)) dir.create(alert_folder)
  alert_file <- file.path(alert_folder, paste(strftime(date, format="%Y.%m.%d"), "-alerts.json", sep = ""))
}


#' getting paramerters for current alert generation
#' This will be based on last successfully aggregated date and it will only be generated if once each scheduling span
do_next_alerts <- function(tasks = get_tasks()) {
  `%>%` <- magrittr::`%>%`
  # Getting period for last alerts
  last_agg <- get_aggregated_period("country_counts")
  alert_to <- last_agg$last
  alert_to_hour <- last_agg$last_hour
  alert_from <- alert_to - (as.numeric(conf$alert_history) + 2)

  # Determining whether we should produce alerts for current hour (if aggregation has produced new records since last alert generation
  do_alerts <- FALSE
  alert_file <-get_alert_file(alert_to)
  if(!file.exists(alert_file)) 
    do_alerts <- TRUE
  else {
    f <- file(alert_file, "rb")
    existing_alerts <- jsonlite::stream_in(f)
    close(f)
    if(nrow(existing_alerts)==0 || nrow(existing_alerts %>% dplyr::filter(date == alert_to & hour == alert_to_hour)) == 0)
      do_alerts <- TRUE
  }
  if(do_alerts) {
    # Getting region details
    regions <- get_country_items()
    #Getting counts for alert period
    counts <- get_aggregates(dataset = "country_counts", filter = list(period = list(alert_from, alert_to)))
    #Using parallel package to calculate alerts on all available cores
    #cl <- parallel::makePSOCKcluster(as.numeric(conf$spark_cores), outfile="")
    #conf <- conf
    topics <- unique(lapply(conf$topics, function(t) t$topic))
    #parallel::clusterExport(cl, list("conf", "topics", "counts", "regions", "alert_to", "calculate_tweet_alerts", "get_country_items", "get_country_items", "calculate_alerts"), envir=environment())
    #alerts <- parallel::parLapply(cl, topics, function(topic) {
    
    # calculating aletrts per topic
    alerts <- lapply(topics, function(topic) {
      m <- paste("Getting alerts for",topic, alert_to) 
      message(m)  
      tasks <- update_alerts_task(tasks, "running", m , start = TRUE)
      calculate_tweet_alerts(
        df = counts,
        topic = topic,
        countries = 1:length(regions), 
        date_type = "created_date", 
        date_min = alert_to, 
        date_max = alert_to, 
        with_retweets = FALSE, 
        location_type = "tweet" , 
        alpha = as.numeric(conf$alert_alpha), 
        no_historic = as.numeric(conf$alert_history), 
        bonferroni_correction = TRUE
      ) %>%
      dplyr::mutate(topic = topic) %>% 
      dplyr::filter(!is.na(alert) & alert == 1)
    })
    #parallel::stopCluster(cl)
    
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
        with_retweets = FALSE, 
        alpha = as.numeric(conf$alert_alpha), 
        no_historic = as.numeric(conf$alert_history),
        bonferroni_correction = TRUE
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


#' Getting calculated alerts for a defined period
#' @export
get_alerts <- function(topic=character(), countries=numeric(), from="1900-01-01", until="2100-01-01") {
  `%>%` <- magrittr::`%>%`
  # preparing filers dealing with possible names collitions with dataframe
  regions <- get_country_items()
  t <- topic
  c <- ( 
    if(class(countries)=="numeric")
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
    df %>% dplyr::filter(
      (if(length(c)==0) TRUE else country %in% c) &
      (if(length(t)==0) TRUE else topic %in% t)
    ) %>%
     dplyr::arrange(topic, hour) %>%
     dplyr::group_by(topic) %>%
     dplyr::mutate(rank = rank(hour, ties.method = "first")) %>%
     dplyr::ungroup()
  })
  Reduce(x = alerts, f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
}

#' get the path for default or user defined subscribed user file
get_subscribers_path <- function() {
  path <- paste(conf$data_dir, "subscribers.xlsx", sep = "/")
  if(!file.exists(path))
    path <- system.file("extdata", "subscribers.xlsx", package = get_package_name())
  path
}

#' get the default or user defined subscribed user list
get_subscribers <-function() {
  readxl::read_excel(get_subscribers_path())  
}

send_alert_emails <- function(tasks = get_tasks()) {
  `%>%` <- magrittr::`%>%`
  task <- 
  # Creating sending email statistics if any
  if(!exists("sent", tasks$alerts)) tasks$alerts$sent <- list()
  # Getting subscriber users
  subscribers <- get_subscribers()
  if(nrow(subscribers)>0) {
    for(i in 1:nrow(subscribers)) {
      user <- subscribers$User[[i]]
      topics <- strsplit(subscribers$Topics[[i]],";")[[1]]
      dest <- strsplit(subscribers$Email[[i]],";")[[1]]
      regions <- strsplit(subscribers$Regions[[i]],";")[[1]]
      excluded <- strsplit(subscribers$`Excluded Topics`[[i]], ";")[[1]]         
      realtime <- strsplit(subscribers$`Real time Topics`[[i]], ";")[[1]]         
      slots <- as.integer(strsplit(subscribers$`Alert Slots`[[i]], ";")[[1]])
      # Adding users statistics if does not existd already
      if(!exists(user, where=tasks$alerts$sent)) 
        tasks$alerts$sent[[user]] <- list()
      # Getting last day alerts date period
      agg_period <- get_aggregated_period("country_counts")
      alert_date <- agg_period$last
      alert_hour <- agg_period$last_hour

      # Getting last day alerts for subscribed user and regions
      user_alerts <- get_alerts(
        topic = if(all(is.na(topics))) NULL else topics,
        countries = if(all(is.na(regions))) NULL else regions,
        from = alert_date,
        until = alert_date
      )

      if(!is.null(user_alerts)) {
        # Excluding alerts for topics ignored for this user
        user_alerts <- (
          if(!all(is.na(excluded)))
            user_alerts %>% dplyr::filter(!(topic %in% exluded))
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
        # Instant alerts are those
        instant_alerts <- user_alerts %>% dplyr::filter(!is.na(realtime) & topics %in% realtime)
        
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
          (is.na(realtime) | !(topics %in% realtime))
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

        user_alerts <- dplyr::union_all(slot_alerts, instant_alerts) 
        # Sending alert email & registering last sent dates and hour to user
        if(nrow(user_alerts) > 0) {
          tasks <- update_alerts_task(tasks, paste("sending alert to", dest), "processing")
          message(paste("Sending alert to ", dest))
          msg <- ( 
            emayili::envelope() %>% 
            emayili::from(conf$smtp_from) %>% 
            emayili::to(dest) %>% 
            emayili::subject(paste("[Epitweetr] detected ", nrow(user_alerts), "signals")) %>% 
            emayili::html(print(xtable::xtable(user_alerts), type="html", file=tempfile()))
          )
          smtp <- emayili::server(host = conf$smtp_host, port=conf$smtp_port, username=conf$smtp_login, insecure=conf$smtp_insecure, password=conf$smtp_password, reuse = FALSE)
          smtp(msg)
          
          # Storing last day sendung alerts for current user
          tasks$alerts$sent[[user]]$date <- alert_date
          if(nrow(slot_alerts)>0) {
            tasks$alerts$sent[[user]]$hour_slot <- alert_hour
            tasks$alerts$sent[[user]]$last_slot <- current_slot
          }
          if(nrow(instant_alerts)>0)
            tasks$alerts$sent[[user]]$hour_instant <- alert_hour

        }
      }
    } 
  }
  tasks
}

