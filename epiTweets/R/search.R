
#' Search for all tweets on topics defined on configuration
#' @export
search_loop <- function() {
  while(TRUE) {
    #Calculating 'get plans' for incompleted imports that has not been able to finish after schedule span minutes
    for(i in 1:length(conf$topics)) {
      conf$topics[[i]]$plan <- update_plans(plans = conf$topics[[i]]$plan, schedule_span = conf$schedule_span)
    }
     
    #waiting until next schedule if not pending imports to do
    wait_for <- min(unlist(lapply(1:length(conf$topics), function(i) can_wait_for(plans = conf$topics[[i]]$plan))) )
    if(wait_for > 0) {
      message(paste("All done! going to sleep for", wait_for, "seconds"))
      Sys.sleep(wait_for)
    }
    #getting ony the next plan to execute for each topic (it could be a previous unfinished plan
    next_plans <-lapply(1:length(conf$topics), function(i)  next_plan(plans = conf$topics[[i]]$plan))

    #calculating ranks based on number of requets. Topics with less completed requets wil get rank = 1
    min_requests <- min(unlist(lapply(1:length(conf$topics), function(i) if(!is.null(next_plans[[i]])) next_plans[[i]]$request else .Machine$integer.max)))
    
    #performing search for each topic with ranking = 1
    for(i in 1:length(conf$topics)) { 
      for(j in 1:length(conf$topics[[i]]$plan)) { 
        plan <- conf$topics[[i]]$plan[[j]]
        if(plan$requests == min_requests) {
            conf$topics[[i]]$plan[[j]] = search_topic(plan = plan, query = conf$topics[[i]]$query, topic = conf$topics[[i]]$topic) 
        }
      }
    }
    save_config(path = paste(conf$dataDir, "conf.json", sep = "/"))
  }
}

#' performing a search for a topic and updating current plan
search_topic <- function(plan, query, topic) {
  message(paste("searching for topic", topic, "since", plan$since_id, "until", plan$max_id))
  month <- format(Sys.time(), "%Y.%m")
  file_name <- paste(format(Sys.time(), "%Y.%m.%d.%H"), "json", sep = ".")
  dest <- paste(conf$dataDir, "tweets", "search", topic, month, file_name, sep = "/")
  create_dirs(topic, month) 
  if(nchar(query)< 400) {
    resp <- twitter_search(q = query, max_id = plan$since_id, since_id = plan$since_target)
    content = httr::content(resp,as="text")
    json <- jsonlite::fromJSON(content)
    write(content, dest, append=TRUE)
    reach_max <- is.data.frame(json$statuses) && nrow(json$statuses) == 100
    new_since_id = 
      if(!is.data.frame(json$statuses)) {bit64::as.integer64(json$search_metadata$since_id_str)  
      } else {Reduce(function(x, y) if(x < y) x else y, lapply(json$statuses$id_str, function(x) bit64::as.integer64(x) -1 ))}
    request_finished(plan, less_than_count =!reach_max, max_id = json$search_metadata$max_id_str, since_id = new_since_id) 
  } else {
    warning(paste("Query too long for API for topic", topic), immediate. = TRUE) 
    plan$requests = plan$requests
    plan 
  }
}


#' Perform a single page search on twitter API
twitter_search <- function(q, since_id = NULL, max_id = NULL, result_type = "recent", count = 100) {
  search_url <- 
    paste(
      search_endopoint
      , "?q="
      , gsub("\\(", "%28", gsub("\\)", "%29", gsub("!", "%21", gsub("\\*", "%2A", gsub("'", "%27", xml2::url_escape(q))))))
      , if(!is.null(since_id)) "&since_id=" else ""
      , if(!is.null(since_id)) since_id else ""
      , if(!is.null(max_id)) "&max_id=" else  ""
      , if(!is.null(max_id)) max_id else  ""
      , "&result_type="
      , result_type
      , "&count="
      , count
      , "&include_entities=true"
      , sep = ""
    )
  res <- twitter_get(search_url)
  return(res)
}


#' Create a new get plan for importing tweets using the search api
get_plan <- function(
  expected_end
  , scheduled_for = Sys.time()
  , start_on = NULL
  , end_on = NULL
  , max_id = NULL
  , since_id = NULL
  , since_target = NULL
  , results_span = 0
  , requests = 0
  , progress = 0.0 
  ) {
  me <- list(
    "expected_end" = if(!is.null(unlist(expected_end))) strptime(unlist(expected_end), "%Y-%m-%d %H:%M:%S") else NULL
    , "scheduled_for" = if(!is.null(unlist(scheduled_for))) strptime(unlist(scheduled_for), "%Y-%m-%d %H:%M:%S") else NULL
    , "start_on" =if(!is.null(unlist(start_on))) strptime(unlist(start_on), "%Y-%m-%d %H:%M:%S") else NULL 
    , "end_on" = if(!is.null(unlist(end_on))) strptime(unlist(end_on), "%Y-%m-%d %H:%M:%S") else NULL
    , "max_id" = if(!is.null(unlist(max_id))) bit64::as.integer64(unlist(max_id)) else  NULL
    , "since_id" = if(!is.null(unlist(since_id))) bit64::as.integer64(unlist(since_id)) else NULL
    , "since_target" = if(!is.null(unlist(since_target))) bit64::as.integer64(unlist(since_target)) else NULL
    , "requests" = unlist(requests)
    , "progress" = unlist(progress)
  )
  class(me) <- append(class(me), "get_plan")
  return(me) 
}

#' Calculate new 'get plans' for a particular topic
#' If no plans are set, a new plan for getting all possible tweets will be set
#' If current plan has started and the expected end has passed, a new plan will be added for collecting new tweets (previous plan will be stored for future execution if possible)
#' Any finished plans after the first will be discharged (note that after 7 days all should be discharged because of empty results as a measure of precaution  a max od 100 plans are stored)
update_plans <- function(plans = list(), schedule_span) {
  if(length(plans) == 0) {
     return(list(get_plan(expected_end = Sys.time() + 60 * schedule_span)))
  } else if(plans[[1]]$requests > 0 && plans[[1]]$expected_end < Sys.time()) {
    first <-  
      get_plan(
        expected_end = if(Sys.time()>plans[[1]]$expected_end + 60 * schedule_span) Sys.time() + 60 * schedule_span else plans[[1]]$expected_end + 60*schedule_span
        , since_target = plans[[1]]$max_id + 1
      )
    non_ended <- plans[sapply(plans, function(x) is.null(x$end_on))]
    return (append(list(first), if(length(non_ended)<100) non_ended else non_ended[1:100]))
  } else {
    first <- plans[[1]]
    rest <- plans[-1]
    non_ended <- rest[unlist(sapply(rest, function(x) is.null(x$end_on)))]
    return (append(list(first), if(length(non_ended)<100) non_ended else non_ended[1:100]))
  }
}

#' Get next plan to plan to download
next_plan <- function(plans) {
  plans <- if("get_plan" %in% class(plans)) list(plans) else plans
  non_ended <- plans[sapply(plans, function(x) is.null(x$end_on) && x$scheduled_for < Sys.time())]
  if(length(non_ended) == 0) {
     return(NULL)
  } else {
    return(non_ended[[1]])
  }
}

#' Get next plan to plan to download
can_wait_for <- function(plans) {
  plans <- if("get_plan" %in% class(plans)) list(plans) else plans
  non_ended <- plans[sapply(plans, function(x) is.null(x$end_on))]
  if(length(non_ended) == 0) {
     #warning("No plans are to be executed.... possible scheduler error", immediate. = TRUE)
     return(10*60)
  } else {
    return(ceiling(as.numeric(difftime(non_ended[[length(non_ended)]]$scheduled_for, Sys.time(), units = "secs"))))
  }
}


#' next plan generic function
request_finished <- function(current, less_than_count, max_id, since_id = NULL) {
  UseMethod("request_finished",current)
}

#' Update a plan after search request is done
#' If first request, started and max will be set
#' If results are non empty result span, since_id and max id are set
#' If results are less than requested the search is supossed to be finished
#' If results are equals to the requested limit, more tweets are expected. In that case if the expected end has not yet arrived and we can estimate the remaining number of requests the next schedule will be set to an estimation of the necessary requests to finish. If we do not know, the current schedule will be left untouched.
request_finished.get_plan <- function(current, less_than_count, max_id, since_id = NULL) {
  current$requests <- current$requests + 1 
  if(is.null(current$start_on)) {
    current$start_on = Sys.time()
    current$max_id = bit64::as.integer64(max_id)
  }
  if(!is.null(since_id)) {
    current$since_id <- bit64::as.integer64(since_id)
  }
  if(!is.null(current$since_target) && !is.null(current$since_id) && !is.null(current$max_id)) {
    current$progress = as.double(current$max_id - current$since_id)/as.double(current$max_id - current$since_target)
  } 
  if(less_than_count || (!is.null(current$since_target) && current$max_id == current$since_target)) {
    current$end_on <- Sys.time()
    current$progress <- 1.0 
  } else {
    if(Sys.time() < current$expected_end && current$progress > 0.0) {
      progressByRequest <- current$progress / current$requests
      requestsToFinish <- (1.0 - current$progress)/progressByRequest
      current$scheduled_for = Sys.time() + as.integer(difftime(current$expected_end, Sys.time(), units = "secs"))/requestsToFinish 
    }
  }
  return(current)
}

#' create topic directories if they do not exists
create_dirs <- function(topic, month) {
  if(!file.exists(paste(conf$dataDir, sep = "/"))){
    dir.create(paste(conf$dataDir, sep = "/"), showWarnings = FALSE)
  }  
  if(!file.exists(paste(conf$dataDir, "tweets", sep = "/"))){
    dir.create(paste(conf$dataDir, "tweets", sep = "/"), showWarnings = FALSE)
  }  
  if(!file.exists(paste(conf$dataDir, "tweets", "search", topic, sep = "/"))){
    dir.create(paste(conf$dataDir, "tweets", "search", topic,  sep = "/"), showWarnings = FALSE)
  }  
  if(!file.exists(paste(conf$dataDir, "tweets", "search", topic, month , sep = "/"))){
    dir.create(paste(conf$dataDir, "tweets", "search", topic, month, sep = "/"), showWarnings = FALSE)
  }  
}
