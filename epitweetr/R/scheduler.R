#' Registers the search runner for the current process
#' @export
register_search_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for search without configuration setup")) 
  register_runner("search")
}


#' Registers the detect runner for the current process
#' @export
register_detect_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for detect without configuration setup")) 
  register_runner("detect")
}

#' Register search runner task and start it
#' @export
register_search_runner_task <- function() {
  register_runner_task("search")
}
#' Register detect runner task and start it
#' @export
register_detect_runner_task <- function() {
  register_runner_task("detect")
}
#' Register task and start it
register_search_runner_task <- function(task_name) {
  stop_if_no_config(paste("Cannot register scheduled task without configuration setup")) 
  #rscript <- file.path(R.home("bin"), "Rscript")
  
  script_name = paste(task_name, "R", sep = ".")

  if(.Platform$OS.type == "windows") {
    if(requireNamespace("taskscheduleR", quietly = TRUE)) {
      #Command with current data dir and copy it to user folder
      script_base <- system.file("extdata", script_name, package = get_package_name())
      script_folder <- paste(gsub("\\\\", "/", Sys.getenv("USERPROFILE")), "epitweetr", sep = "/")
      if(!file.exists(script_folder)){
        dir.create(script_folder, showWarnings = FALSE)
      }  
      script <- paste(script_folder, script_name, sep = "/")
      file.copy(from = script_base, to = script, overwrite = TRUE)

      taskscheduleR::taskscheduler_create(
        taskname = paste("epitweetr", task_name, "loop", sep = "_")
        , rscript = script
        , schedule = "HOUR"
        , rscript_args = conf$data_dir
        , schtasks_extra="/F"
      )
    }
    else stop("Please install taskscheduler Package")
  } else {
     Stop("Not implemetef yet or this OS")
  }
		   
}

#' Get search runner execution status
#' @export
is_search_running <- function() {
  stop_if_no_config(paste("Cannot check running status for search without configuration setup")) 
  get_running_task_pid("search")>=0
}

#' Get search runner execution status
#' @export
is_detect_running <- function() {
  stop_if_no_config(paste("Cannot check detect batch status for search without configuration setup")) 
  get_running_task_pid("detect")>=0
}

#' Get runner active PID
get_running_task_pid <- function(name) {
  pid_path <- paste(conf$data_dir, "/",name, ".PID", sep = "")
  if(file.exists(pid_path)) {
    # Getting last runner pid
    last_pid <- as.integer(readChar(pid_path, file.info(pid_path)$size))
      # Checking if last_pid is still running
    pid_running <- ( 
      if(.Platform$OS.type == "windows") {
        length(system(paste('tasklist /nh /fi "pid eq ',last_pid,'"'), intern = TRUE)) > 1
      }
      else if(.Platform$OS.type == "mac") 
        system(paste("ps -cax | grep R | grep ", last_pid), ignore.stdout = TRUE)==0 
      else 
        system(paste("ps -cax | grep R | grep ", last_pid), ignore.stdout = TRUE)==0
    )
    if(pid_running)
      last_pid
    else
      -1
  }
  else -1
}


#' Register as the scheduler runner if there is no other scheduler running or quit otherwise
register_runner <- function(name) {
  stop_if_no_config(paste("Cannot register ", name ," without configuration setup")) 
  # getting current 
  pid <- Sys.getpid()
  last_pid <- get_running_task_pid(name)
  if(last_pid >= 0 && last_pid != pid) {
     stop(paste("Runner ", name ,"PID:", last_pid, "is already running"))
  }
  # If here means that the current process has to register as the runner service
  pid_path <- paste(conf$data_dir, "/",name, ".PID", sep = "")
  write(pid, file = pid_path, append = FALSE)
}

# Getting the scheduler task lists with current status updated.
get_tasks <- function(statuses = list()) {
  stop_if_no_config()
  tasks_path <- paste(conf$data_dir, "tasks.json", sep = "/")
  tasks <- if(file.exists(tasks_path)) {
    #if tasks files exists getting tasks from disk
    t <- jsonlite::fromJSON(tasks_path, simplifyVector = FALSE, auto_unbox = TRUE)
    # casting date time values
    for(i in 1:length(t)) {
      t[[i]]$scheduled_for <- if(class(t[[i]]$scheduled_for) == "numeric")  as.POSIXlt(t[[i]]$scheduled_for/1000,  origin="1970-01-01") else NA
      t[[i]]$started_on <- if(class(t[[i]]$started_on) == "numeric")  as.POSIXlt(t[[i]]$started_on/1000,  origin="1970-01-01") else NA
      t[[i]]$end_on <- if(class(t[[i]]$end_on) == "numeric")  as.POSIXlt(t[[i]]$end_on/1000,  origin="1970-01-01") else NA
      t[[i]]$status <- if(class(t[[i]]$status) == "character")  t[[i]]$status else NA
    }
    t
  } else {
    # If no tasks file is setup a default is built based on configuration
    ret <- list()
    #get geonames
    ret$geonames <- list(
      task = "geonames",
      order = 1,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
    #get geonames
    ret$languages <- list(
      task = "languages",
      order = 2,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
    #geo tag
    ret$geotag <- list(
      task = "geotag",
      order = 3,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
    #aggregate
    ret$aggregate <- list(
      task = "aggregate",
      order = 4,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
    #alerts
    ret$alerts <- list(
      task = "alerts",
      order = 5,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
    ret
  }
  # Activating one shot tasks with configuration changes
  if(in_pending_status(tasks$geonames)) {
     tasks$geonames$url = conf$geonames_url  
     tasks$geonames$status <- "pending" 
  }
    
  # Updating languages tasks
  langs <- get_available_languages()
  lcodes <- langs$Code
  lurls <- setNames(langs$Url, langs$Code)
  lnames <- setNames(langs$Label, langs$Code)
  conf_codes <- sapply(conf$languages, function(l) l$code)
  task_codes <- tasks$languages$codes
  update_times <- setNames(sapply(conf$languages, function(l) l$modified_on), sapply(conf$languages, function(l) l$code))
  
  to_remove <- lcodes[sapply(lcodes, function(c) c %in% task_codes && !(c %in% conf_codes))]
  to_add <- lcodes[sapply(lcodes, function(c) !(c %in% task_codes) && (c %in% conf_codes))]
  to_update <- lcodes[sapply(lcodes, function(c) (c %in% task_codes) && (c %in% conf_codes) 
    && ( is.na(tasks$languages$started_on)
       || is.na(tasks$languages$end_on)
       || (!is.null(update_times[[c]]) && strptime(update_times[[c]], "%Y-%m-%d %H:%M:%S") > tasks$languages$started_on)
       || tasks$languages$started_on > tasks$languages$end_on
    ))]
  done <- lcodes[sapply(lcodes, function(c) (c %in% task_codes) && (c %in% conf_codes) 
    && (!is.na(tasks$languages$started_on)
      && ! is.na(tasks$languages$end_on)
      && (is.null(update_times[[c]]) || strptime(update_times[c], "%Y-%m-%d %H:%M:%S") <= tasks$languages$started_on)
      && tasks$languages$started_on <= tasks$languages$end_on
    ))]

  tasks$languages$codes = as.list(c(to_remove, to_add, to_update, done))
  tasks$languages$statuses = as.list(c(
    sapply(to_remove, function(l) "to remove"), 
    sapply(to_add, function(l) "to add"), 
    sapply(to_update, function(l) "to update"), 
    sapply(done, function(l) "done") 
  ))
  tasks$languages$urls = lapply(tasks$languages$codes, function(c) lurls[c])
  tasks$languages$names = lapply(tasks$languages$codes, function(c) lnames[c])
  tasks$languages$vectors = lapply(tasks$languages$codes, function(c) {
    conf$tasks$languages = sapply(conf$languages[sapply(conf$languages, function(l) l$code == c)], function(l) l$vectors)  
  })
  tasks$languages$status <- (
    if((is.na(tasks$languages$status) || tasks$languages$status != "running") &&  length(to_remove) + length(to_add) + length(to_update) > 0) 
      "pending"
    else
      tasks$languages$status
  ) 
  if(length(statuses)==0)
    tasks
  else
    tasks[sapply(tasks, function(t) t$status %in% statuses)]
}

# Getting the scheduler task lists with scheduled_for times updates.
# If tasks file exits it will read it, or get default tasks otherwise
plan_tasks <-function(statuses = list()) {
  tasks <- get_tasks(statuses)
  # Updating next execution time for each task
  now <- Sys.time()
  sorted_tasks <- order(sapply(tasks, function(l) l$order))
  for(i in sorted_tasks) {
    # Scheduling pending tasks
    if(tasks[[i]]$task %in% c("geonames", "languages")) {
      if(!is.na(tasks[[i]]$status) && tasks[[i]]$status %in% c("pending", "failed", "running")) {
        tasks[[i]]$status <- "scheduled"
        tasks[[i]]$scheduled_for <- now + (i - 1)/1000
      }
    } else if (tasks[[i]]$task %in% c("geotag", "aggregate", "alerts")) { 
      if(is.na(tasks[[i]]$status) || tasks[[i]]$status %in% c("pending", "failed", "running")) {
        tasks[[i]]$status <- "scheduled"
        if(is.na(tasks[[i]]$end_on)) { 
          tasks[[i]]$scheduled_for <- now + (i - 1)/1000
        } else if(tasks[[i]]$last_excuted >= tasks[[i]]$scheduled_for) {
          # the last schedule was already executed a new schedule has to be set
          span <- ret$schedule_span
          start_hour <- ret$schedule_start_hour
          per_day <- as.integer(24 * 60/span)
          # calculating possible next slot for
          hour_slots <- sort((c(0:(per_day-1)) * (span/60) + start_hour) %% 24)
          hour_slots <- c(hour_slots, hour_slots[[1]] + 24) #This adding first slot of next day
          day_slots <- hour_slots * 60 * 60 + (as.POSIXlt(as.Date(tasks[[i]]$scheduled_for)))
          next_slot <- day_slots[day_slots > tasks[[i]]$scheduled_for][[1]]
          #if next slot is in future set it. If it is in past, set now
          tasks[[i]]$scheduled_for <- if(next_slot < now) next_slot else now + (i - 1)/1000
        } 
      }
    }  
  }
  save_tasks(tasks)
  if(length(statuses)==0)
    tasks
  else
    tasks[sapply(tasks, function(t) t$status %in% statuses)]
}

#' saves the provided task lists to the data directory.
#'
#' @export
save_tasks <- function(tasks) {
  stop_if_no_config()
  tasks_path <- paste(conf$data_dir, "tasks.json", sep = "/")
  jsonlite::write_json(tasks, tasks_path, pretty = TRUE, force = TRUE, auto_unbox = TRUE, POSIXt="epoch")
}

#' Infinite looop executing tasks respecting the order and time scheduling window.
#' Included tasks are update geonames, update vectors, geotag, aggregate and alert detection 
#' If tasks file exits it will read it, or get default task otherwise
#' @export
detect_loop <-function(data_dir = paste(getwd(), "data", sep = "/")) {
  setup_config(data_dir = data_dir)
  while(TRUE) {
    # getting tasks to execute 
    tasks <- plan_tasks()
    i_next <- order(sapply(tasks, function(t) if(!is.na(t$status) && t$status %in% c("scheduled", "failed")) as.numeric(t$scheduled_for) else as.numeric(Sys.time())+3600+t$order))[[1]] 
    if(tasks[[i_next]]$status %in% c("failed", "running") && (is.na(tasks[[i_next]]$status) || tasks[[i_next]]$failures > 3)) {
      tasks[[i_next]]$status = "aborted"
      tasks[[i_next]]$message = "Cannot continue mas number of retries reached"
      save_tasks(tasks)
      stop("Cannot continue mas number of retries reached")
    }
    
    message(paste("Executing task", tasks[[i_next]]$task))
    if(tasks[[i_next]]$task == "geonames") {
      tasks <- update_geonames(tasks)  
    }
    else if(tasks[[i_next]]$task == "languages") {
      tasks <- update_languages(tasks)  
    }
    else if(tasks[[i_next]]$task == "geotag") {
      tasks <- geotag_tweets(tasks) 
    }
    else if(tasks[[i_next]]$task == "aggregate") {
      tasks <- aggregate_tweets(tasks = tasks) 
    }
    else if(tasks[[i_next]]$task == "alerts") {
      tasks <- generate_alerts(tasks)
    }

    save_tasks(tasks)
    setup_config(data_dir = conf$data_dir)
    # To remove
    Sys.sleep(1)
  }

}

#' Check if provided task is in pending status
in_pending_status <- function(task) {
  (
    (is.na(task$status) || task$status %in% c("pending", "success", "failure")) 
    &&
    (
      (task$task == "geonames" 
        && !is.na(conf$geonames_updated_on)
        && (is.na(task$end_on) || task$end_on < strptime(conf$geonames_updated_on, "%Y-%m-%d %H:%M:%S"))
      ) || (
       task$task == "languages" 
        && !is.na(conf$lang_updated_on)
        && (is.na(task$end_on) || task$end_on < strptime(conf$lang_updated_on, "%Y-%m-%d %H:%M:%S"))
      )  
    )
  )  
}



#' Updating geonames task for reporting progress on geonames refresh
update_geonames_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$geonames$started_on = Sys.time() 
  if(end) tasks$geonames$end_on = Sys.time() 
  tasks$geonames$status =  status
  tasks$geonames$message = message
  if(status == "failed") {
    if(exists("failures", where = tasks$geonames))
      tasks$geonames$failures = tasks$geonames$failures + 1
    else
      tasks$geonames$failures = 1
  }
  save_tasks(tasks)
  return(tasks)
}

#' Updating task for reporting progress on languages refresh
update_languages_task <- function(tasks, status, message, start = FALSE, end = FALSE, lang_code = character(0), lang_start = FALSE, lang_done = FALSE) {
  if(start) tasks$languages$started_on = Sys.time() 
  if(end) tasks$languages$end_on = Sys.time()
  for(i in 1:length(tasks$languages$code)) {
    if(tasks$languages$code[[i]] == lang_code && lang_start) tasks$languages$statuses[[i]] = "running"
    if(tasks$languages$code[[i]] == lang_code && lang_done) tasks$languages$statuses[[i]] = "done"
  }     
  tasks$languages$status =  status
  tasks$languages$message = message
  if(status == "failed") {
    if(exists("failures", where = tasks$languages))
      tasks$languages$failures = tasks$languages$failures + 1
    else
      tasks$languages$failures = 1
  } else if(status == "success")
      tasks$languages$failures = 0
    
  save_tasks(tasks)
  return(tasks)
}


#' Updating geotag task for reporting progress on geotagging
update_geotag_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$geotag$started_on = Sys.time() 
  if(end) tasks$geotag$end_on = Sys.time() 
  tasks$geotag$status =  status
  tasks$geotag$message = message
  if(status == "failed") {
    if(exists("failures", where = tasks$geotag))
      tasks$geotag$failures = tasks$geotag$failures + 1
    else
      tasks$geotag$failures = 1
  }
  save_tasks(tasks)
  return(tasks)

}

#' Updating aggregate task for reporting progress on aggregation
update_aggregate_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$aggregate$started_on = Sys.time() 
  if(end) tasks$aggregate$end_on = Sys.time() 
  tasks$aggregate$status =  status
  tasks$aggregatr$message = message
  if(status == "failed") {
    if(exists("failures", where = tasks$aggregate))
      tasks$aggregatr$failures = tasks$aggregate$failures + 1
    else
      tasks$aggregate$failures = 1
  }
  save_tasks(tasks)
  return(tasks)

}

#' Updating alert task for reporting progress on alert detection
update_alerts_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$alerts$started_on = Sys.time() 
  if(end) tasks$alerts$end_on = Sys.time() 
  tasks$alerts$status =  status
  tasks$alerts$message = message
  if(status == "failed") {
    if(exists("failures", where = tasks$alerts))
      tasks$alerts$failures = tasks$alerts$failures + 1
    else
      tasks$alerts$failures = 1
  }
  save_tasks(tasks)
  return(tasks)
}
