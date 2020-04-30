#' Run the search loop runner
#' @export
register_search_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for search without configuration setup")) 
  register_runner("search")
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
      if(.Platform$OS.type == "windows") system(paste(
        'tasklist /nh /fi "pid eq ',last_pid,'" | find /i "R" >nul && (
         exit 1
        ) || (
         exit 0
        )')) == 0 
      else if(.Platform$OS.type == "mac") system(paste("ps -cax | grep R | grep ", last_pid), ignore.stdout = TRUE)==0 
      else system(paste("ps -cax | grep R | grep ", last_pid), ignore.stdout = TRUE)==0
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

# Getting the scheduler task lists with current statusi updated.
get_tasks <-function(statuses = list()) {
  stop_if_no_config()
  tasks_path <- paste(conf$data_dir, "tasks.json", sep = "/")
  tasks <- if(file.exists(tasks_path)) {
    #if tasks files exists getting tasks from disk
    t <- jsonlite::fromJSON(tasks_path, simplifyVector = FALSE, auto_unbox = TRUE)
    # casting date time values
    for(i in 1:length(t)) {
      t[[i]]$scheduled_for <- if(class(t[[i]]$scheduled_for) == "numeric")  as.POSIXlt(t[[i]]$scheduled_for/1000,  origin="1970-01-01") else NA
      t[[i]]$last_start <- if(class(t[[i]]$last_start) == "numeric")  as.POSIXlt(t[[i]]$last_start/1000,  origin="1970-01-01") else NA
      t[[i]]$last_end <- if(class(t[[i]]$last_end) == "numeric")  as.POSIXlt(t[[i]]$last_end/1000,  origin="1970-01-01") else NA
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
      last_start = NA,
      last_end = NA,
      status = NA,
      scheduled_for = NA
    )
    #get geonames
    ret$languages <- list(
      task = "languages",
      order = 2,
      last_start = NA,
      last_end = NA,
      status = NA,
      scheduled_for = NA
    )
    #geo tag
    ret$geotag <- list(
      task = "geotag",
      order = 3,
      last_start = NA,
      last_end = NA,
      status = NA,
      scheduled_for = NA
    )
    #aggregate
    ret$aggregate <- list(
      task = "aggregate",
      order = 4,
      last_start = NA,
      last_end = NA,
      status = NA,
      scheduled_for = NA
    )
    #alerts
    ret$alerts <- list(
      task = "alerts",
      order = 5,
      last_start = NA,
      last_end = NA,
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
    && ( is.na(tasks$languages$last_start)
       || is.na(tasks$languages$last_end)
       || strptime(update_times[c], "%Y-%m-%d %H:%M:%S") > tasks$languages$last_start 
       || tasks$languages$last_start > tasks$languages$last_end
    ))]
  done <- lcodes[sapply(lcodes, function(c) (c %in% task_codes) && (c %in% conf_codes) 
    && (strptime(update_times[c], "%Y-%m-%d %H:%M:%S") <= tasks$languages$last_start
       || tasks$languages$last_start <= tasks$languages$last_end
    ))]

  tasks$languages$codes = as.list(c(to_remove, to_add, to_update, done))
  tasks$languages$statuses = as.list(c(
    sapply(to_remove, function(l) "to remove"), 
    sapply(to_add, function(l) "to add"), 
    sapply(to_update, function(l) "to update"), 
    sapply(to_remove, function(l) "to remove") 
  ))
  tasks$languages$urls = lapply(tasks$languages$codes, function(c) lurls[c])
  tasks$languages$names = lapply(tasks$languages$codes, function(c) lnames[c])
  tasks$languages$status <- (
    if(length(to_remove) + length(to_add) + length(to_update) > 0) 
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
# If tasks file exits it will read it, or get default taks otherwise
plan_tasks <-function(statuses = list()) {
  tasks <- get_tasks(statuses)
  # Updating next execution time for each task
  now <- Sys.time()
  sorted_tasks <- order(sapply(tasks, function(l) l$order))
  for(i in sorted_tasks) {
    # Scheduling pending tasks
    if(!is.na(tasks[[i]]$status) && tasks[[i]]$status %in% c("pending", "failed")) {
      tasks[[i]]$status <- "scheduled"
      if(tasks[[i]]$task %in% c("geonames", "languages")) {
        tasks[[i]]$scheduled_for <- now + (i - 1)/1000
      } else if (tasks[[i]]$task %in% c("geotag", "aggregate", "alerts")) {
          if(is.na(tasks[[i]]$last_end)) { 
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
#' Included tasks are 
#' If tasks file exits it will read it, or get default taks otherwise
#' @export
scheduler_loop <-function() {
  setup_config(data_dir = conf$data_dir)
  while(TRUE) {
    # getting tasks to execute 
    tasks <- get_tasks()
    i_next <- order(sapply(tasks, function(t) if(t$status == "scheduled") t$scheduled_for else Sys.time()+3600+order))[[1]]
    
    save_tasks(tasks)

    setup_config(data_dir = conf$data_dir)
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
        && (is.na(task$last_end) || task$last_end < strptime(conf$geonames_updated_on, "%Y-%m-%d %H:%M:%S"))
      ) || (
       task$task == "languages" 
        && !is.na(conf$lang_updated_on)
        && (is.na(task$last_end) || task$last_end < strptime(conf$lang_updated_on, "%Y-%m-%d %H:%M:%S"))
      )  
    )
  )  
}
