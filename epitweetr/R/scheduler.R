#' Run the search loop runner
#' @export
register_runner <- function(name) {
  setup_config()
  register_runner("search")
  search_loop() 
}


#' Register as the scheduler runner if there is no other scheduler running or quit otherwise
#' @export
register_runner <- function(name) {
  stop_if_no_config(paste("Cannot register ", name ," without configuration setup")) 
  # getting current 
  pid <- Sys.getpid()
  pid_path <- paste(conf$data_dir, name, ".PID", sep = "/")
  if(file.exists(pid_path)) {
    # Getting last runner pid
    last_pid <- as.integer(readChar(pid_path, file.info(pid_path)$size))
    if(last_pid != pid) {
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
        stop(paste("Runner ", name ,"PID:", last_pid, "is already running"))
    }
  }
  # If here means that the current process has to register as the runner service
  write(pid, file = pid_path, append = FALSE)
  
}
# Getting the scheduler task lists with scheduled_for times updates.
# If tasks file exits it will read it, or get default taks otherwise
get_tasks <-function() {
  stop_if_no_config()
  tasks_path <- paste(conf$data_dir, "tasks.json", sep = "/")
  tasks <- if(file.exists(tasks_path)) {
    #if tasks files exists getting tasks from disk
    t <- jsonlite::fromJSON(tasks_path, simplifyVector = FALSE, auto_unbox = TRUE)
    # casting date time values
    for(i in 1:length(t)) {
      t[[i]]$scheduled_for <- if(class(t[[i]]$scheduled_for) == "numeric")  as.POSIXlt(t[[i]]$scheduled_for/1000,  origin="1970-01-01") else NA
      t[[i]]$last_executed <- if(class(t[[i]]$last_executed) == "numeric")  as.POSIXlt(t[[i]]$last_executed/1000,  origin="1970-01-01") else NA
    }
    t
  } else {
    # If no tasks file is setup a default is built based on configuration
    ret <- list()
    #get geonames
    ret$geonames <- list(
      task = "geonames",
      order = 1,
      last_executed = NA,
      scheduled_for = NA,
      active = TRUE,
      url = "http://download.geonames.org/export/dump/allCountries.zip"  
    )
    #get geonames
    ret$languages <- list(
      task = "languages",
      order = 2,
      last_executed = NA,
      scheduled_for = NA,
      active = TRUE,
      codes = lapply(conf$languages, function(l) l$code), 
      add_remove = lapply(conf$languages, function(l) 1), 
      urls = {
        langs <- get_available_languages()
        lnames <- setNames(langs$Url, langs$Code)
        lapply(conf$languages, function(l) lnames[l$code])
      }
    )
    #geo tag
    ret$geotag <- list(
      task = "geotag",
      order = 3,
      last_executed = NA,
      scheduled_for = NA,
      active = TRUE 
    )
    #aggregate
    ret$aggregate <- list(
      task = "aggregate",
      order = 4,
      last_executed = NA,
      scheduled_for = NA,
      active = TRUE 
    )
    #alerts
    ret$alerts <- list(
      task = "alerts",
      order = 5,
      last_executed = NA,
      scheduled_for = NA,
      active = TRUE 
    )
    ret
  }
  # Updating next execution time for each task
  now <- Sys.time()
  sorted_tasks <- order(sapply(tasks, function(l) l$order))
  for(i in sorted_tasks) {
    # Setting default values if scheduled_for is not set or if scheduled for is the past
    if(tasks[[i]]$active && (is.na(tasks[[i]]$scheduled_for) || now >= tasks[[i]]$scheduled_for )) {
      if(tasks[[i]]$task %in% c("geonames", "languages")) {
        tasks[[i]]$scheduled_for <- now + (i - 1)/1000
      } else if (tasks[[i]]$task %in% c("geotag", "aggregate", "alerts")) {
          if(is.na(tasks[[i]]$last_executed)) { 
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
  jsonlite::write_json(tasks, tasks_path, pretty = TRUE, force = TRUE, auto_unbox = TRUE, POSIXt="epoch")
  tasks
}


