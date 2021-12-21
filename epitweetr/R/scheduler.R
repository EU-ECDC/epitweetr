# Registers the search runner (by writing search.PID file) for the current process 
# or stops if no configuration has been set or if it is already running
register_search_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for search without configuration setup")) 
  register_runner("search")
}


# Registers the detect runner (by writing detect.PID file) for the current process 
# or stops if no configuration has been set or if it is already running
register_detect_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for detect without configuration setup")) 
  register_runner("detect")
}

# Registers the detect runner (by writing detect.PID file) for the current process 
# or stops if no configuration has been set or if it is already running
register_fs_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for fs without configuration setup")) 
  register_runner("fs")
}

 
#' @title Registers the fs_monitor for the current process or exits
#' @description registers the fs_monitor (by writing detect.PID file) for the current process or stops if no configuration has been set or if it is already running
#' @return Nothing
#' @details Registers the fs_monitor (by writing detect.PID file) for the current process or stops if no configuration has been set or if it is already running
#' this function is exported so it can be called nicely from using the future package, but it is not intended to be directly used by users
#' @examples 
#' if(FALSE){
#'    #getting tasks statuses
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    register_fs_monitor()
#' }
#' @rdname register_fs_monitor
#' @export 
register_fs_monitor <- function() {
  stop_if_no_config(paste("Cannot check running status for fs monitor without configuration setup")) 
  register_runner("fs_mon")
}

# Registers the a task runner (by writing "name".PID file) for the current process 
# or stops if no configuration has been set or if it is already running
register_runner <- function(name) {
  stop_if_no_config(paste("Cannot register ", name ," without configuration setup")) 
  # getting current pid
  pid <- Sys.getpid()
  # getting last running PID
  last_pid <- get_running_task_pid(name)
  if(last_pid >= 0 && last_pid != pid) {
     stop(paste("Runner ", name ,"PID:", last_pid, "is already running"))
  }
  # If here means that the current process has to register as the runner service
  pid_path <- paste(conf$data_dir, "/",name, ".PID", sep = "")
  write(pid, file = pid_path, append = FALSE)
}

# Register search runner task and schedule the loop to run each hour
# These tasks are currently available in Windows only
register_search_runner_task <- function() {
  register_runner_task("search")
}

# Register detect runner task and schedule the loop to run each hour
# These tasks are currently available in Windows only
register_detect_runner_task <- function() {
  register_runner_task("detect")
}

# Register a task on system task scheduler to run each hour
# Register fd runner task and schedule the loop to run each hour
# These tasks are currently available in Windows only
register_fs_runner_task <- function() {
  register_runner_task("fs")
}

# This tasks is currently Windows only
register_runner_task <- function(task_name) {
  # Making sure configuration has been set
  stop_if_no_config(paste("Cannot register scheduled task without configuration setup")) 
  
  # Getting the embedded script name to be called in order to make the script run
  script_name <- paste(task_name, "R", sep = ".")

  # Filtering by OS, currently only Windows is supported
  if(.Platform$OS.type == "windows") {
    # testing if taskscheduleR can be loaded
    if(requireNamespace("taskscheduleR", quietly = TRUE)) {
      # Getting source script
      script_base <- system.file("extdata", script_name, package = get_package_name())
      # Getting destination folder which is going to be on user c:/Users/user/epitweetr
      script_folder <- paste(gsub("\\\\", "/", Sys.getenv("USERPROFILE")), "epitweetr", sep = "/")
      if(!file.exists(script_folder)){
        dir.create(script_folder, showWarnings = FALSE)
      }  
      script <- paste(script_folder, script_name, sep = "/")
      # Copying the script 
      file.copy(from = script_base, to = script, overwrite = TRUE)

      # Registering the task as a schedule task using taskscheduleR
      # Note that the date format is being guess using %DATE% shell variable
      # This value can be overridden by conf$fore_date_format setting
      taskscheduleR::taskscheduler_create(
        taskname = paste("epitweetr", task_name, "loop", sep = "_")
        , rscript = script
        , schedule = "HOUR"
        , rscript_args = paste("\"", conf$data_dir,"\"", sep = "")
        , startdate =  
          if(conf$force_date_format == "")
            tail(strsplit(shell("echo %DATE%", intern= TRUE), " ")[[1]], 1)
          else
            strftime(Sys.time(), conf$force_date_format)
        , schtasks_extra="/F"
      )
    }
    else warning("Please install taskscheduleR package")
  } else {
     warning("Task scheduling is not implemented yet on this OS. You can still schedule it manually. Please refer to package vignette.")
  }
		   
}

# Get search runner execution status
is_search_running <- function() {
  stop_if_no_config(paste("Cannot check running status for search without configuration setup")) 
  get_running_task_pid("search")>=0
}

# Get search runner execution status
is_fs_running <- function() {
  stop_if_no_config(paste("Cannot check running status for fs without configuration setup")) 
  tryCatch(httr::GET(url=get_scala_ping_url(), httr::timeout(0.2))$status_code == 200, error = function(e) FALSE, warning = function(w) FALSE)
}

# Get search runner execution status
is_detect_running <- function() {
  stop_if_no_config(paste("Cannot check detect batch status for search without configuration setup")) 
  get_running_task_pid("detect")>=0
}

# Get runner active PID this function will return -1 if no process was detected
get_running_task_pid <- function(name) {
  # Getting PID file name based on task name which stored the PID of last time this task was running
  pid_path <- paste(conf$data_dir, "/",name, ".PID", sep = "")
  if(file.exists(pid_path)) {
    # Getting last runner pid from PID file if exists
    last_pid <- as.integer(readChar(pid_path, file.info(pid_path)$size))
    # Checking if last_pid is still running 
    # this check makes a system dependent call to get running processes launch by R* or rsession binaries
    # if the last_pid is found on the list of processes returned then we assume is running
    pid_running <- ( 
      if(.Platform$OS.type == "windows") {
        length(grep("R\\.exe|Rscript\\.exe|rsession\\.exe|Rterm\\.exe", system(paste('tasklist /nh /fi "pid eq ',last_pid,'"'), intern = TRUE))) > 0
      }
      else if(.Platform$OS.type == "mac") 
        system(paste("ps -cax | grep 'R\\|rsession' | grep ", last_pid), ignore.stdout = TRUE)==0 
      else 
        system(paste("ps -cax | grep 'R\\|rsession' | grep ", last_pid), ignore.stdout = TRUE)==0
    )
    if(pid_running)
      last_pid
    else
      -1
  }
  else -1
}



#' @title Get the \code{\link{detect_loop}} task status
#' @description Reads the status of the \code{\link{detect_loop}} tasks and updates it with changes requested by the Shiny app
#' @param statuses Character vector for limiting the status of the returned tasks, default: list()
#' @return A named list containing all necessary information to run and monitor the detect loop tasks.
#' @details After reading the tasks.json file and parsing it with jsonlite, this function will update the necessary fields in the 
#' tasks for executing and monitoring them.
#' @examples 
#' if(FALSE){
#'    #getting tasks statuses
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    tasks <- get_tasks()
#' }
#' @seealso 
#'  \code{\link{download_dependencies}}
#'
#'  \code{\link{update_geonames}}
#'
#'  \code{\link{update_languages}}
#'
#'  \code{\link{detect_loop}}
#'  
#'  \code{\link{generate_alerts}}
#'
#' @rdname get_tasks
#' @export 
#' @importFrom jsonlite fromJSON
get_tasks <- function(statuses = list()) {
  stop_if_no_config()
  # get the task path which should be data_folder/tasks.json
  # this file is only written by the detect loop process and read by all (shiny, search) 
  tasks_path <- get_tasks_path()

  #Loading tasks from file if exists 
  tasks <- if(file.exists(tasks_path)) {
    #if tasks files exists getting tasks from disk
    t <- jsonlite::fromJSON(tasks_path, simplifyVector = FALSE, auto_unbox = TRUE)
    # casting date time values (they are interpreted as character by jsonlite)
    for(i in 1:length(t)) {
      t[[i]]$scheduled_for <- if(class(t[[i]]$scheduled_for) == "numeric")  as.POSIXlt(t[[i]]$scheduled_for/1000,  origin="1970-01-01") else NA
      t[[i]]$started_on <- if(class(t[[i]]$started_on) == "numeric")  as.POSIXlt(t[[i]]$started_on/1000,  origin="1970-01-01") else NA
      t[[i]]$end_on <- if(class(t[[i]]$end_on) == "numeric")  as.POSIXlt(t[[i]]$end_on/1000,  origin="1970-01-01") else NA
      t[[i]]$status <- if(class(t[[i]]$status) == "character")  t[[i]]$status else NA
    }
    t
  } else {
    # If no tasks file is setup a default is built based on configuration
    list()
  }
  # Setting default dependencies task status if not set
  if(!exists("dependencies", where = tasks)) {
    #get Java & Scala dependencies
    tasks$dependencies <- list(
      task = "dependencies",
      order = 0,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  }
  # Setting default geonames task status if not set
  if(!exists("geonames", where = tasks)) {
    #get geonames
    tasks$geonames <- list(
        task = "geonames",
      order = 1,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  }
  # Setting default languages task status if not set
  if(!exists("languages", where = tasks)) {
    #get geonames
    tasks$languages <- list(
      task = "languages",
      order = 2,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  }
  # Setting default languages task status if not set
  if(exists("geotag", where = tasks)) {
    tasks$geotag <- NULL
  }
  # Setting default aggregate task status if not set
  if(exists("aggregate", where = tasks)) {
    tasks$aggregate <- NULL
  }

  # Setting default alerts task status if not set
  if(!exists("alerts", where = tasks)) {
    #alerts
    tasks$alerts <- list(
      task = "alerts",
      order = 3,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  } else if(exists("alerts", where = tasks) && tasks$alerts$order != 3) { #dealing with bad order case after migration
    tasks$alerts$order = 3
  }
  # The rest of the function code is to detect changes that has been requested by the shiny app
  # changes are detected by looking at xxx_updated_on or xxx_requested_on settings on the configuration saved from the shiny_app

  # Activating dependencies task if requested by the shiny app
  if(in_pending_status(tasks$dependencies)) {
     tasks$dependencies$maven_repo = conf$maven_repo 
     tasks$dependencies$winutils_url = conf$winutils_url 
     tasks$dependencies$status <- "pending" 
  }
    
  # Activating geonames task if requested by the shiny app
  if(in_pending_status(tasks$geonames)) {
     tasks$geonames$url = conf$geonames_url  
     tasks$geonames$status <- "pending" 
  }
    
  # Activating languages task if requested by the shiny app
  if(in_pending_status(tasks$languages)) 
    tasks$languages$status <- "pending"

  # Getting language information (used to detect id there is an action to perform on languages)
  # The tasks files contains the status of languages processed by the detect loop
  # The difference between the settings from the shiny app and the tasks will allow epitweetr to detect the actions to perform on languages
  langs <- get_available_languages() # All available languages for epitweetr
  lcodes <- langs$Code # available language codes
  lurls <- setNames(langs$Url, langs$Code) # available language URLs (to download models)
  lnames <- setNames(langs$Label, langs$Code) # available language names
  conf_codes <- sapply(conf$languages, function(l) l$code) # target languages to use in epitweetr as requested by setting (shiny app)
  task_codes <- tasks$languages$codes # code of languages already processed by the detect loop
  update_times <- setNames(sapply(conf$languages, function(l) l$modified_on), sapply(conf$languages, function(l) l$code)) # time where the change on languages was required by the shiny app

  # Calculating languages to add, update and remove by comparing configuration setting  with language information processed by the detect loop
  # remove: languages that are not anymore ion the settings
  to_remove <- lcodes[sapply(lcodes, function(c) c %in% task_codes && !(c %in% conf_codes))]
  # add: languages added on the settings
  to_add <- lcodes[sapply(lcodes, function(c) !(c %in% task_codes) && (c %in% conf_codes))]
  # update: languages with an update after the last time the language was update was started by the detect loop
  to_update <- lcodes[sapply(lcodes, function(c) (c %in% task_codes) && (c %in% conf_codes) 
    && ( is.na(tasks$languages$started_on)
       || is.na(tasks$languages$end_on)
       || (!is.null(update_times[[c]]) && strptime(update_times[[c]], "%Y-%m-%d %H:%M:%S") > tasks$languages$started_on)
       || tasks$languages$started_on > tasks$languages$end_on
    ))]
  # languages with no changes
  done <- lcodes[sapply(lcodes, function(c) (c %in% task_codes) && (c %in% conf_codes) 
    && (!is.na(tasks$languages$started_on)
      && ! is.na(tasks$languages$end_on)
      && (is.null(update_times[[c]]) || strptime(update_times[c], "%Y-%m-%d %H:%M:%S") <= tasks$languages$started_on)
      && tasks$languages$started_on <= tasks$languages$end_on
    ))]

  # merging four possible language cases to produce an updated version of what the detect loop should do
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
  
  # updating the language status and showing it as pending if we detected work to do
  tasks$languages$status <- (
    if((!is.na(tasks$languages$status) && tasks$languages$status == "success") &&  length(to_remove) + length(to_add) + length(to_update) > 0) 
      "pending"
    else
      tasks$languages$status
  ) 

  #filtering tasks by status if required and return
  if(length(statuses)==0)
    tasks
  else
    tasks[sapply(tasks, function(t) t$status %in% statuses)]
}

# Getting the scheduler task lists and plan execution by setting the scheduled_for times
# pan If tasks file exits it will read it, or get default tasks otherwise
plan_tasks <-function(statuses = list()) {
  tasks <- get_tasks(statuses)
  # Updating next execution time for each task
  now <- Sys.time()

  #sorting tasks by normal execution order
  sorted_tasks <- order(sapply(tasks, function(l) l$order))
  change <- FALSE 
  for(i in sorted_tasks) {
    # Scheduling pending tasks one shot tasks (not recurrent) they will simply be executed in order when pending
    if(tasks[[i]]$task %in% c("dependencies", "geonames", "languages")) {
      if(is.na(tasks[[i]]$status) || tasks[[i]]$status == "pending") {
        tasks[[i]]$status <- "scheduled"
        tasks[[i]]$scheduled_for <- now + (i - 1)/1000 # The (i-1)/1000 is not needed anymore
        change <- TRUE
        break #Just the first pending task is set to scheduled status
      }
    } else if (tasks[[i]]$task %in% c("alerts")) { 
      # dealing with recurrent tasks first if take in consideration the case when a manual request for execution has been performed
      if(in_requested_status(tasks[[i]])) {
        tasks[[i]]$status <- "scheduled"
        tasks[[i]]$scheduled_for <- now
        tasks[[i]]$failures <- 0
        change <- TRUE
        break # Just the first requested task is executed
      } else if(is.na(tasks[[i]]$status) || (  # dealing with normal recurrent scheduling. This is the case when status is not set (first time)
        tasks[[i]]$status %in% c("pending", "success", "scheduled") #or if status is pending success or scheduled (ready for next execution) 
        && { # this is the task ORDER after the last ended task base
          last_ended <- 
            Reduce(
	            x = list(tasks$alerts), 
	            f = function(a, b) { 
               if(is.na(a$end_on) && is.na(b$end_on)) {
                 if(a$order > b$order) a else b
               } else if(is.na(a$end_on)) b 
                 else if(is.na(b$end_on)) a 
                 else if(a$end_on > b$end_on) a 
                 else b
              }
            )
          one_shot_task_count <- 3 #NUMBER OF ONE SHOT TASKS UPDATE IT ON CHANGE
          next_order <- if(last_ended$order +1  <= max(sapply(tasks, `[[`, "order"))) last_ended$order + 1 else one_shot_task_count 
          tasks[[i]]$order == next_order

      })) {
        # In this case this is the next task to be executed on normal scheduling
        tasks[[i]]$status <- "scheduled"
        if(is.na(tasks[[i]]$end_on)) { # If taks has never ended then schedule it straight away
          tasks[[i]]$scheduled_for <- now + (i - 1)/1000
        } else if(tasks[[i]]$end_on >= tasks[[i]]$scheduled_for) { #if the task has ended after the last schedule (which should always be the case)
          # the last schedule was already executed a new schedule has to be set
          day_slots <- get_task_day_slots(tasks[[i]]) #Getting all day slots (times when the detect loop can be executed
          next_slots <- day_slots[day_slots > tasks[[i]]$scheduled_for] #getting the future execution slots
          #if next slot is in future set it. If it is in past, set now
          tasks[[i]]$scheduled_for <- if(length(next_slots)>0 && next_slots[[1]] > now) next_slots[[1]] else now + (i - 1)/1000
        }
        change <- TRUE
        break # only first task is set to scheduled
      }
    }  
  }
  # saving changes done on tasks
  if(change)
    save_tasks(tasks)

  # filtering the returned tasks by requested status
  if(length(statuses)==0)
    tasks
  else
    tasks[sapply(tasks, function(t) t$status %in% statuses)]
}

# get detect loop day slots based on start hour
# detect loop recurrent tasks will only be executed on this times
get_task_day_slots <- function(task) {
  span <- conf$schedule_span
  start_hour <- conf$schedule_start_hour
  per_day <- as.integer(24 * 60/span)
  # calculating possible next slot for
  hour_slots <- sort((c(0:(per_day-1)) * (span/60) + start_hour) %% 24)
  hour_slots <- c(hour_slots, hour_slots[[1]] + 24) #This adding first slot of next day
  day_slots <- hour_slots * 60 * 60 + (as.POSIXlt(as.Date(task$scheduled_for)))
  day_slots
}

# saves the provided task lists to the data directory.
save_tasks <- function(tasks) {
  stop_if_no_config()
  tasks_path <- get_tasks_path()
  write_json_atomic(tasks, tasks_path, pretty = TRUE, force = TRUE, auto_unbox = TRUE, POSIXt="epoch")
}

#' @title Runs the detect loop
#' @description Infinite loop ensuring the daily signal detection and email alerts 
#' @param data_dir Path to the 'data directory' containing application settings, models and collected tweets.
#' If not provided the system will try to reuse the existing one from last session call of \code{\link{setup_config}} or use the EPI_HOME environment variable, default: NA
#' @return nothing
#' @details The detect loop is composed of three 'one shot tasks' \code{\link{download_dependencies}}, \code{\link{update_geonames}}, \code{\link{update_languages}} ensuring the system has
#' all necessary components and data to run the three recurrent tasks \code{\link{generate_alerts}}
#'
#' The loop report progress on the 'tasks.json' file which is read or created by this function.
#'
#' The recurrent tasks are scheduled to be executed each 'detect span' minutes, which is a parameter set on the Shiny app.
#'
#' If any of these tasks fails it will be retried three times before going to a abort status. Aborted tasks can be relaunched from the Shiny app. 
#' @examples 
#' if(FALSE){
#'    #Running the detect loop
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    detect_loop()
#' }
#' @rdname detect_loop
#' @seealso
#'  \code{\link{download_dependencies}}
#'
#'  \code{\link{update_geonames}}
#'
#'  \code{\link{update_languages}}
#'
#'  \code{\link{detect_loop}}
#'  
#'  \code{\link{generate_alerts}}
#'
#'  \code{\link{get_tasks}}
#' @export 
detect_loop <- function(data_dir = NA) {
  # ensuring that config is set or using the one provided as a parameter
  if(is.na(data_dir) )
    setup_config_if_not_already()
  else
    setup_config(data_dir = data_dir)

  # registering the search runner (search.PID) or stopping if it is already running
  register_detect_runner()  

  # lass_sleeping_message set to ensure it will be shown if all is done
  last_sleeping_message <- Sys.time() - (conf$schedule_span * 60)
  while(TRUE) {
    # getting tasks to execute with one scheduled 
    tasks <- plan_tasks()
    # seeing if there is something to do 
    # either there is a scheduled task
    # or a task is in an inconsistency state (running) or failed (failed or aborted) 
    # aborted tasks are being taken into consideration since they should stop further taks from being executed 
    if(length(tasks[sapply(tasks, function(t) 
        !is.na(t$status) 
	        && (
	          t$status %in% c("failed", "running", "aborted")
	          || (t$status %in% c("scheduled") && t$scheduled_for <= Sys.time())
	        ))]
        )>0) 
    {
      # getting the position of the task to execute (first in one of the active status)
      i_next <- order(sapply(tasks, function(t) if(!is.na(t$status) && t$status %in% c("aborted", "scheduled", "failed", "running")) t$order else 999))[[1]] 
      # proceeding if task is not already aborted (otherwise keep looping)
      if(tasks[[i_next]]$status != "aborted") {
        # increasing number of failures if failed of running
        if(tasks[[i_next]]$status %in% c("failed", "running")) { 
          tasks[[i_next]]$failures = (if(!exists("failures", where = tasks[[i_next]])) 1 else tasks[[i_next]]$failures + 1)
        }
        # setting to aborted if the max number of retries has been reached
        if(tasks[[i_next]]$status %in% c("failed", "running") && tasks[[i_next]]$failures > 3) {
          tasks[[i_next]]$status = "aborted"
          tasks[[i_next]]$message = paste("Max number of retries reached", tasks[[i_next]]$message, sep = "\n")
          save_tasks(tasks)
          health_check(one_per_day = FALSE)
        }
        else { 
          # if task is not aborted proceeding with execution
          message(paste(Sys.time(), ": Executing task", tasks[[i_next]]$task))
          if(tasks[[i_next]]$task == "dependencies") {
            tasks <- download_dependencies(tasks)  
          }
          else if(tasks[[i_next]]$task == "geonames") {
            tasks <- update_geonames(tasks)  
          }
          else if(tasks[[i_next]]$task == "languages") {
            tasks <- update_languages(tasks)  
          }
          else if(tasks[[i_next]]$task == "alerts") {
            tasks <- generate_alerts(tasks)
          }
          # resetting failures to zero if successfully executed
          if(tasks[[i_next]]$status == "success") tasks[[i_next]]$failures = 0

          # saving tasks 
          save_tasks(tasks)
        }
      }
    } else if(last_sleeping_message + (conf$schedule_span * 60) < Sys.time() && 
      length(tasks[sapply(tasks, function(t) 
        !is.na(t$status) 
	        && (t$status %in% c("scheduled") && t$scheduled_for >= Sys.time())
	        )]
        )>0) { 
      #nothing to do (and not aborted tasks and 'nothing to do' message has not yet been shown for current detect slot
      i_next <- order(sapply(tasks, function(t) if(!is.na(t$status) && t$status %in% c("scheduled")) t$order else 999))[[1]] 
      message(paste(Sys.time(), ": Nothing else todo. Going to sleep each 5 seconds until ", format(tasks[[i_next]]$scheduled_for, tz=Sys.timezone(),usetz=TRUE)))
      last_sleeping_message <- Sys.time()
    }
    # epitweetr sanity check and sending email in case of issues
    health_check()
    # updating config to capture changes coming from search loop or shiny app
    setup_config(data_dir = conf$data_dir)
    # sleep for 5 seconds
    Sys.sleep(5)
  }

}

# Check if provided task is in pending status which means that a user has requested a manual run and the task is not already running or scheduled
in_pending_status <- function(task) {
  (
    (is.na(task$status) || task$status %in% c("pending", "success", "failure", "aborted")) 
    &&
    (
      (task$task == "dependencies" 
        && !is.na(conf$dep_updated_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$dep_updated_on, "%Y-%m-%d %H:%M:%S"))
      ) || (
       task$task == "geonames" 
        && !is.na(conf$geonames_updated_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$geonames_updated_on, "%Y-%m-%d %H:%M:%S"))
      ) || (
       task$task == "languages" 
        && !is.na(conf$lang_updated_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$lang_updated_on, "%Y-%m-%d %H:%M:%S"))
      )  || (
       task$task == "alerts" 
        && !is.na(conf$alerts_requested_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$alerts_requested_on, "%Y-%m-%d %H:%M:%S"))
      )   
    )
  )  
}

# Check if provided task is in requested status which means that a user has requested manual run
in_requested_status <- function(task) {
  (
    (
      (task$task == "dependencies" 
        && !is.na(conf$dep_updated_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$dep_updated_on, "%Y-%m-%d %H:%M:%S"))
      ) || (
       task$task == "geonames" 
        && !is.na(conf$geonames_updated_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$geonames_updated_on, "%Y-%m-%d %H:%M:%S"))
      ) || (
       task$task == "languages" 
        && !is.na(conf$lang_updated_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$lang_updated_on, "%Y-%m-%d %H:%M:%S"))
      ) || (
       task$task == "alerts" 
        && !is.na(conf$alerts_requested_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$alerts_requested_on, "%Y-%m-%d %H:%M:%S"))
      )   
    )
  )  
}



# Updating geonames task for reporting progress on geonames refresh
update_geonames_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$geonames$started_on = Sys.time() 
  if(end) tasks$geonames$end_on = Sys.time() 
  tasks$geonames$status =  status
  tasks$geonames$message = message
  save_tasks(tasks)
  return(tasks)
}

# Updating task for reporting progress on languages refresh
update_languages_task <- function(tasks, status, message, start = FALSE, end = FALSE, lang_code = character(0), lang_start = FALSE, lang_done = FALSE, lang_removed = FALSE) {
  if(start) tasks$languages$started_on = Sys.time() 
  if(end) tasks$languages$end_on = Sys.time()
  i <- 1
  while(i <= length(tasks$languages$code)) {
    if(tasks$languages$codes[[i]] == lang_code && lang_start) tasks$languages$statuses[[i]] = "running"
    if(tasks$languages$codes[[i]] == lang_code && lang_done) tasks$languages$statuses[[i]] = "done"
    if(tasks$languages$codes[[i]] == lang_code && lang_removed) {
      tasks$languages$codes <- tasks$languages$codes[-i]
      tasks$languages$statuses <- tasks$languages$statuses[-i]
      tasks$languages$urls <- tasks$languages$urls[-i]
      tasks$languages$names <- tasks$languages$names[-i]
      tasks$languages$vectors <- tasks$languages$vectors[-i]
      i <- i - 1
    }
    i <- i + 1
  }     
  
  tasks$languages$status =  status
  tasks$languages$message = message
  save_tasks(tasks)
  return(tasks)
}



# Updating alert task for reporting progress on alert detection
update_alerts_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$alerts$started_on = Sys.time() 
  if(end) tasks$alerts$end_on = Sys.time() 
  tasks$alerts$status =  status
  tasks$alerts$message = message
  save_tasks(tasks)
  return(tasks)
}


# Updating dependencies task for reporting progress on downloading Java & Scala dependencies
update_dep_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$dependencies$started_on = Sys.time() 
  if(end) tasks$dependencies$end_on = Sys.time() 
  tasks$dependencies$status =  status
  tasks$dependencies$message = message
  save_tasks(tasks)
  return(tasks)
}
