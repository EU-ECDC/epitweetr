# Registers the search runner for the current process
register_search_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for search without configuration setup")) 
  register_runner("search")
}


# Registers the detect runner for the current process
register_detect_runner <- function() {
  stop_if_no_config(paste("Cannot check running status for detect without configuration setup")) 
  register_runner("detect")
}

# Register search runner task and start it
register_search_runner_task <- function() {
  register_runner_task("search")
}

# Register detect runner task and start it
register_detect_runner_task <- function() {
  register_runner_task("detect")
}

# Register task and start it
register_runner_task <- function(task_name) {
  stop_if_no_config(paste("Cannot register scheduled task without configuration setup")) 
  
  script_name <- paste(task_name, "R", sep = ".")

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
        , rscript_args = paste("\"", conf$data_dir,"\"", sep = "")
        , startdate =  
          if(conf$force_date_format == "")
            tail(strsplit(shell("echo %DATE%", intern= TRUE), " ")[[1]], 1)
          else
            strftime(Sys.time(), conf$force_date_format)
        , schtasks_extra="/F"
      )
    }
    else warning("Please install taskscheduler Package")
  } else {
     warning("Task scheduling is not implemeted yet or this OS. You can still schedule it manually. Please refer to package vignette.")
  }
		   
}

# Get search runner execution status
is_search_running <- function() {
  stop_if_no_config(paste("Cannot check running status for search without configuration setup")) 
  get_running_task_pid("search")>=0
}

# Get search runner execution status
is_detect_running <- function() {
  stop_if_no_config(paste("Cannot check detect batch status for search without configuration setup")) 
  get_running_task_pid("detect")>=0
}

# Get runner active PID
get_running_task_pid <- function(name) {
  pid_path <- paste(conf$data_dir, "/",name, ".PID", sep = "")
  if(file.exists(pid_path)) {
    # Getting last runner pid
    last_pid <- as.integer(readChar(pid_path, file.info(pid_path)$size))
      # Checking if last_pid is still running
    pid_running <- ( 
      if(.Platform$OS.type == "windows") {
        length(grep("R\\.exe|Rscript\\.exe|rsession\\.exe", system(paste('tasklist /nh /fi "pid eq ',last_pid,'"'), intern = TRUE))) > 0
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


# Register as the scheduler runner if there is no other scheduler running or quit otherwise
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
#'  \code{\link{geotag_tweets}}
#'  
#'  \code{\link{aggregate_tweets}}
#'
#'  \code{\link{generate_alerts}}
#'
#' @rdname get_tasks
#' @export 
#' @importFrom jsonlite fromJSON
get_tasks <- function(statuses = list()) {
  stop_if_no_config()
  tasks_path <- get_tasks_path()
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
    list()
  }
  if(!exists("dependencies", where = tasks)) {
    #get java & scala dependencies
    tasks$dependencies <- list(
      task = "dependencies",
      order = 0,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  }
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
  if(!exists("geotag", where = tasks)) {
    #geo tag
    tasks$geotag <- list(
      task = "geotag",
      order = 3,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  }
  if(!exists("aggregate", where = tasks)) {
    #aggregate
    tasks$aggregate <- list(
      task = "aggregate",
      order = 4,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  }
  if(!exists("alerts", where = tasks)) {
    #alerts
    tasks$alerts <- list(
      task = "alerts",
      order = 5,
      started_on = NA,
      end_on = NA,
      status = NA,
      scheduled_for = NA
    )
  }
  # Activating one shot tasks with configuration changes
  if(in_pending_status(tasks$dependencies)) {
     tasks$dependencies$maven_repo = conf$maven_repo 
     tasks$dependencies$winutils_url = conf$winutils_url 
     tasks$dependencies$status <- "pending" 
  }
    
  # Activating one shot tasks with configuration changes
  if(in_pending_status(tasks$geonames)) {
     tasks$geonames$url = conf$geonames_url  
     tasks$geonames$status <- "pending" 
  }
    
  # Updating languages tasks
  if(in_pending_status(tasks$languages)) 
    tasks$languages$status <- "pending"

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
    if((!is.na(tasks$languages$status) && tasks$languages$status != "running") &&  length(to_remove) + length(to_add) + length(to_update) > 0) 
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
# pan If tasks file exits it will read it, or get default tasks otherwise
plan_tasks <-function(statuses = list()) {
  tasks <- get_tasks(statuses)
  # Updating next execution time for each task
  now <- Sys.time()
  sorted_tasks <- order(sapply(tasks, function(l) l$order)) 
  for(i in sorted_tasks) {
    # Scheduling pending tasks
    if(tasks[[i]]$task %in% c("dependencies", "geonames", "languages")) {
      if(is.na(tasks[[i]]$status) || tasks[[i]]$status == "pending") {
        tasks[[i]]$status <- "scheduled"
        tasks[[i]]$scheduled_for <- now + (i - 1)/1000
        break
      }
    } else if (tasks[[i]]$task %in% c("geotag", "aggregate", "alerts")) { 
      if(in_requested_status(tasks[[i]])) {
        tasks[[i]]$status <- "scheduled"
        tasks[[i]]$scheduled_for <- now
        tasks[[i]]$failures <- 0
        break
      } else if(is.na(tasks[[i]]$status) || (
        tasks[[i]]$status %in% c("pending", "success", "scheduled") 
        && {
          last_ended <- 
            Reduce(
	            x = list(tasks$geotag, tasks$aggregate, tasks$alerts), 
	            f = function(a, b) { 
               if(is.na(a$end_on) && is.na(b$end_on)) {
                 if(a$order > b$order) a else b
               } else if(is.na(a$end_on)) b 
                 else if(is.na(b$end_on)) a 
                 else if(a$end_on > b$end_on) a 
                 else b
              }
            )
          next_order <- if(last_ended$order +1  <= max(sapply(tasks, `[[`, "order"))) last_ended$order + 1 else 3
          tasks[[i]]$order == next_order

      })) {
        tasks[[i]]$status <- "scheduled"
        if(is.na(tasks[[i]]$end_on)) { 
          tasks[[i]]$scheduled_for <- now + (i - 1)/1000
        } else if(tasks[[i]]$end_on >= tasks[[i]]$scheduled_for) {
          # the last schedule was already executed a new schedule has to be set
          day_slots <- get_task_day_slots(tasks[[i]])
          next_slot <- day_slots[day_slots > tasks[[i]]$scheduled_for][[1]]
          #if next slot is in future set it. If it is in past, set now
          tasks[[i]]$scheduled_for <- if(next_slot > now) next_slot else now + (i - 1)/1000
        }
        break 
      }
    }  
  }
  save_tasks(tasks)
  if(length(statuses)==0)
    tasks
  else
    tasks[sapply(tasks, function(t) t$status %in% statuses)]
}

# get day slots
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
#' all necessary components and data to run the three recurrent tasks, \code{\link{geotag_tweets}}, \code{\link{aggregate_tweets}}, \code{\link{generate_alerts}}
#'
#' The loop report progress on the 'tasks.json' file which is read or created by this function.
#'
#' The recurrent tasks are scheduled to be executed each 'detect span' minutes, which is a parameter set on the Shiny app.
#'
#' If any of these tasks fails it will be retried three times before going to a abort status. Aborted tasks can be relauched from the Shiny app. 
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
#'  \code{\link{geotag_tweets}}
#'  
#'  \code{\link{aggregate_tweets}}
#'  
#'  \code{\link{generate_alerts}}
#'
#'  \code{\link{get_tasks}}
#' @export 
detect_loop <- function(data_dir = NA) {
  if(is.na(data_dir) )
    setup_config_if_not_already()
  else
    setup_config(data_dir = data_dir)
  register_detect_runner()  
  last_sleeping_message <- Sys.time() - (conf$schedule_span * 60)
  while(TRUE) {
    # getting tasks to execute 
    tasks <- plan_tasks()
    if(length(tasks[sapply(tasks, function(t) 
        !is.na(t$status) 
	        && (
	          t$status %in% c("failed", "running", "aborted")
	          || (t$status %in% c("scheduled") && t$scheduled_for <= Sys.time())
	        ))]
        )>0) 
    {
      i_next <- order(sapply(tasks, function(t) if(!is.na(t$status) && t$status %in% c("aborted", "scheduled", "failed", "running")) t$order else 999))[[1]] 
      if(tasks[[i_next]]$status != "aborted") {
        if(tasks[[i_next]]$status %in% c("failed", "running")) { 
          tasks[[i_next]]$failures = (if(!exists("failures", where = tasks[[i_next]])) 1 else tasks[[i_next]]$failures + 1)
        }
        if(tasks[[i_next]]$status %in% c("failed", "running") && tasks[[i_next]]$failures > 3) {
          tasks[[i_next]]$status = "aborted"
          tasks[[i_next]]$message = paste("Max number of retries reached", tasks[[i_next]]$message, sep = "\n")
          save_tasks(tasks)
        }
        else { 
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
          else if(tasks[[i_next]]$task == "geotag") {
            tasks <- geotag_tweets(tasks) 
          }
          else if(tasks[[i_next]]$task == "aggregate") {
            tasks <- aggregate_tweets(tasks = tasks) 
          }
          else if(tasks[[i_next]]$task == "alerts") {
            tasks <- generate_alerts(tasks)
          }

          if(tasks[[i_next]]$status == "success") tasks[[i_next]]$failures = 0
          save_tasks(tasks)
        }
      }
    } else if(last_sleeping_message + (conf$schedule_span * 60) < Sys.time() && 
      length(tasks[sapply(tasks, function(t) 
        !is.na(t$status) 
	        && (t$status %in% c("scheduled") && t$scheduled_for >= Sys.time())
	        )]
        )>0) { 
      i_next <- order(sapply(tasks, function(t) if(!is.na(t$status) && t$status %in% c("scheduled")) t$order else 999))[[1]] 
      message(paste(Sys.time(), ": Nothing else todo. Going to sleep each 5 seconds until ", format(tasks[[i_next]]$scheduled_for, tz=Sys.timezone(),usetz=TRUE)))
      last_sleeping_message <- Sys.time()
    }
    setup_config(data_dir = conf$data_dir)
    # To remove
    Sys.sleep(5)
  }

}

# Check if provided task is in pending status
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
      ) || (
       task$task == "aggregate" 
        && !is.na(conf$aggregate_requested_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$aggregate_requested_on, "%Y-%m-%d %H:%M:%S"))
      )  || (
       task$task == "geotag" 
        && !is.na(conf$geotag_requested_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$geotag_requested_on, "%Y-%m-%d %H:%M:%S"))
      )  || (
       task$task == "alerts" 
        && !is.na(conf$alerts_requested_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$alerts_requested_on, "%Y-%m-%d %H:%M:%S"))
      )   
    )
  )  
}

# Check if provided task is in requested status
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
       task$task == "aggregate" 
        && !is.na(conf$aggregate_requested_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$aggregate_requested_on, "%Y-%m-%d %H:%M:%S"))
      )  || (
       task$task == "geotag" 
        && !is.na(conf$geotag_requested_on)
        && (is.na(task$started_on) || task$started_on < strptime(conf$geotag_requested_on, "%Y-%m-%d %H:%M:%S"))
      )  || (
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
update_languages_task <- function(tasks, status, message, start = FALSE, end = FALSE, lang_code = character(0), lang_start = FALSE, lang_done = FALSE) {
  if(start) tasks$languages$started_on = Sys.time() 
  if(end) tasks$languages$end_on = Sys.time()
  for(i in 1:length(tasks$languages$code)) {
    if(tasks$languages$code[[i]] == lang_code && lang_start) tasks$languages$statuses[[i]] = "running"
    if(tasks$languages$code[[i]] == lang_code && lang_done) tasks$languages$statuses[[i]] = "done"
  }     
  tasks$languages$status =  status
  tasks$languages$message = message
  save_tasks(tasks)
  return(tasks)
}


# Updating geotag task for reporting progress on geotagging
update_geotag_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$geotag$started_on = Sys.time() 
  if(end) tasks$geotag$end_on = Sys.time() 
  tasks$geotag$status =  status
  tasks$geotag$message = message
  save_tasks(tasks)
  return(tasks)

}

# Updating aggregate task for reporting progress on aggregation
update_aggregate_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$aggregate$started_on = Sys.time() 
  if(end) tasks$aggregate$end_on = Sys.time() 
  tasks$aggregate$status =  status
  tasks$aggregate$message = message
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


# Updating dependencies task for reporting progress on downloading java & scala dependencies
update_dep_task <- function(tasks, status, message, start = FALSE, end = FALSE) {
  if(start) tasks$dependencies$started_on = Sys.time() 
  if(end) tasks$dependencies$end_on = Sys.time() 
  tasks$dependencies$status =  status
  tasks$dependencies$message = message
  save_tasks(tasks)
  return(tasks)
}
