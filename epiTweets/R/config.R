#Environment for storing configuration
conf <- new.env()

#' Get platform dependent keyring
#'
get_key_ring <- function(backend = NULL) {
  if(!is.null(backend)) {
    Sys.setenv(kr_backend = backend)
  }
  Sys.setenv(kr_name = "ecdc_wtitter_tool_kr_name")
  Sys.setenv(kr_service = "kr_service")
  keyring = Sys.getenv("kr_backend")
  if(keyring == "secret_service") {
    kb <- keyring::backend_secret_service$new()
  } else if(keyring == "wincred") {
    kb <- keyring::backend_wincred$new()
  } else if(keyring == "macos") {
    kb <- keyring::backend_macos$new()
  } else if(keyring == "file") {
    kb <- keyring::backend_file$new()
  } else {
    kb <- keyring::backend_env$new()
  }
  kr_name <- Sys.getenv("kr_name") 
  if(kb$has_keyring_support()) {
     krs <- kb$keyring_list()
     if(nrow(krs[krs$keyring == kr_name,]) == 0) {
       kb$keyring_create(kr_name)
       #kb$.__enclos_env__$private$keyring_create_direct(kr_name, kr_passphrase)
     }
    kb$keyring_set_default(kr_name)
  }

  if(kb$keyring_is_locked(keyring = kr_name)) {
    kb$keyring_unlock(keyring = kr_name)
  }
  return (kb)
}

#' Set a secret from the chosen secret management backend
set_secret <- function(secret, value) {
  get_key_ring()$set_with_value(service = Sys.getenv("kr_service"), username = secret, password = value)
}

#' Checks weather a secret is set on the secret management backend
is_secret_set <- function(secret) {
 secrets <- get_key_ring()$list(service =  Sys.getenv("kr_service")) 
 return(nrow(secrets[secrets$username == secret, ])>0) 
}
#' Get a secret from the chosen secret management backend
get_secret <- function(secret) {
  get_key_ring()$get(service =  Sys.getenv("kr_service"), username = secret)
}

#' Build configuration values for application from a configuration json file
#' @export
setup_config <- function(path = "data/conf.json") {
  temp <- jsonlite::read_json(path, simplifyVector = TRUE)
  conf$keyring <- temp$keyring
  conf$schedule_span <- temp$schedule_span
  conf$dataDir <- temp$dataDir
  kr <- get_key_ring(conf$keyring)
  conf$topics <- temp$topics
  conf$topics$plan <- NA
  if("plan" %in% colnames(temp$topics)) {
    for(i in 1:nrow(temp$topics)) {
      plans <-  
        lapply(split(temp$topics[i,c("plan")], seq(nrow(temp$topics[i,c("plan")]))), 
          function(plan) get_plan(
            expected_end = plan$expected_end
            , scheduled_for = plan$scheduled_for
            , start_on =plan$start_on
            , end_on = plan$end_on
            , max_id = plan$max_id
            , since_id = plan$since_id
            , since_target = plan$since_target
            , results_span = plan$results_span
            , requests = plan$requests
            , progress = plan$progress
        ))
        conf$topics[i, c("plan")] <- fold_to_assign(plans)
    }
  }
  conf$twitter_auth <- list()
  for(v in c("app", "access_token", "access_token_secret", "api_key", "api_secret")) {
    if(is_secret_set(v)) {
      conf$twitter_auth[[v]] <- get_secret(v)
    }
  }
}

#' Save the configuration options to disk
#' @export
save_config <- function(path = "data/conf.json") {
  temp <- list()
  temp$dataDir <- conf$dataDir
  temp$schedule_span <- conf$schedule_span
  temp$keyring <- conf$keyring
  temp$topics <- conf$topics
  temp$topics$rank <- NULL
  temp$topics$schedule_to_run <- NULL
  jsonlite::write_json(temp, path, pretty = TRUE, force = TRUE)

}

#' Update twitter auth tokens on configuration object
#' @export
set_twitter_app_auth <- function(app, access_token, access_token_secret, api_key, api_secret) {
  conf$twitter_auth$app <- app
  conf$twitter_auth$access_token <- access_token
  conf$twitter_auth$access_token_secret <- access_token_secret
  conf$twitter_auth$api_key <- api_key
  conf$twitter_auth$api_secret <- api_secret
  for(v in c("app", "access_token", "access_token_secret", "api_key", "api_secret")) {
    set_secret(v, conf$twitter_auth[[v]])
  }
}

