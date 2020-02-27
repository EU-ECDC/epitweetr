

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
       kb$keyring_create_direct(kr_name)
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
 return(nrow(secrets[vars$username == secret])>1) 
}
#' Get a secret from the chosen secret management backend
get_secret <- function(secret) {
  get_key_ring()$get(service =  Sys.getenv("kr_service"), username = secret)
}

#' Build configuration values for application
#' @export
build_config <- function(conf_file = "data/conf.json") {
  conf <- jsonlite::fromJSON(conf_file)
  kr <- get_key_ring(conf$keyring)
  conf$twitter_auth <- list()
  for(v in c("access_token", "access_token_secret", "api_key", "api_secret")) {
    if(is_secret_set(v)) {
      conf[v] <- get_secret(v)
    }
  }
}



