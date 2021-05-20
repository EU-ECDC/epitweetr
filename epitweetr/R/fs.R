#' @export 
fs_loop <-  function(data_dir = NA) {
  # Setting or reusing the data directory
  if(is.na(data_dir) )
    setup_config_if_not_already()
  else
    setup_config(data_dir = data_dir)
  
  # Registering the fs runner using current PID and ensuring no other instance of the search is actually running.
  register_fs_runner()
  
  # Infinite loop calling the fs runner
  while(TRUE) {
    spark_job(
      paste(
	      "fsService"
        , "epiHome" , conf$data_dir
      )
    )
  }
}

