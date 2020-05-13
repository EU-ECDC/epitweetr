#!/usr/bin/env Rscript
# script for running background tasks
args = commandArgs(trailingOnly=TRUE)

# test if the data directory path has been provided
if (length(args)==0) {
  stop("The config file has not been provided", call.=FALSE)
} 

data_dir <- args[1]
epitweetr::detect_loop(data_dir)  


