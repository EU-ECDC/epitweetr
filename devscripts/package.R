print(getwd())

if(!("devtools" %in% installed.packages()[,"Package"]))
  install.packages("devtools")

devtools::document()
devtools::build(binary=TRUE)
devtools::build(binary=FALSE)

if(!file.exists("install")){
  dir.create("install", showWarnings = FALSE)
}

installer_name <- (
  c("epitweetr_0.0.0.9000.tar.gz",
    if(.Platform$OS.type == "windows")
      "epitweetr_0.0.0.9000.zip" 
    else
      "epitweetr_0.0.0.9000_R_x86_64-pc-linux-gnu.tar.gz" 
  )
)
file.rename(file.path("..", installer_name), file.path("..", "install", installer_name))

detach("package:epitweetr", unload=TRUE)
#devtools::install_local(path =  file.path("install", installer_name), dependencies = TRUE)
install.packages(file.path("..", "install", installer_name), dependencies = TRUE)

