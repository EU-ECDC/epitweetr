# creating package and installing it
print(getwd())

if(!("devtools" %in% installed.packages()[,"Package"]))
  install.packages("devtools")

devtools::install_deps()
devtools::document()
devtools::build(binary=TRUE)
devtools::build(binary=FALSE)
devtools::build_manual()

if(!file.exists(file.path("..", "install"))){
  dir.create(file.path("..", "install"), showWarnings = FALSE)
}
if(!file.exists(file.path("..", "manual"))){
  dir.create(file.path("..", "manual"), showWarnings = FALSE)
}

#moving installer
installer_name <- (
  c(paste("epitweetr_",packageVersion("epitweetr"),".tar.gz", sep = ""),
    if(.Platform$OS.type == "windows")
      paste("epitweetr_",packageVersion("epitweetr"),".zip", sep = "") 
    else
      paste("epitweetr_",packageVersion("epitweetr"),"_R_x86_64-pc-linux-gnu.tar.gz", sep = "") 
  )
)
file.rename(file.path("..", installer_name), file.path("..", "install", installer_name))

manual_name <- paste("epitweetr_",packageVersion("epitweetr"),".pdf", sep = "") 
#moving manual
file.rename(file.path("..", manual_name), file.path("..", "manual", manual_name))


detach("package:epitweetr", unload=TRUE)
install.packages(file.path("..", "install", installer_name[[2]]), dependencies = TRUE)

