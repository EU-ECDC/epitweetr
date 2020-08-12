print(getwd())

if(!("devtools" %in% installed.packages()[,"Package"]))
  install.packages("devtools")

devtools::document()
devtools::build(binary=TRUE)

#removing directories to avoid bug preventing .rbuildigrore being read
if(.Platform$OS.type == "windows") {
  file.rename("scala/lib_managed", "../lib_managed")
  file.rename("scala/target", "../target")
  file.rename("scala/null", "../null")
  file.rename("scala/project/target", "../project_target")
  file.rename("scala/project/project", "../project_project")
  devtools::build(binary=FALSE)
  file.rename("../lib_managed", "scala/lib_managed")
  file.rename("../target", "scala/target")
  file.rename("../null", "scala/null")
  file.rename("../project_target", "scala/project/target")
  file.rename("../project_project", "scala/project/project")

} else 
  devtools::build(binary=FALSE)

if(!file.exists("install")){
  dir.create("install", showWarnings = FALSE)
}

installer_name <- (
  c(paste("epitweetr_",packageVersion("epitweetr"),".tar.gz", sep = ""),
    if(.Platform$OS.type == "windows")
      paste("epitweetr_",packageVersion("epitweetr"),".zip", sep = "") 
    else
      paste("epitweetr_",packageVersion("epitweetr"),"_R_x86_64-pc-linux-gnu.tar.gz", sep = "") 
  )
)
file.rename(file.path("..", installer_name), file.path("..", "install", installer_name))

detach("package:epitweetr", unload=TRUE)
#devtools::install_local(path =  file.path("install", installer_name), dependencies = TRUE)
install.packages(file.path("..", "install", installer_name[[2]]), dependencies = TRUE)

