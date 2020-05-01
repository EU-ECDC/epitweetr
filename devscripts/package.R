print(getwd())

if(!("devtools" %in% installed.packages()[,"Package"]))
  install.packages("devtools")

devtools::install_deps(pkg = ".", dependencies = TRUE)
devtools::document()
devtools::install()
