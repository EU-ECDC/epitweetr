print(getwd())
devtools::install_deps(pkg = ".", dependencies = TRUE)
devtools::document()
devtools::install()
