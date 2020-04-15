#.onLoad <- function(libname, pkgname){
#   setup_config(ignore_keyring = TRUE)
#   if(conf$rJava) {
#     options( java.parameters = paste("-Xmx", conf$spark_memory, sep="" ))
#     rJava::.jpackage(pkgname, lib.loc=libname)
#   }
#}
