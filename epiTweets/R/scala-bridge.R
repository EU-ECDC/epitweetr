#' Print a dummy hello world message
#' @export
test <- function(conf = "data/conf.json" ) {
 conf <- jsonlite::fromJSON(conf)

 return (rJava::J("org.ecdc.twitter.Geolocation")$localize(paste(conf$dataDir, "tweets", sep = "/")))
}
