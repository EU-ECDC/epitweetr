package org.ecdc.epitweetr.geo

case class GeoTrainings(items:Seq[GeoTraining])

case class GeoTrainingSource(value:String)
object GeoTrainingSource {
  def apply(value:String) = value.toLowerCase.replace(" ", "-") match {
    case "tweet" => GeoTrainingSource.tweet
    case "epitweetr-model" => GeoTrainingSource.epitweetrModel
    case "epitweetr-database" => GeoTrainingSource.epitweetrDatabase
    case _ => GeoTrainingSource.manual
  }
  val tweet = new GeoTrainingSource("tweet") 
  val epitweetrModel = new GeoTrainingSource("epitweetr-model") 
  val epitweetrDatabase = new GeoTrainingSource("epitweetr-database") 
  val manual = new GeoTrainingSource("manual")
}

case class GeoTrainingPart(value:String)
object GeoTrainingPart {
  def apply(value:String) = value.toLowerCase.replace(" ", "-") match {
    case "text" => GeoTrainingPart.text
    case "user-location" => GeoTrainingPart.userLocation
    case "user-description" => GeoTrainingPart.userDescription
    case _ => new GeoTrainingPart(value)
  }
  val text = new GeoTrainingPart("text") 
  val userLocation = new GeoTrainingPart("user-location") 
  val userDescription = new GeoTrainingPart("user-description")
}

case class GeoTraining(
  category:String,  
  text:String,  
  locationInText:Option[String], 
  isLocation:Option[Boolean],  
  forcedLocationCode:Option[String], 
  forcedLocationName:Option[String], 
  source:GeoTrainingSource,
  tweetId:Option[String],
  lang:Option[String],
  tweetPart:Option[GeoTrainingPart], 
  foundLocation:Option[String], 
  foundLocationCode:Option[String], 
  foundCuntryCode:Option[String]
)
