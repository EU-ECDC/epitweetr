package org.ecdc.epitweetr

import spray.json._
import java.nio.file.{Paths, Files}
import org.ecdc.twitter.Language
import org.ecdc.epitweetr.geo.Geonames
import org.ecdc.epitweetr.fs.EpiSerialisation

case class Settings(epiHome:String) {
  var _properties:Option[JsObject] = None
  def load() = {
    val psource = scala.io.Source.fromFile(Paths.get(epiHome, "properties.json").toString)
    val plines = try psource.mkString finally psource.close() 
    _properties = Some(plines.parseJson.asJsObject)
    this
  }
  def loadJson(relativePath:String) = {
    val path = Paths.get(epiHome, relativePath)
    if(Files.exists(path)) {
      val psource = scala.io.Source.fromFile(path.toString, "UTF-8")
      val plines = try psource.mkString finally psource.close() 
      Some(plines.parseJson.asJsObject)
    }
    else None
  }

  def properties = _properties match {
    case Some(p) => p
    case None => throw new Exception("Please call load() before calling properties on these settings")
  } 

  def geoLocationThreshold = properties.fields.get("geolocation_threshold").map(v => v.asInstanceOf[JsString].value.toInt)
  def geonamesSimplify = properties.fields.get("geonames_simplify").map(v => v.asInstanceOf[JsBoolean].value)
  def knownUsers = 
    properties.fields.get("known_users").map(v => 
      v.asInstanceOf[JsArray]
        .elements
        .map(v => v.asInstanceOf[JsString].value)
    )
  def languages =  
    properties.fields.get("languages").map(v => 
      v.asInstanceOf[JsArray]
        .elements
        .map(v => v.asInstanceOf[JsObject].fields
          .mapValues(f => f.asInstanceOf[JsString].value)
        ).map(f => Language(f("name"), f("code"), f("vectors")))
    )

  def sparkCores = properties.fields.get("spark_cores").map(v => v.asInstanceOf[JsNumber].value.toInt)
  def sparkMemory =  properties.fields.get("spark_memory").map(v => v.asInstanceOf[JsString].value)
  def winutils =  System.getProperty("os.name").toLowerCase.contains("windows") match {
    case true => Some(Paths.get(this.epiHome, "hadoop").toString)
    case fakse => None
  }
  def langIndexPath =  Paths.get(epiHome, "geo","lang_vectors.index").toString 
  def collectionPath =  Paths.get(epiHome, "collections").toString 
  def geolocationThreshold = properties.fields.get("geolocation_threshold").map(v => v.asInstanceOf[JsString].value.toInt)
  def geolocationStrategy = Some("demy.mllib.index.PredictStrategy")
  def geoNBefore = 10
  def geoNAfter = 4
  def geonames = 
    Geonames(
      Paths.get(epiHome, "geo", "allCountries.txt").toString,  
      Paths.get(epiHome, "geo").toString,  
      properties.fields.get("geonames_simplify").map(v => v.asInstanceOf[JsBoolean].value).get
    )
  def fsRoot = Paths.get(epiHome, "fs").toString
  def fsBatchTimeout = properties.fields.get("fs_batch_timeout").map(v => v.asInstanceOf[JsNumber].value.toInt).getOrElse(60*60)
  def fsQueryTimeout = properties.fields.get("fs_query_timeout").map(v => v.asInstanceOf[JsNumber].value.toInt).getOrElse(60)
  def fsLongBatchTimeout = properties.fields.get("fs_long_batch_timeout").map(v => v.asInstanceOf[JsNumber].value.toInt).getOrElse(60*60*24)
  def fsPort = properties.fields.get("fs_port").map(v => v.asInstanceOf[JsNumber].value.toInt).getOrElse(8080)
  
  def splitter = properties.fields.get("tweetSplitterRegex").map(v => v.asInstanceOf[JsString].value).getOrElse(Settings.defaultSplitter)
  def forcedGeo = this.loadJson("geo/forced-geo.json").map(json => EpiSerialisation.forcedGeoFormat.read(json)) 
  def forcedGeoCodes = this.loadJson("geo/forced-geo-codes.json").map(json => EpiSerialisation.forcedGeoCodesFormat.read(json)) 
  def topicKeyWords = this.loadJson("geo/topic-keywords.json").map(json => EpiSerialisation.topicKeyWordsFormat.read(json)) 

}
object Settings {
  def defaultSplitter = "(http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|\\bRT\\b+|\\bvia\\b+|\\bvía\\b+|((?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+"
}
