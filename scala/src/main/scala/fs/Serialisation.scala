package org.ecdc.epitweetr.fs

import org.ecdc.epitweetr.EpitweetrActor
import org.ecdc.epitweetr.geo.{GeoTrainings, GeoTraining, GeoTrainingSource, GeoTrainingPart}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{JsString, JsNull, JsValue, JsNumber, DefaultJsonProtocol, JsonFormat, RootJsonFormat, JsObject, JsArray, JsBoolean, JsField, JsonParser}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale 
import scala.util.Try
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField}
import org.apache.lucene.index.IndexableField
import scala.collection.JavaConverters._
import java.net.URLDecoder

case class AlertClassification(alerts:Seq[TaggedAlert], runs:Option[Seq[AlertRun]])
case class TaggedAlert(
  id:String,
  date:String,
  topic:String,
  country:String,
  number_of_tweets:Int, 
  topwords:Option[String],
  toptweets:Map[String, Seq[String]],
  given_category:Option[String],
  epitweetr_category:Option[String],
  test:Option[Boolean],
  augmented:Option[Boolean],
  deleted:Option[Boolean],

) {
  def setEpitweetrCategory(cat:String) =  
    TaggedAlert(
      id = id,
      date = date,
      topic = topic,
      country = country,
      number_of_tweets = number_of_tweets, 
      topwords = topwords,
      toptweets = toptweets,
      given_category = given_category,
      epitweetr_category = Some(cat),
      test = test,
      augmented = augmented,
      deleted = deleted
    )
}

case class AlertRun(
  ranking:Int,
  models:String,
  alerts:Option[Int],
  runs:Int,
  f1score:Double,
  accuracy:Double, 
  precision_by_class:String,
  sensitivity_by_class:String,
  fscore_by_class:String,
  last_run:Option[String],
  balance_classes:Option[Boolean],
  force_to_use:Option[Boolean],
  active:Option[Boolean],
  documentation:Option[String],
  custom_parameters:Option[Map[String, String]] 
) {
  def setRanking(rank:Int) = 
     AlertRun(
       ranking = rank,
       models = models,
       alerts = alerts,
       runs = runs,
       f1score = f1score,
       accuracy = accuracy, 
       precision_by_class = precision_by_class,
       sensitivity_by_class = sensitivity_by_class,
       fscore_by_class = fscore_by_class,
       last_run = last_run,
       balance_classes = balance_classes,
       force_to_use = force_to_use,
       active = active,
       documentation = documentation,
       custom_parameters = custom_parameters 
     ) 
  def setCount(count:Int) = 
     AlertRun(
       ranking = ranking,
       models = models,
       alerts = Some(count),
       runs = runs,
       f1score = f1score,
       accuracy = accuracy, 
       precision_by_class = precision_by_class,
       sensitivity_by_class = sensitivity_by_class,
       fscore_by_class = fscore_by_class,
       last_run = last_run,
       balance_classes = balance_classes,
       force_to_use = force_to_use,
       active = active,
       documentation = documentation,
       custom_parameters = custom_parameters 
     ) 
}

case class TopicKeyWords(items:Map[String, Set[String]])
case class ForcedGeo(items:Map[String, String])
case class ForcedGeoCodes(items:Map[String, String])


case class TextsToGeo(items:Seq[TextToGeo])
case class TextToGeo(id:String, text:String, lang:Option[String])

case class TweetsV1(items:Seq[TweetV1])
case class TopicTweetsV1(topic:String, tweets:TweetsV1)
case class TweetV1(tweet_id:Long, text:String, linked_text:Option[String],user_description:String, linked_user_description:Option[String],
             is_retweet:Boolean, screen_name:String, user_name:String, user_id:Long , user_location:String, linked_user_name:Option[String], linked_screen_name:Option[String], 
             linked_user_location:Option[String], created_at:Instant, lang:String, linked_lang:Option[String], tweet_longitude:Option[Float], tweet_latitude:Option[Float], 
             linked_longitude:Option[Float], linked_latitude:Option[Float], place_type:Option[String], place_name:Option[String], place_full_name:Option[String], 
             linked_place_full_name:Option[String], place_country_code:Option[String], place_country:Option[String], 
             place_longitude:Option[Float], place_latitude:Option[Float],linked_place_longitude:Option[Float], linked_place_latitude:Option[Float],
             hashtags:Option[Seq[String]], urls:Option[Seq[String]], contexts:Option[Seq[String]], entities:Option[Seq[String]])

case class TweetsV2(data:Option[Seq[TweetV2]], includes:Option[IncludesV2], meta:MetaV2) {
  def toV1 = TweetsV1(items = data.map(tts => tts.map(t => t.toV1()(includes.get))).getOrElse(Seq[TweetV1]()))
}

case class TweetV2(id:String, lang:String, author_id:String, text:String, possibly_sensitive:Boolean, created_at:String, 
              referenced_tweets:Option[Seq[TweetRefV2]], geo:Option[PlaceRefV2], entities:Option[EntitiesV2], context_annotations:Option[Seq[AnnotationV2]]
              ) {
  def toV1()(implicit includes:IncludesV2) = 
   TweetV1(
     tweet_id = this.id.toLong, 
     text = this.text, 
     linked_text = this.getLinkedTweet().map(t => t.text),
     user_description = this.getAuthor().description.getOrElse(""), 
     linked_user_description = this.getLinkedAuthor().map(u => u.description.getOrElse("")),
     is_retweet = this.referenced_tweets.map(rts => rts.size > 0).getOrElse(false), 
     screen_name = this.getAuthor().name, 
     user_name = this.getAuthor().username, 
     user_id = this.getAuthor().id.toLong , 
     user_location = this.getAuthor().location.getOrElse(""), 
     linked_user_name = this.getLinkedAuthor().map(u => u.username), 
     linked_screen_name = this.getLinkedAuthor().map(u => u.name), 
     linked_user_location = this.getLinkedAuthor().map(u => u.location.getOrElse("")), 
     created_at = Instant.parse(this.created_at), 
     lang = this.lang, 
     linked_lang = this.getLinkedTweet().map(t => t.lang), 
     tweet_longitude = this.getLong(), 
     tweet_latitude =   this.getLat(), 
     linked_longitude =  this.getLinkedTweet.flatMap(t => t.getLong()), 
     linked_latitude =  this.getLinkedTweet.flatMap(t => t.getLat()), 
     place_type = this.getPlace().map(p => p.place_type), 
     place_name = this.getPlace().map(p => p.name), 
     place_full_name = this.getPlace().map(p => p.full_name), 
     linked_place_full_name = this.getLinkedTweet().flatMap(t => t.getPlace().map(p => p.full_name)), 
     place_country_code = this.getPlace().map(p => p.country_code), 
     place_country = this.getPlace().map(p => p.country), 
     place_longitude = this.getPlace().flatMap(p => p.geo.map(g => g.getAvgLong)), 
     place_latitude = this.getPlace().flatMap(p => p.geo.map(g => g.getAvgLat)),
     linked_place_longitude = this.getLinkedTweet().flatMap(t => t.getPlace().flatMap(p => p.geo.map(g => g.getAvgLong))), 
     linked_place_latitude =  this.getLinkedTweet().flatMap(t => t.getPlace().flatMap(p => p.geo.map(g => g.getAvgLat))),
     hashtags = this.entities.toSeq.flatMap(ent => ent.hashtags.toSeq.flatMap(tags => tags.map(ts => ts.tag))) match {case a if a.size == 0 => None case a => Some(a)},
     urls = this.entities.toSeq.flatMap(ent => ent.urls.toSeq.flatMap(us => us.map(u => u.expanded_url))) match {case a if a.size == 0 => None case a =>  Some(a)},
     contexts = this.context_annotations.toSeq.flatMap(cas => cas.toSeq.flatMap(ann => ann.domain.map(d => d.name))) match {case a if a.size == 0 => None case a => Some(a)},
     entities = this.context_annotations.toSeq.flatMap(cas => cas.toSeq.flatMap(ann => ann.entity.map(e => e.name))) match {case a if a.size == 0 => None case a => Some(a)}
   )
   def getAuthor()(implicit includes:IncludesV2) = includes.userMap(this.author_id)
   def getLinkedTweet()(implicit includes:IncludesV2) = this.referenced_tweets.flatMap(rfd => includes.tweetMap.get(rfd.head.id))
   def getLinkedAuthor()(implicit includes:IncludesV2) = getLinkedTweet().map(lt => includes.userMap(lt.author_id))
   def getLat()(implicit includes:IncludesV2) =  this.geo.flatMap(g => g.coordinates.flatMap(coords => coords.coordinates).flatMap{case Seq(lat, long) => Some(long) case _ => None})
   def getLong()(implicit includes:IncludesV2) =  this.geo.flatMap(g => g.coordinates.flatMap(coords => coords.coordinates).flatMap{case Seq(lat, long) => Some(lat) case _ => None})
   def getPlace()(implicit includes:IncludesV2) = this.geo.flatMap(g => g.place_id.flatMap(place_id => includes.placeMap.get(place_id)))
}

case class IncludesV2(users:Option[Seq[UserV2]], tweets:Option[Seq[TweetV2]], places:Option[Seq[PlaceV2]]) {
  def userMap = this.users.map(uss => uss.map(u => (u.id, u)).toMap).getOrElse(Map[String, UserV2]())
  def tweetMap = this.tweets.map(tts => tts.map(t => (t.id, t)).toMap).getOrElse(Map[String, TweetV2]())
  def placeMap = this.places.map(pss => pss.map(p => (p.id, p)).toMap).getOrElse(Map[String, PlaceV2]())
}
case class TweetRefV2(`type`:String, id:String)
case class PlaceRefV2(place_id:Option[String], coordinates:Option[CoordinatesV2])
case class EntitiesV2(hashtags:Option[Seq[HashTagV2]], urls:Option[Seq[UrlV2]])
case class HashTagV2(tag:String)
case class UrlV2(url:String, expanded_url:String)
case class AnnotationV2(domain:Option[AnnotatedItemV2], entity:Option[AnnotatedItemV2])
case class AnnotatedItemV2(id:String, name:String) 
case class CoordinatesV2(`type`:String, coordinates:Option[Seq[Float]])
case class PlaceV2(full_name:String, id:String, contained_within:Option[Array[String]], country:String, country_code:String, geo:Option[GeoJson], name:String, place_type:String)
case class GeoJson(`type`:String, bbox:Seq[Float]) {
  def getAvgLong = (bbox(0) + bbox(2))/2 
  def getAvgLat = (bbox(1) + bbox(3))/2 
}
case class MetaV2(result_count:Int, newest_id:Option[String], oldest_id:Option[String])
case class UserV2(id:String, name:String, username:String, description:Option[String], location:Option[String])


case class GeolocatedTweets(items:Seq[GeolocatedTweet])
case class Location(geo_code:String, geo_country_code:String,geo_id:Long, geo_latitude:Double, geo_longitude:Double, geo_name:String, geo_type:String)
case class GeolocatedTweet(var topic:String, id:Long, is_geo_located:Boolean, lang:String, linked_place_full_name_loc:Option[Location], linked_text_loc:Option[Location], 
    place_full_name_loc:Option[Location], text_loc:Option[Location], user_description_loc:Option[Location], user_location_loc:Option[Location]) {
  def fixTopic = {
    this.topic = URLDecoder.decode(this.topic, "UTF-8")
    this

  }
}


case class Collection(
  name:String,
  dateCol:String,
  pks:Seq[String],
  aggr:Map[String, String],
  aggregation:Aggregation 
)

case class Aggregation(
  columns:Seq[String], 
  groupBy:Option[Seq[String]], 
  filterBy:Option[Seq[String]], 
  sortBy:Option[Seq[String]], 
  sourceExpressions:Option[Seq[String]], 
  params:Option[Map[String, String]],
)


object EpiSerialisation
  extends SprayJsonSupport
    with DefaultJsonProtocol {
    implicit val luceneSuccessFormat = jsonFormat1(EpitweetrActor.Success.apply)
    implicit val luceneFailureFormat = jsonFormat1(EpitweetrActor.Failure.apply)
    implicit val datesProcessedFormat = jsonFormat2(LuceneActor.DatesProcessed.apply)
    implicit val periodResponseFormat = jsonFormat3(LuceneActor.PeriodResponse.apply)
    implicit val commitRequestFormat = jsonFormat0(LuceneActor.CommitRequest.apply)
    implicit val locationRequestFormat = jsonFormat7(Location.apply)
    implicit val geolocatedTweetFormat = jsonFormat10(GeolocatedTweet.apply)
    implicit val aggregationFormat = jsonFormat6(Aggregation.apply)
    implicit val collectionFormat = jsonFormat5(Collection.apply)
    implicit val userV2Format = jsonFormat5(UserV2.apply)
    implicit val metaV2Format = jsonFormat3(MetaV2.apply)
    implicit val geoJsonV2Format = jsonFormat2(GeoJson.apply)
    implicit val placeV2Format = jsonFormat8(PlaceV2.apply)
    implicit val coordinatesV2Format = jsonFormat2(CoordinatesV2.apply)
    implicit val placeRefV2Format = jsonFormat2(PlaceRefV2.apply)
    implicit val tweetRefV2Format = jsonFormat2(TweetRefV2.apply)
    implicit val urlV2 =  jsonFormat2(UrlV2.apply)
    implicit val hashTagV2 =  jsonFormat1(HashTagV2.apply)
    implicit val entitiesV2 =  jsonFormat2(EntitiesV2.apply)
    implicit val annotatedItemV2 =  jsonFormat2(AnnotatedItemV2.apply)
    implicit val annotationV2 =  jsonFormat2(AnnotationV2.apply)
    implicit val tweetV2Format = jsonFormat10(TweetV2.apply)
    implicit val includesV2Format = jsonFormat3(IncludesV2.apply)
    implicit val tweetsV2Format = jsonFormat3(TweetsV2.apply)
    implicit val textToGeoFormat = jsonFormat3(TextToGeo.apply)
    implicit val taggedAlertsFormat = jsonFormat12(TaggedAlert.apply)
    implicit val alertRunFormat = jsonFormat15(AlertRun.apply)
    implicit val alertClassificationFormat = jsonFormat2(AlertClassification.apply)


    implicit object tweetV1Format extends RootJsonFormat[TweetV1] {
      def write(t: TweetV1) =
        JsObject(Map(
          "tweet_id" -> JsNumber(t.tweet_id), 
          "text" -> JsString(t.text), 
          "linked_text" -> t.linked_text.map(t => JsString(t)).getOrElse(JsNull),
          "user_description"-> JsString(t.user_description), 
          "linked_user_description"-> t.linked_user_description.map(t => JsString(t)).getOrElse(JsNull),
          "is_retweet" -> JsBoolean(t.is_retweet),
          "screen_name" -> JsString(t.screen_name),
          "user_name" -> JsString(t.user_name), 
          "user_id" -> JsNumber(t.user_id), 
          "user_location" -> JsString(t.user_location),
          "linked_user_name" -> t.linked_user_name.map(t => JsString(t)).getOrElse(JsNull), 
          "linked_screen_name" -> t.linked_screen_name.map(t => JsString(t)).getOrElse(JsNull), 
          "linked_user_location" -> t.linked_user_location.map(t => JsString(t)).getOrElse(JsNull), 
          "created_at" -> JsString(t.created_at.toString), 
          "lang" -> JsString(t.lang), 
          "linked_lang" -> t.linked_lang.map(t => JsString(t)).getOrElse(JsNull), 
          "tweet_longitude" -> t.tweet_longitude.map(t => JsNumber(t)).getOrElse(JsNull), 
          "tweet_latitude" -> t.tweet_latitude.map(t => JsNumber(t)).getOrElse(JsNull), 
          "linked_longitude" -> t.linked_longitude.map(t => JsNumber(t)).getOrElse(JsNull),
          "linked_latitude" -> t.linked_latitude.map(t => JsNumber(t)).getOrElse(JsNull), 
          "place_type" -> t.place_type.map(t => JsString(t)).getOrElse(JsNull), 
          "place_name" -> t.place_name.map(t => JsString(t)).getOrElse(JsNull), 
          "place_full_name" ->t.place_full_name.map(t => JsString(t)).getOrElse(JsNull), 
          "linked_place_full_name" ->t.linked_place_full_name.map(t => JsString(t)).getOrElse(JsNull), 
          "place_country_code" -> t.place_country_code.map(t => JsString(t)).getOrElse(JsNull), 
          "place_country" -> t.place_country.map(t => JsString(t)).getOrElse(JsNull),
          "place_longitude" ->t.place_longitude.map(t => JsNumber(t)).getOrElse(JsNull), 
          "place_latitude" -> t.place_latitude.map(t => JsNumber(t)).getOrElse(JsNull),
          "linked_place_longitude" -> t.linked_place_longitude.map(t => JsNumber(t)).getOrElse(JsNull), 
          "linked_place_latitude" -> t.linked_place_latitude.map(t => JsNumber(t)).getOrElse(JsNull),
          "hashtags" -> t.hashtags.map(tags => JsArray(tags.map(t => JsString(t)):_*)).getOrElse(JsNull), 
          "urls" -> t.urls.map(urs => JsArray(urs.map(t => JsString(t)):_*)).getOrElse(JsNull), 
          "contexts" -> t.contexts.map(conts => JsArray(conts.map(t => JsString(t)):_*)).getOrElse(JsNull), 
          "entities" -> t.entities.map(ents => JsArray(ents.map(t => JsString(t)):_*)).getOrElse(JsNull)
      ))
      
      val twitterDateFormat = "EEE MMM dd HH:mm:ss Z yyyy"
      val twitterDateParser = DateTimeFormatter.ofPattern(twitterDateFormat).withLocale(Locale.US)
      def read(value: JsValue) = {
        def linkedFields(fields:Map[String, JsValue]) = 
          fields.get("retweeted_status")
            .orElse(fields.get("quoted_status"))
             .map(lt => lt.asInstanceOf[JsObject].fields)
        def userFields(fields:Map[String, JsValue]) = fields("user").asInstanceOf[JsObject].fields
        def linkedUserFields(fields:Map[String, JsValue]) = linkedFields(fields).map(lf => lf("user").asInstanceOf[JsObject].fields)
        def coordinates(fields:Map[String, JsValue]) = fields("coordinates") match {
          case JsObject(cf) => cf.get("coordinates").map(c => c.asInstanceOf[JsArray].elements)
          case JsNull => None
          case o => throw new Exception(f"Unexpected type for coordinates ${o.getClass.getName}")
        }
        def longitude(fields:Map[String, JsValue]) = coordinates(fields).map(p => p(0).asInstanceOf[JsNumber].value.floatValue)
        def latitude(fields:Map[String, JsValue]) = coordinates(fields).map(p => p(1).asInstanceOf[JsNumber].value.floatValue)
        def placeFields(fields:Map[String, JsValue]) = fields("place") match {
          case JsObject(cf) => Some(cf)
          case JsNull => None
          case o => throw new Exception(f"Unexpected type for place ${o.getClass.getName}")
        }        
        def bboxAvg(placeFields:Map[String, JsValue], i:Int) = {
          placeFields("bounding_box") match {
            case JsObject(bb) => 
              val baseArray = bb("coordinates").asInstanceOf[JsArray].elements(0).asInstanceOf[JsArray].elements
              Some(
                (
                baseArray(0).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
                + baseArray(1).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
                + baseArray(2).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
                + baseArray(3).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
                )/4
              )
            case JsNull => None
            case o => throw new Exception(f"Unexpected type for place ${o.getClass.getName}")
          }
        }
        def fullPlace(placeFields:Map[String, JsValue]) = {
          if(placeFields("country_code").asInstanceOf[JsString].value== "US")
             s"${placeFields("full_name").asInstanceOf[JsString].value}  ${placeFields("country").asInstanceOf[JsString].value}"
          else
             placeFields("full_name").asInstanceOf[JsString].value
        }
        value match {
          case JsObject(fields) =>
            if(fields.contains("id_str")) //API File
              TweetV1(
                tweet_id= fields("id_str").asInstanceOf[JsString].value.toLong, 
                text= fields("text").asInstanceOf[JsString].value, 
                linked_text= linkedFields(fields).map(lf => lf("text").asInstanceOf[JsString].value),
                user_description = userFields(fields)("description").asInstanceOf[JsString].value, 
                linked_user_description= linkedUserFields(fields).map(luf => luf("description").asInstanceOf[JsString].value),
                is_retweet = !fields.get("retweeted_status").isEmpty,
                screen_name= userFields(fields)("screen_name").asInstanceOf[JsString].value,
                user_name= userFields(fields)("name").asInstanceOf[JsString].value,
                user_id = userFields(fields)("id_str").asInstanceOf[JsString].value.toLong,
                user_location= userFields(fields)("location").asInstanceOf[JsString].value,
                linked_user_name= linkedUserFields(fields).map(luf => luf("name").asInstanceOf[JsString].value), 
                linked_screen_name= linkedUserFields(fields).map(luf => luf("screen_name").asInstanceOf[JsString].value), 
                linked_user_location= linkedUserFields(fields).map(luf => luf("location").asInstanceOf[JsString].value), 
                created_at= Instant.from(twitterDateParser.parse(fields("created_at").asInstanceOf[JsString].value)), 
                lang = fields("lang").asInstanceOf[JsString].value, 
                linked_lang= linkedFields(fields).map(lf => lf("lang").asInstanceOf[JsString].value), 
                tweet_longitude= longitude(fields), 
                tweet_latitude= latitude(fields), 
                linked_longitude = linkedFields(fields).map(lf => longitude(lf)).flatten,
                linked_latitude=  linkedFields(fields).map(lf => latitude(lf)).flatten, 
                place_type= Try(placeFields(fields).map(pf => pf("place_type").asInstanceOf[JsString].value)).recover{case e => throw new Exception(s"Cannot get place_type from $fields")}.get, 
                place_name= placeFields(fields).map(pf => pf("name").asInstanceOf[JsString].value), 
                place_full_name = placeFields(fields).map(pf => fullPlace(pf)), 
                linked_place_full_name=  linkedFields(fields).map(lf => placeFields(lf).map(pf => fullPlace(pf))).flatten, 
                place_country_code= placeFields(fields).map(pf => pf("country_code").asInstanceOf[JsString].value), 
                place_country= placeFields(fields).map(pf => pf("country").asInstanceOf[JsString].value),
                place_longitude= placeFields(fields).map(pf => bboxAvg(pf, 0)).flatten, 
                place_latitude= placeFields(fields).map(pf => bboxAvg(pf, 1)).flatten,
                linked_place_longitude = linkedFields(fields).map(lf => placeFields(lf).map(pf => bboxAvg(pf, 0)).flatten).flatten, 
                linked_place_latitude = linkedFields(fields).map(lf => placeFields(lf).map(pf => bboxAvg(pf, 1)).flatten).flatten,
                hashtags = fields.get("entities").flatMap{
                  case JsObject(entFields) =>
                    Some(entFields.get("hashtags") match {
                      case Some(JsArray(tags)) => 
                        tags.toSeq.flatMap(tag => 
                          (tag match {
                            case JsObject(tagFields) =>
                              tagFields.get("text") match {
                                case Some(JsString(value)) => Some(value)
                                case _ => None
                              }
                            case _ => None
                          }).toSeq
                        )
                      case _ => Seq[String]()
                    })
                  case _ => None
                }, 
                urls = fields.get("entities").flatMap{
                  case JsObject(entFields) =>
                    Some(entFields.get("urls") match {
                      case Some(JsArray(urls)) => 
                        (urls.toSeq.flatMap(url => 
                          url match {
                            case JsObject(urlFields) =>
                              urlFields.get("expanded_url") match {
                                case Some(JsString(value)) => Some(value)
                                case _ => None
                              }
                            case _ => None
                          }).toSeq
                        )
                      case _ => Seq[String]()
                    })
                  case _ => None
                }, 
                contexts = None, 
                entities = None
              )
            else // Object comming from serialized json
              TweetV1(
                tweet_id= fields("tweet_id").asInstanceOf[JsNumber].value.toLong, 
                text= fields("text").asInstanceOf[JsString].value, 
                linked_text=  fields.get("linked_text").map(f=> if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten,
                user_description = fields("user_description").asInstanceOf[JsString].value, 
                linked_user_description= fields.get("linked_user_description").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten,
                is_retweet = fields("is_retweet").asInstanceOf[JsBoolean].value,
                screen_name= fields("screen_name").asInstanceOf[JsString].value,
                user_name= fields("user_name").asInstanceOf[JsString].value,
                user_id = fields("user_id").asInstanceOf[JsNumber].value.toLong,
                user_location= fields("user_location").asInstanceOf[JsString].value,
                linked_user_name= fields.get("linked_user_name").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                linked_screen_name= fields.get("linked_screen_name").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                linked_user_location= fields.get("linked_user_location").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                created_at= Instant.parse(fields("created_at").asInstanceOf[JsString].value), 
                lang = fields("lang").asInstanceOf[JsString].value, 
                linked_lang= fields.get("linked_lang").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                tweet_longitude= fields.get("tweet_longitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten, 
                tweet_latitude= fields.get("tweet_latitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten, 
                linked_longitude = fields.get("linked_longitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten,
                linked_latitude=  fields.get("linked_latitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten, 
                place_type=  fields.get("place_type").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                place_name=  fields.get("place_name").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                place_full_name =  fields.get("place_full_name").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                linked_place_full_name=  fields.get("linked_place_full_name").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                place_country_code=  fields.get("place_country_code").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten, 
                place_country=  fields.get("place_country").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsString].value)).flatten,
                place_longitude= fields.get("place_longitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten, 
                place_latitude= fields.get("place_latitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten,
                linked_place_longitude = fields.get("linked_place_longitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten, 
                linked_place_latitude =fields.get("linked_placee_latitude").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsNumber].value.toFloat)).flatten,
                hashtags = fields.get("hashtags").map(f => if(f == JsNull) None else 
                  f match {
                    case JsArray(elements) => Some(elements.toSeq.map(v => v.asInstanceOf[JsString].value))
                    case f => throw new Exception(s"Unexpected type on Tweet Json hashtags (v1) $f")
                  }
                ).flatten, 
                urls = fields.get("urls").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsArray].elements.toSeq.map(v => v.asInstanceOf[JsString].value))).flatten, 
                contexts = fields.get("contexts").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsArray].elements.toSeq.map(v => v.asInstanceOf[JsString].value))).flatten, 
                entities = fields.get("entities").map(f => if(f == JsNull) None else Some(f.asInstanceOf[JsArray].elements.toSeq.map(v => v.asInstanceOf[JsString].value))).flatten
              )
          case _ =>
            throw new Exception(s"@epi cannot deserialize $value to TweetV1")
        }
      }
    }
    implicit object textsToGeoFormat extends RootJsonFormat[TextsToGeo] {
      def write(t: TextsToGeo) = JsArray(t.items.map(c => textToGeoFormat.write(c)):_*)
      def read(value: JsValue) = value match {
        case JsArray(items) => TextsToGeo(items = items.map(t => textToGeoFormat.read(t)).toSeq)
        case JsObject(fields) =>
          fields.get("items") match {
            case Some(JsArray(items)) => TextsToGeo(items = items.map(t => textToGeoFormat.read(t)).toSeq)
            case _ => throw new Exception("@epi cannot find expected items field to get to geos")
          }
        case _ => throw new Exception(s"@epi cannot deserialize $value to ToGeos")
      }
    }
    implicit object geoTrainingsFormat extends RootJsonFormat[GeoTrainings] {
      def write(t: GeoTrainings) = JsArray(t.items.map(c => geoTrainingFormat.write(c)):_*)
      def read(value: JsValue) = value match {
        case JsArray(items) => GeoTrainings(items = items.map(t => geoTrainingFormat.read(t)).toSeq)
        case JsObject(fields) =>
          fields.get("items") match {
            case Some(JsArray(items)) => GeoTrainings(items = items.map(t => geoTrainingFormat.read(t)).toSeq)
            case _ => throw new Exception("@epi cannot find expected items field to get geoTrainingd")
          }
        case _ => throw new Exception(s"@epi cannot deserialize $value to GeoTrainings")
      }
    }
    implicit object geoTrainingFormat extends RootJsonFormat[GeoTraining] {
      def write(t: GeoTraining) = throw new NotImplementedError("Writing GeoTraining is as json is not implemented")
      def read(value: JsValue) = {
        value match {
          case JsObject(fields) =>
              GeoTraining(
                category = fields("Type").asInstanceOf[JsString].value,  
                text = fields("Text").asInstanceOf[JsString].value,  
                locationInText = fields.get("Location in text").map(v => v.asInstanceOf[JsString].value), 
                isLocation = fields.get("Location yes/no")
                  .map(v => v.asInstanceOf[JsString].value.toLowerCase match { 
                    case "ok" => Some(true)
                    case "ko" => Some(false)
                    case "yes" => Some(true)
                    case "no" => Some(false)
                    case _ => None
                  })
                  .flatten,  
                forcedLocationCode = fields.get("Associate country code").map(v => v.asInstanceOf[JsString].value), 
                forcedLocationName =fields.get("Associate with").map(v => v.asInstanceOf[JsString].value) , 
                source = fields.get("Source")
                  .map(v => v.asInstanceOf[JsString].value)
                  .map(v => GeoTrainingSource(v))
                  .getOrElse(GeoTrainingSource.manual), 
                tweetId = fields.get("Tweet Id").map(v => v.asInstanceOf[JsString].value),
                lang = fields.get("Lang")
                  .map(v => v.asInstanceOf[JsString].value.toLowerCase)
                  .map{
                    case null => None
                    case "all" => None
                    case v => Some(v)
                  }.flatten,
                tweetPart = fields.get("Tweet part")
                  .map(v => v.asInstanceOf[JsString].value)
                  .map(v => GeoTrainingPart(v)), 
                foundLocation = fields.get("Epitweetr match").map(v => v.asInstanceOf[JsString].value) , 
                foundLocationCode = fields.get("Epitweetr country match").map(v => v.asInstanceOf[JsString].value), 
                foundCountryCode = fields.get("Epitweetr country code match").map(v => v.asInstanceOf[JsString].value)
              )
          case _ =>
            throw new Exception(s"@epi cannot deserialize $value to GeoTraining")
        }
      }
    }
    implicit object tweetsV1Format extends RootJsonFormat[TweetsV1] {
      def write(t: TweetsV1) = JsArray(t.items.map(c => tweetV1Format.write(c)):_*)
      def read(value: JsValue) = value match {
        case JsArray(items) => TweetsV1(items = items.map(t => tweetV1Format.read(t)).toSeq)
        case JsObject(fields) =>
          (fields.get("statuses"), fields.get("data"), fields.get("meta")) match {
            case (Some(JsArray(items)), _, _) => TweetsV1(items = items.map(t => tweetV1Format.read(t)).toSeq)
            case (None, Some(JsArray(tweets)), _)  => tweetsV2Format.read(value).toV1
            case (None, None, Some(_))  =>  TweetsV1(items = Seq[TweetV1]()) 
            case _ => throw new Exception("@epi cannot find expected statuses array on search Json result")
          }
        case _ => throw new Exception(s"@epi cannot deserialize $value to TweetsV1")
      }
    }
    implicit val topicTweetsFormat = jsonFormat2(TopicTweetsV1.apply)
    implicit object luceneDocFormat extends RootJsonFormat[Document] {
      def write(doc: Document) = writeField(fields = doc.getFields.asScala.toSeq, totalCount = None, transform  = Map[String, (String, String)](), asArray =Set[String]())
      def customWrite(
        doc: Document, 
        forceString:Set[String]=Set[String](), 
        totalCount:Option[Long]=None, 
        transform:Map[String, (String, String)] = Map[String, (String, String)](), 
        asArray:Set[String]//=Set[String]()
      ) = writeField(fields = doc.getFields.asScala.toSeq, forceString=forceString, totalCount = totalCount, transform = transform, asArray = asArray)
      def writeField(
        fields:Seq[IndexableField], 
        forceString:Set[String] = Set[String](), 
        path:Option[String] = None, 
        totalCount:Option[Long] = None, 
        transform:Map[String, (String, String)],
        asArray:Set[String]
      ):JsValue = {
        val (baseFields, childrenNames) = path match {
          case Some(p) => (
            fields.filter(f => f.name.startsWith(p) && !f.name.substring(p.size).contains(".")), 
            fields
              .map(_.name)
              .filter(n => n.startsWith(p) && n.substring(p.size).contains("."))
              .map(n => n.substring(p.size).split("\\.")(0))
              .distinct
          ) 
          case None => (
            fields.filter(f => !f.name.contains(".")), 
            fields
              .map(_.name)
              .filter(n => n.contains("."))
              .map(n => n.split("\\.")(0))
              .distinct

          )
        }
        JsObject(Map(
          (baseFields.map{f =>
            val fname = f.name.substring(path.getOrElse("").size)
            if(asArray.contains(f.name) && f.stringValue != null)
              (fname, JsArray(f.stringValue.split("\n").map(v => JsString(transform.get(fname).map{case (s, r) =>v.replaceAll(s, r)}.getOrElse(v))):_*))
            else if(forceString.contains(f.name) && f.stringValue != null)
              (fname, JsString(transform.get(fname).map{case (s, r) =>f.stringValue.replaceAll(s, r)}.getOrElse(f.stringValue)))
            else if(f.numericValue != null && !forceString.contains(f.name))
              (fname, f.numericValue match {
                case v:java.lang.Integer => JsNumber(v)
                case v:java.lang.Float => JsNumber(v)
                case v:java.lang.Double => JsNumber(v)
                case v:java.lang.Long => JsNumber(v)
                case v:java.math.BigDecimal => JsNumber(BigDecimal(v.toString))
                case _ => throw new NotImplementedError("I do not know how convert ${v.getClass.getName} into JsNumber")
              })
            else if(f.stringValue != null)
              (fname, JsString(transform.get(fname).map{case (s, r) =>f.stringValue.replaceAll(s, r)}.getOrElse(f.stringValue)))
            else if(f.binaryValue != null && f.binaryValue.length == 1)
              (fname, JsBoolean(f.binaryValue.bytes(0) == 1.toByte))
            else 
              throw new NotImplementedError(f"Serializing lucene field is only supported for boolean, number and string so far and got $f")
          } ++
            childrenNames.map(c => (c, writeField(fields = fields, path = Some(f"${path.map(p => p+".").getOrElse("")}$c."), totalCount = None, transform = transform, asArray = asArray))) ++
            (if(path.isEmpty && !totalCount.isEmpty) Seq("totalCount" -> JsNumber(totalCount.get)) else Seq[(String, JsNumber)]())
          ):_*
        ))
      }
      def read(value: JsValue) = {
        throw new NotImplementedError(f"Deserializing lucene documebts is not supported") 
      }
    }
    implicit object geolocatedTweetsFormat extends RootJsonFormat[GeolocatedTweets] {
      def write(t: GeolocatedTweets) = JsArray(t.items.map(c => geolocatedTweetFormat.write(c)):_*)
      def read(value: JsValue) = value match {
        case JsArray(items) => GeolocatedTweets(items = items.map(t => geolocatedTweetFormat.read(t)).map(g => g.fixTopic).toSeq)
        case JsObject(fields) =>
          fields.get("items") match {
            case Some(JsArray(items)) => GeolocatedTweets(items = items.map(t => geolocatedTweetFormat.read(t)).map(g => g.fixTopic).toSeq)
            case _ => throw new Exception("@epi cannot find expected itmes array on search Json result")
          }
        case _ => throw new Exception(s"@epi cannot deserialize $value to GeolocatedTweets")
      }
    }
    implicit object topicKeyWordsFormat extends RootJsonFormat[TopicKeyWords] {
      def write(t: TopicKeyWords) = JsObject(t.items.map{case (k, s) => new JsField(k, JsArray(s.toSeq.map(v => JsString(v)):_*))})
      def read(value: JsValue) = value match {
        case JsObject(fields) =>
          TopicKeyWords(
            fields.map{
              case (name, JsArray(items)) => 
                (name, 
                  items.map{ 
                    case JsString(value) => value
                    case e => throw new Exception(s"@epi1 TopicKeyWords is expected to have only a map of string sets, but got ${e}")
                  }.toSet
                )
              case (name, JsString(value)) => 
                (name, Set(value))
              case e => throw new Exception(s"@epi2 TopicKeyWords is expected to have only a map of string sets, but got ${e}")
            }
            .toMap
          )
        case e => throw new Exception(s"@epi3 TopicKeyWords is expected to have only a map of string sets, but got ${e}")
      }
    }
    implicit object forcedGeoFormat extends RootJsonFormat[ForcedGeo] {
      def write(t: ForcedGeo) = JsObject(t.items.map{case (k, v) => new JsField(k, JsString(v))})
      def read(value: JsValue) = value match {
        case JsObject(fields) =>
          ForcedGeo(
            fields.map{
              case (name, JsString(value)) => (name, value)
              case _ => throw new Exception("@epi ForcedGeo is expected to have only a map of values")
            }
            .toMap
          )
        case _ => throw new Exception("@epi ForcedGeo is expected to have only a map of values")
      }
    }
    implicit object forcedGeoCodesFormat extends RootJsonFormat[ForcedGeoCodes] {
      def write(t: ForcedGeoCodes) = JsObject(t.items.map{case (k, v) => new JsField(k, JsString(v))})
      def read(value: JsValue) = value match {
        case JsObject(fields) =>
          ForcedGeoCodes(
            fields.map{
              case (name, JsString(value)) => (name, value)
              case e => throw new Exception(s"@epi ForcedGeoCodes is expected to have only a map of values but got ${e}")
            }
            .toMap
          )
        case e => throw new Exception(s"@epi ForcedGeo is expected to have only a map of values, but got ${e}")
      }
    }
}

