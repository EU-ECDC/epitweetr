package org.ecdc.epitweetr.fs

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{JsString, JsNull, JsValue, JsNumber, DefaultJsonProtocol, JsonFormat, RootJsonFormat, JsObject, JsArray, JsBoolean}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale 
import scala.util.Try

case class TweetV1(tweet_id:Long, text:String, linked_text:Option[String],user_description:String, linked_user_description:Option[String],
             is_retweet:Boolean, screen_name:String, user_name:String, user_id:Long , user_location:String, linked_user_name:Option[String], linked_screen_name:Option[String], 
             linked_user_location:Option[String], created_at:Instant, lang:String, linked_lang:Option[String], tweet_longitude:Option[Float], tweet_latitude:Option[Float], 
             linked_longitude:Option[Float], linked_latitude:Option[Float], place_type:Option[String], place_name:Option[String], place_full_name:Option[String], 
             linked_place_full_name:Option[String], place_country_code:Option[String], place_country:Option[String], 
             place_longitude:Option[Float], place_latitude:Option[Float],linked_place_longitude:Option[Float], linked_place_latitude:Option[Float])

case class TweetsV1(items:Seq[TweetV1])

object EpiSerialisation
  extends SprayJsonSupport
    with DefaultJsonProtocol {
    implicit val luceneSuccessFormat = jsonFormat1(LuceneActor.Success.apply)
    implicit val luceneFailureFormat = jsonFormat1(LuceneActor.Failure.apply)
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
          "linked_place_latitude" -> t.linked_place_latitude.map(t => JsNumber(t)).getOrElse(JsNull)
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
          val baseArray = placeFields("bounding_box").asInstanceOf[JsObject].fields("coordinates").asInstanceOf[JsArray].elements(0).asInstanceOf[JsArray].elements
          (
          baseArray(0).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
          + baseArray(1).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
          + baseArray(2).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
          + baseArray(3).asInstanceOf[JsArray].elements(i).asInstanceOf[JsNumber].value.floatValue
          )/4
        }
        def fullPlace(placeFields:Map[String, JsValue]) = {
          if(placeFields("country_code").asInstanceOf[JsString].value== "US")
             s"${placeFields("full_name").asInstanceOf[JsString].value}  ${placeFields("country").asInstanceOf[JsString].value}"
          else
             placeFields("full_name").asInstanceOf[JsString].value
        }
        value match {
          case JsObject(fields) =>
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
              place_longitude= placeFields(fields).map(pf => bboxAvg(pf, 0)), 
              place_latitude= placeFields(fields).map(pf => bboxAvg(pf, 1)),
              linked_place_longitude = linkedFields(fields).map(lf => placeFields(lf).map(pf => bboxAvg(pf, 0))).flatten, 
              linked_place_latitude = linkedFields(fields).map(lf => placeFields(lf).map(pf => bboxAvg(pf, 1))).flatten
            )
          case _ =>
            throw new Exception(s"@epi cannot deserialize $value to TweetV1")
        }
      }
    }
    implicit object tweetsV1Format extends RootJsonFormat[TweetsV1] {
      def write(t: TweetsV1) = JsArray(t.items.map(c => tweetV1Format.write(c)):_*)
      def read(value: JsValue) = value match {
        case JsArray(items) => TweetsV1(items = items.map(t => tweetV1Format.read(t)).toSeq)
        case JsObject(fields) =>
          fields.get("statuses") match {
            case Some(JsArray(items)) => TweetsV1(items = items.map(t => tweetV1Format.read(t)).toSeq)
            case _ => throw new Exception("@epi cannot find ewpected statuses array on search Json result")
          }
        case _ => throw new Exception(s"@epi cannot deserialize $value to TweetsV1")
      }
    }
}

