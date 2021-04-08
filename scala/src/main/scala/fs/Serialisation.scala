package org.ecdc.epitweetr.fs

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{JsString, JsNull, JsValue, JsNumber, DefaultJsonProtocol, JsonFormat, RootJsonFormat, JsObject, JsArray, JsBoolean}
import java.sql.Timestamp
import java.time.{ZonedDateTime, OffsetDateTime, ZoneId}

case class TweetV1(topic:String, tweet_id:Long, text:String, linked_text:String,user_description:String, linked_user_description:String,
             is_retweet:Boolean, screen_name:String, user_name:String, user_id:Long , user_location:String, linked_user_name:String, linked_screen_name:String, 
             linked_user_location:String, created_at:String, lang:String, linked_lang:String, tweet_longitude:Float, tweet_latitude:Float, linked_longitude:Float, linked_latitude:Float,
             place_type:String, place_name:String, place_full_name:String, linked_place_full_name:String, place_country_code:String, place_country:String, place_longitude:Float, place_latitude:Float,
             linked_place_longitude:Float, linked_place_latitude:Float)

case class TweetsV1(items:Seq[TweetV1])

object EpiSerialisation
  extends SprayJsonSupport
    with DefaultJsonProtocol {
    implicit object timeStampFormat extends JsonFormat[Timestamp] {
      def write(t: Timestamp) =
        JsString(ZonedDateTime.ofInstant(t.toInstant(), ZoneId.systemDefault).toOffsetDateTime.toString)

      def read(value: JsValue) = value match {
        case JsString(serialized) =>
          Timestamp.from(OffsetDateTime.parse(serialized).toInstant)
        case JsNull => null.asInstanceOf[Timestamp]
        case _ =>
          throw new Exception(s"@epi cannot deserialize $value to Timestamp")
      }
    }
    implicit val luceneSuccessFormat = jsonFormat1(LuceneActor.Success.apply)
    implicit val luceneFailureFormat = jsonFormat1(LuceneActor.Failure.apply)
    implicit object tweetV1Format extends RootJsonFormat[TweetV1] {
      def write(t: TweetV1) =
        JsObject(Map(
          "topic" -> JsString(t.topic), 
          "tweeyt_id" -> JsNumber(t.tweet_id), 
          "text" -> JsString(t.text), 
          "linked_text" -> JsString(t.linked_text),
          "user_description"-> JsString(t.user_description), 
          "linked_user_description"-> JsString(t.linked_user_description),
          "is_retweet" -> JsBoolean(t.is_retweet),
          "screen_name" -> JsString(t.screen_name),
          "user_name" -> JsString(t.user_name), 
          "user_id" -> JsNumber(t.user_id), 
          "user_location" -> JsString(t.user_location),
          "linked_user_name" -> JsString(t.linked_user_name), 
          "linked_screen_name" -> JsString(t.linked_screen_name), 
          "linked_user_location" -> JsString(t.linked_user_location), 
          "created_at" -> JsString(t.created_at), 
          "lang" -> JsString(t.lang), 
          "linked_lang" -> JsString(t.linked_lang), 
          "tweet_longitude" -> JsNumber(t.tweet_longitude), 
          "tweet_latitude" -> JsNumber(t.tweet_latitude), 
          "linked_longitude" -> JsNumber(t.linked_longitude),
          "linked_latitude" -> JsNumber(t.linked_latitude), 
          "place_type" -> JsString(t.place_type), 
          "place_name" -> JsString(t.place_name), 
          "place_full_name" ->JsString(t.place_full_name), 
          "linked_place_full_name" ->JsString(t.linked_place_full_name), 
          "place_country_code" -> JsString(t.place_country_code), 
          "place_country" -> JsString(t.place_country),
          "place_longitude" ->JsNumber(t.place_longitude), 
          "place_latitude" -> JsNumber(t.place_latitude),
          "linked_place_longitude" -> JsNumber(t.linked_place_longitude), 
          "linked_place_latitude" -> JsNumber(t.linked_place_latitude)
        ))

      def read(value: JsValue) = value match {
        case JsObject(fields) =>
          TweetV1(
            topic= fields.get("topic").get.asInstanceOf[JsString].value, 
            tweet_id= fields.get("tweet_id").get.asInstanceOf[JsNumber].value.toLongExact, 
            text= fields.get("text").get.asInstanceOf[JsString].value, 
            linked_text= fields.get("linked_text").get.asInstanceOf[JsString].value,
            user_description= fields.get("user_description").get.asInstanceOf[JsString].value, 
            linked_user_description= fields.get("linked_user_description").get.asInstanceOf[JsString].value,
            is_retweet = fields.get("is_retweet").get.asInstanceOf[JsBoolean].value,
            screen_name= fields.get("screen_name").get.asInstanceOf[JsString].value,
            user_name= fields.get("user_name").get.asInstanceOf[JsString].value,
            user_id= fields.get("user_id").get.asInstanceOf[JsNumber].value.toLongExact,
            user_location= fields.get("user_location").get.asInstanceOf[JsString].value,
            linked_user_name= fields.get("linked_user_name").get.asInstanceOf[JsString].value, 
            linked_screen_name= fields.get("linked_screen_name").get.asInstanceOf[JsString].value, 
            linked_user_location= fields.get("linked_user_location").get.asInstanceOf[JsString].value, 
            created_at= fields.get("created_at").get.asInstanceOf[JsString].value, 
            lang= fields.get("lang").get.asInstanceOf[JsString].value, 
            linked_lang= fields.get("linked_lang").get.asInstanceOf[JsString].value, 
            tweet_longitude= fields.get("tweet_longitude").get.asInstanceOf[JsNumber].value.floatValue, 
            tweet_latitude= fields.get("tweet_latitude").get.asInstanceOf[JsNumber].value.floatValue, 
            linked_longitude= fields.get("linded_longitude").get.asInstanceOf[JsNumber].value.floatValue,
            linked_latitude= fields.get("linked_latitude").get.asInstanceOf[JsNumber].value.floatValue, 
            place_type= fields.get("place_type").get.asInstanceOf[JsString].value, 
            place_name= fields.get("place_name").get.asInstanceOf[JsString].value, 
            place_full_name= fields.get("place_full_name").get.asInstanceOf[JsString].value, 
            linked_place_full_name= fields.get("linked_place_full_name").get.asInstanceOf[JsString].value, 
            place_country_code= fields.get("place_country_code").get.asInstanceOf[JsString].value, 
            place_country= fields.get("place_country").get.asInstanceOf[JsString].value,
            place_longitude= fields.get("place_longitude").get.asInstanceOf[JsNumber].value.floatValue, 
            place_latitude= fields.get("place_latitude").get.asInstanceOf[JsNumber].value.floatValue,
            linked_place_longitude= fields.get("linked_place_longitude").get.asInstanceOf[JsNumber].value.floatValue, 
            linked_place_latitude= fields.get("linked_place_latitude").get.asInstanceOf[JsNumber].value.floatValue
          )
        case _ =>
          throw new Exception(s"@epi cannot deserialize $value to TweetV1")
      }
    }
    implicit object tweetsV1Format extends RootJsonFormat[TweetsV1] {
      def write(t: TweetsV1) = JsArray(t.items.map(c => tweetV1Format.write(c)):_*)
      def read(value: JsValue) = value match {
        case JsArray(items) => TweetsV1(items = items.map(t => tweetV1Format.read(t)).toSeq)
        case _ => throw new Exception(s"@epi cannot deserialize $value to TweetsV1")
      }
    }
}

