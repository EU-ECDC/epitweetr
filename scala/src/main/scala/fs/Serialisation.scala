package org.ecdc.epitweetr.fs

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{JsString, JsNull, JsValue, JsNumber, DefaultJsonProtocol, JsonFormat, RootJsonFormat, JsObject, JsArray, JsBoolean}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale 
import scala.util.Try
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField}
import org.apache.lucene.index.IndexableField
import scala.collection.JavaConverters._
import java.net.URLDecoder

case class TweetV1(tweet_id:Long, text:String, linked_text:Option[String],user_description:String, linked_user_description:Option[String],
             is_retweet:Boolean, screen_name:String, user_name:String, user_id:Long , user_location:String, linked_user_name:Option[String], linked_screen_name:Option[String], 
             linked_user_location:Option[String], created_at:Instant, lang:String, linked_lang:Option[String], tweet_longitude:Option[Float], tweet_latitude:Option[Float], 
             linked_longitude:Option[Float], linked_latitude:Option[Float], place_type:Option[String], place_name:Option[String], place_full_name:Option[String], 
             linked_place_full_name:Option[String], place_country_code:Option[String], place_country:Option[String], 
             place_longitude:Option[Float], place_latitude:Option[Float],linked_place_longitude:Option[Float], linked_place_latitude:Option[Float])


case class Location(geo_code:String, geo_country_code:String,geo_id:Long, geo_latitude:Double, geo_longitude:Double, geo_name:String, geo_type:String)

case class Geolocated(var topic:String, id:Long, is_geo_located:Boolean, lang:String, linked_place_full_name_loc:Option[Location], linked_text_loc:Option[Location], 
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

case class Geolocateds(items:Seq[Geolocated])

case class TweetsV1(items:Seq[TweetV1])
case class TopicTweetsV1(topic:String, tweets:TweetsV1)

object EpiSerialisation
  extends SprayJsonSupport
    with DefaultJsonProtocol {
    implicit val luceneSuccessFormat = jsonFormat1(LuceneActor.Success.apply)
    implicit val luceneFailureFormat = jsonFormat1(LuceneActor.Failure.apply)
    implicit val datesProcessedFormat = jsonFormat2(LuceneActor.DatesProcessed.apply)
    implicit val commitRequestFormat = jsonFormat0(LuceneActor.CommitRequest.apply)
    implicit val locationRequestFormat = jsonFormat7(Location.apply)
    implicit val geolocatedFormat = jsonFormat10(Geolocated.apply)
    implicit val aggregationFormat = jsonFormat6(Aggregation.apply)
    implicit val collectionFormat = jsonFormat5(Collection.apply)
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
                place_longitude= placeFields(fields).map(pf => bboxAvg(pf, 0)), 
                place_latitude= placeFields(fields).map(pf => bboxAvg(pf, 1)),
                linked_place_longitude = linkedFields(fields).map(lf => placeFields(lf).map(pf => bboxAvg(pf, 0))).flatten, 
                linked_place_latitude = linkedFields(fields).map(lf => placeFields(lf).map(pf => bboxAvg(pf, 1))).flatten
              )
            else // Object comming from serialized json
              TweetV1(
                tweet_id= fields("tweet_id").asInstanceOf[JsNumber].value.toLong, 
                text= fields("text").asInstanceOf[JsString].value, 
                linked_text=  fields.get("linked_text").map(f=> f.asInstanceOf[JsString].value),
                user_description = fields("user_description").asInstanceOf[JsString].value, 
                linked_user_description= fields.get("linked_user_description").map(f => f.asInstanceOf[JsString].value),
                is_retweet = fields("is_retweet").asInstanceOf[JsBoolean].value,
                screen_name= fields("screen_name").asInstanceOf[JsString].value,
                user_name= fields("user_name").asInstanceOf[JsString].value,
                user_id = fields("user_id").asInstanceOf[JsNumber].value.toLong,
                user_location= fields("user_location").asInstanceOf[JsString].value,
                linked_user_name= fields.get("linked_user_name").map(f => f.asInstanceOf[JsString].value), 
                linked_screen_name= fields.get("linked_screen_name").map(f => f.asInstanceOf[JsString].value), 
                linked_user_location= fields.get("linked_user_location").map(f => f.asInstanceOf[JsString].value), 
                created_at= Instant.parse(fields("created_at").asInstanceOf[JsString].value), 
                lang = fields("lang").asInstanceOf[JsString].value, 
                linked_lang= fields.get("linked_lang").map(f => f.asInstanceOf[JsString].value), 
                tweet_longitude= fields.get("tweet_longitude").map(f => f.asInstanceOf[JsNumber].value.toFloat), 
                tweet_latitude= fields.get("tweet_latitude").map(f => f.asInstanceOf[JsNumber].value.toFloat), 
                linked_longitude = fields.get("linked_longitude").map(f => f.asInstanceOf[JsNumber].value.toFloat),
                linked_latitude=  fields.get("linked_latitude").map(f => f.asInstanceOf[JsNumber].value.toFloat), 
                place_type=  fields.get("place_type").map(f => f.asInstanceOf[JsString].value), 
                place_name=  fields.get("place_name").map(f => f.asInstanceOf[JsString].value), 
                place_full_name =  fields.get("place_full_name").map(f => f.asInstanceOf[JsString].value), 
                linked_place_full_name=  fields.get("linked_place_full_name").map(f => f.asInstanceOf[JsString].value), 
                place_country_code=  fields.get("place_country_code").map(f => f.asInstanceOf[JsString].value), 
                place_country=  fields.get("place_country").map(f => f.asInstanceOf[JsString].value),
                place_longitude= fields.get("place_longitude").map(f => f.asInstanceOf[JsNumber].value.toFloat), 
                place_latitude= fields.get("place_latitude").map(f => f.asInstanceOf[JsNumber].value.toFloat),
                linked_place_longitude = fields.get("linked_place_longitude").map(f => f.asInstanceOf[JsNumber].value.toFloat), 
                linked_place_latitude =fields.get("linked_placee_latitude").map(f => f.asInstanceOf[JsNumber].value.toFloat)
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
            case _ => throw new Exception("@epi cannot find expected statuses array on search Json result")
          }
        case _ => throw new Exception(s"@epi cannot deserialize $value to TweetsV1")
      }
    }
    implicit val topicTweetsFormat = jsonFormat2(TopicTweetsV1.apply)
    implicit object luceneDocFormat extends RootJsonFormat[Document] {
      def write(doc: Document) = writeField(fields = doc.getFields.asScala.toSeq)
      def writeField(fields:Seq[IndexableField], path:Option[String] = None):JsValue = {
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
            if(f.numericValue != null)
              (fname, f.numericValue match {
                case v:java.lang.Integer => JsNumber(v)
                case v:java.lang.Float => JsNumber(v)
                case v:java.lang.Double => JsNumber(v)
                case v:java.lang.Long => JsNumber(v)
                case v:java.math.BigDecimal => JsNumber(BigDecimal(v.toString))
                case _ => throw new NotImplementedError("I do not know how convert ${v.getClass.getName} into JsNumber")
              })
            else if(f.stringValue != null)
              (fname, JsString(f.stringValue))
            else if(f.binaryValue != null && f.binaryValue.length == 1)
              (fname, JsBoolean(f.binaryValue.bytes(0) == 1.toByte))
            else 
              throw new NotImplementedError(f"Serializing lucene field is only supported for boolean, number and string so far and got $f")
          } ++
            childrenNames.map(c => (c, writeField(fields = fields, path = Some(f"${path.map(p => p+".").getOrElse("")}$c."))))
          ):_*
        ))
      }
      def read(value: JsValue) = {
        throw new NotImplementedError(f"Deserializing lucene documebts is not supported") 
      }
    }
    implicit object geolocatedsFormat extends RootJsonFormat[Geolocateds] {
      def write(t: Geolocateds) = JsArray(t.items.map(c => geolocatedFormat.write(c)):_*)
      def read(value: JsValue) = value match {
        case JsArray(items) => Geolocateds(items = items.map(t => geolocatedFormat.read(t)).map(g => g.fixTopic).toSeq)
        case JsObject(fields) =>
          fields.get("items") match {
            case Some(JsArray(items)) => Geolocateds(items = items.map(t => geolocatedFormat.read(t)).map(g => g.fixTopic).toSeq)
            case _ => throw new Exception("@epi cannot find expected itmes array on search Json result")
          }
        case _ => throw new Exception(s"@epi cannot deserialize $value to TweetsV1")
      }
    }
    //implicit val geolocatedCreatedFormat = jsonFormat10(Geolocated.apply)
}

