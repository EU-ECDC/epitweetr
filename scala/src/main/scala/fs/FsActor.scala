package org.ecdc.epitweetr.fs

import org.ecdc.twitter.{JavaBridge,  schemas}
import org.ecdc.epitweetr.{Settings}
import akka.pattern.{ask, pipe}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{Source}
import akka.actor.ActorRef
import akka.Done
import akka.util.ByteString
import akka.util.{Timeout}
import java.time.LocalDateTime
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ArrayBuffer}
import java.time.temporal.{IsoFields, ChronoUnit}
import java.time.{Instant, ZoneOffset, LocalDateTime}
import java.nio.file.Paths
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions.{col, udf, input_file_name, explode, coalesce, when, lit, concat, struct, expr, lower}
import demy.storage.{Storage, FSNode}
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField}
import org.apache.lucene.search.{Query, TermQuery, BooleanQuery, PrefixQuery, TermRangeQuery}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.index.Term
import demy.storage.{Storage, FSNode}
import scala.concurrent.ExecutionContext
import org.ecdc.twitter.Geonames.Geolocate
import scala.util.{Try, Success, Failure}


class LuceneActor(conf:Settings) extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val cnf = conf
  def receive = {
    case TopicTweetsV1(topic, ts) =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        val dateMap = ts.items.groupBy(t => t.created_at.toString.take(10))
        dateMap.foreach{case (date, tweets) =>
          val index = LuceneActor.getIndex(tweets(0).created_at)
          tweets.foreach{t => index.indexTweet(t, topic)}
        }
        dateMap.keys.toSeq
      }
      .map{dates =>
        LuceneActor.DatesProcessed("ok", dates)
      }
      .pipeTo(sender())
    case LuceneActor.GeolocateRequest(TopicTweetsV1(topic, ts)) =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        implicit val spark = LuceneActor.getSparkSession()
        implicit val storage = LuceneActor.getSparkStorage
        LuceneActor.add2Geolocate(TopicTweetsV1(topic, ts))  
      }.onComplete {
        case Success(_) => 
        case Failure(t) => println("Error during geolocalisation: " + t.getMessage)
      }
    case LuceneActor.GeolocatedsCreated(Geolocateds(items), dates) =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        val indexes = dates.flatMap(d => LuceneActor.getReadIndexes(d, d))
        items.foreach{g =>
          var found = false 
          for(index <- indexes if !found) {
            found = index.indexGeolocated(g)
          }
        }
        items.size
      }
      .map{c =>
        LuceneActor.Success(s"$c geolocated properly processed")
      }
      .pipeTo(sender())
    case ts:LuceneActor.CommitRequest =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        LuceneActor.commit()
      }
      .map{c =>
        LuceneActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    case LuceneActor.CloseRequest =>
      implicit val holder = LuceneActor.writeHolder
      Future {
        LuceneActor.close()
        LuceneActor.closeSparkSession()
      }
      .map{c =>
        LuceneActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    case LuceneActor.SearchRequest(query, topic, from, to, max, jsonnl, caller) => 
      implicit val timeout: Timeout = 600.seconds //For ask property
      implicit val holder = LuceneActor.readHolder
      Future {
        val indexes = LuceneActor.getReadIndexes(from, to).iterator
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        var first = true
        assert(indexes.isInstanceOf[Iterator[_]])
        indexes
          .flatMap{i => 
            val qb = new BooleanQuery.Builder()
            query.map{q => 
              qb.add(i.parseQuery(q), Occur.MUST)
            }
            qb.add(new TermQuery(new Term("topic", topic)), Occur.MUST) 
            qb.add(TermRangeQuery.newStringRange("created_date", from.toString.take(10), to.toString.take(10), true, true), Occur.MUST) 
            i.searchAll(qb.build)
          }
          .map(doc => EpiSerialisation.luceneDocFormat.write(doc))
          .take(max)
          .foreach{line => 
            Await.result(caller ? ByteString(
              s"${if(!jsonnl && !first) "\n," else if(jsonnl && !first) "\n" else "" }${line.toString}", 
              ByteString.UTF_8
            ), Duration.Inf)
            first = false
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }
    case LuceneActor.AggregateRequest(query, from, to, columns, groupBy, filterBy, sortBy, sourceExpressions, jsonnl, caller) =>
      implicit val timeout: Timeout = 600.seconds //For ask property
      implicit val holder = LuceneActor.readHolder
      val spark = LuceneActor.getSparkSession()
      val sc = spark.sparkContext
      Future {
        val keys = LuceneActor.getReadKeys(from, to).toSeq
        var first = true

        //adding internal partition until we reach the expected level of parallelism
        val nParts = conf.sparkCores.get * 2
        val indexHashes:Seq[(Seq[String], Seq[Option[String]])] = if(keys.size > nParts) { //making sure that we will have enough partitions to satisfy the parallelism level
          keys.map(k => (Seq(k), Seq(None.asInstanceOf[Option[String]])))
        } else {
          val prefixLength =  Math.ceil(Math.log10(nParts)).toInt 
          val max = Math.pow(10, prefixLength).toInt
          Range(0, max).map(i => (i % nParts, i)).groupBy(_._1).values.toSeq.map(s => (keys, s.map(i => Some(s"%0${prefixLength}d".format(i._2)))))
        }
        println(s"keys $from, $to : ${keys.toArray.mkString(",")}")
        //val epiHome = conf.epiHome
        val rdd = sc.parallelize(indexHashes.toSeq)
          .repartition(conf.sparkCores.get)
          .mapPartitions{iter => 
            implicit val holder = LuceneActor.readHolder
            //implicit val cnf = Settings(epiHome)
            cnf.load()
            var lastKey = ""
            var index:TweetIndex=null 
            (for((keys, hashes) <- iter; key <- keys) yield {  
              if(lastKey != key)
                index = LuceneActor.getIndex(key)
              val qb = new BooleanQuery.Builder()
              qb.add(index.parseQuery(query), Occur.MUST)
              qb.add(TermRangeQuery.newStringRange("created_date", from.toString.take(10), to.toString.take(10), true, true), Occur.MUST) 
              if(!hashes.flatMap(o => o).isEmpty) {
                val hQuery = new BooleanQuery.Builder()
                hashes.flatMap(o => o).foreach(hash => hQuery.add(new PrefixQuery(new Term("hash", hash)), Occur.SHOULD))
                qb.add(hQuery.build, Occur.MUST)
              }
              index.searchAll(qb.build)
                .map(doc => EpiSerialisation.luceneDocFormat.write(doc).toString)
            }).flatMap(docs => docs)

          }

        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }

        Some(spark.read.schema(schemas.geoLocatedTweetSchema).json(rdd))
          .map{
            case df if(filterBy.size > 0) => 
              df.where(filterBy.map(w => expr(w)).reduce(_ && _))
            case df => df
          }
          .map(df => df.where(col("lang").isin(conf.languages.get.map(_.code):_*)))
          .map{
            case df if(groupBy.size == 0) => 
              df.select(columns.map(c => expr(c)):_*) 
            case df => 
              df.groupBy(groupBy.map(gb => expr(gb)):_*)
                .agg(expr(columns.head), columns.drop(1).map(c => expr(c)):_*)
          }
          .map{
            case df if(sortBy.size == 0) => 
              df 
            case df =>
              df.orderBy(sortBy.map(ob => expr(ob)):_*)
          }
          .get
          .toJSON
          .toLocalIterator
          .asScala
          .foreach{line => 
            Await.result(caller ? ByteString(
              s"${if(!jsonnl && !first) "\n," else if(jsonnl && !first) "\n" else "" }${line}", 
              ByteString.UTF_8
            ), Duration.Inf)
            first = false
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }
    case b => 
      Future(LuceneActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}

case class IndexHolder(
  var spark:Option[SparkSession] = None,
  val dirs:HashMap[String, String] = HashMap[String, String](),
  val indexes:HashMap[String, TweetIndex] = HashMap[String, TweetIndex](),
  val writeEnabled:Boolean = false,
  var toGeolocate:ArrayBuffer[TopicTweetsV1] = ArrayBuffer[TopicTweetsV1](),
  var geolocating:Boolean = false 
)
 
object LuceneActor {
  lazy val readHolder = LuceneActor.getHolder(writeEnabled = false)
  lazy val writeHolder = LuceneActor.getHolder(writeEnabled = true)
  case class Success(msg:String)
  case class DatesProcessed(msg:String, dates:Seq[String] = Seq[String]())
  case class GeolocatedsCreated (geolocated:Geolocateds, dates:Seq[Instant])
  case class GeolocateRequest(items:TopicTweetsV1)
  case class Failure(msg:String)
  case class CommitRequest()
  case class CloseRequest()
  case class SearchRequest(query:Option[String], topic:String, from:Instant, to:Instant, max:Int, jsonnl:Boolean, caller:ActorRef)
  case class AggregateRequest(
    query:String, 
    from:Instant, 
    to:Instant, 
    columns:Seq[String], 
    groupBy:Seq[String], 
    filterBy:Seq[String], 
    sortBy:Seq[String], 
    sourceExpressions:Map[String, Seq[String]], 
    jsonnl:Boolean, 
    caller:ActorRef 
  )
  def getHolder(writeEnabled:Boolean) = IndexHolder(writeEnabled = writeEnabled)
  def commit()(implicit holder:IndexHolder) = {
    close(closeDirectory = true)
  }
  def close(closeDirectory:Boolean= true)(implicit holder:IndexHolder) = {
    holder.dirs.synchronized {
      var now:Option[Long] = None 
      holder.dirs.foreach{ case (key, path) =>
          now = now.orElse(Some(System.nanoTime))
          val i = holder.indexes(key)
          if(i.writeEnabled && i.writer.get.isOpen) {
            i.writer.get.commit()
            i.writer.get.close()
          }
          i.reader.close()
          if(closeDirectory) {
            i.index.close()
          }
        case _ =>
      }
      holder.indexes.clear
      holder.dirs.clear
      now.map(n => println(s"commit done on ${(System.nanoTime - n) / 1000000000} secs"))
    }
  }
  def closeSparkSession()(implicit holder:IndexHolder) = {
    holder.spark match{
      case Some(spark) =>
        spark.stop()
      case _ =>
    }
  }

  def getSparkSession()(implicit conf:Settings, holder:IndexHolder) = {
    holder.dirs.synchronized {
      conf.load()
      if(holder.spark.isEmpty) {
        holder.spark =  Some(JavaBridge.getSparkSession(conf.sparkCores.getOrElse(0))) 
      }
    }
    holder.spark.get
  }
  def getSparkStorage = Storage.getSparkStorage
  def getIndex(forInstant:Instant)(implicit conf:Settings, holder:IndexHolder):TweetIndex = {
    getIndex(getIndexKey(forInstant))
  }
  def getIndex(key:String)(implicit conf:Settings, holder:IndexHolder):TweetIndex = {
    holder.dirs.synchronized {
      if(!holder.dirs.contains(key)) {
        conf.load()
        holder.dirs(key) = Paths.get(conf.epiHome, "tweets", "fs", key).toString()
      }
    }
    holder.dirs(key).synchronized {
      if(holder.indexes.get(key).isEmpty || !holder.indexes(key).isOpen) {
        holder.indexes(key) = TweetIndex(holder.dirs(key), holder.writeEnabled)
      }
    }
    holder.indexes(key)
  }

  def getIndexKey(forInstant:Instant) = {
    Some(LocalDateTime.ofInstant(forInstant, ZoneOffset.UTC))
      .map(utc => s"${utc.get(IsoFields.WEEK_BASED_YEAR)*100 + utc.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)}")
      .map(s => s"${s.take(4)}.${s.substring(4)}")
      .get
  }

  def getReadKeys(from:Instant, to:Instant)(implicit conf:Settings, holder:IndexHolder) = 
     Range(0, ChronoUnit.DAYS.between(from, to.plus(1, ChronoUnit.DAYS)).toInt)
       .map(i => from.plus(i,  ChronoUnit.DAYS))
       .map(i => getIndexKey(i))
       .distinct
       .filter(key => Paths.get(conf.epiHome, "tweets", "fs", key).toFile.exists())
  
  def getReadIndexes(from:Instant, to:Instant)(implicit conf:Settings, holder:IndexHolder) = 
    getReadKeys(from, to).map(key => getIndex(key))

  def add2Geolocate(tweets:TopicTweetsV1)(implicit conf:Settings, holder:IndexHolder, ec: ExecutionContext) = {
    val toGeo = holder.toGeolocate.synchronized { 
      holder.toGeolocate += tweets
      if(!holder.geolocating) {
        println(s"Go!!!!!! ${holder.toGeolocate.size}")
        holder.geolocating = true
        val r =  holder.toGeolocate.clone
        holder.toGeolocate.clear
        r
      } else {
        ArrayBuffer[TopicTweetsV1]()
      }
    }
    
    Future{
      if(toGeo.size > 0) {
        implicit val spark = LuceneActor.getSparkSession()
        implicit val storage = LuceneActor.getSparkStorage
        LuceneActor.geolocateTweets(toGeo)
        holder.geolocating = false
      }
    }.onComplete{
       case scala.util.Success(_) => 
       case scala.util.Failure(t) =>
         holder.toGeolocate.synchronized { 
           println("Error during geolocalisation: Retrying on next request" + t.getMessage)
           holder.toGeolocate ++= toGeo
           holder.geolocating = false
       }
    }
  }

  val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+|RT|via|vía"
  val defaultTextLangCols =
    Map(
      "text"->Some("lang")
      , "linked_text"->Some("linked_lang")
      , "user_description"->Some("lang")
      //, "linked_user_description"->Some("linked_lang")
      , "user_location"->Some("lang")
      //, "linked_user_location"->Some("linked_lang")
      , "place_full_name"->None.asInstanceOf[Option[String]]
      , "linked_place_full_name"->None.asInstanceOf[Option[String]]
    )
  def geolocateTweets(tweets:ArrayBuffer[TopicTweetsV1])(implicit spark:SparkSession, conf:Settings, storage:Storage) {
    import spark.implicits._
    val sc = spark.sparkContext
    val startTime = System.nanoTime
    var x =
      sc.parallelize(tweets.flatMap{ case(TopicTweetsV1(topic, tweets)) => tweets.items.map(t => (topic, t))})
        .repartition(conf.sparkCores.get)
        .toDF
        .as[(String, TweetV1)].toDF("topic", "tweet")
        .select(col("topic"), col("tweet.*"))
        .geolocate(
          textLangCols = defaultTextLangCols
          , minScore = conf.geolocationThreshold.get
          , maxLevDistance = 0
          , nGram = 3
          , tokenizerRegex = twitterSplitter
          , langs = conf.languages.get
          , geonames = conf.geonames
          , reuseGeoIndex = true
          , langIndexPath=conf.langIndexPath
          , reuseLangIndex = true
          , strategy = conf.geolocationStrategy.get
        )
        .select((
          Seq(col("topic"), col("lang"), col("tweet_id").as("id"), col("created_at"))
            ++  defaultTextLangCols.keys.toSeq
                .map(c => s"${c}_loc")
                .map(c => when(col(s"$c.geo_id").isNotNull, struct(Seq("geo_id","geo_name", "geo_code","geo_type","geo_country_code", "geo_longitude", "geo_latitude").map(cc => col(s"$c.$cc")):_*)).as(c))
          ):_*
        )
        .withColumn("is_geo_located"
          , when(defaultTextLangCols.keys.toSeq.map(c => col(s"${c}_loc").isNull).reduce( _ && _)
           , false
           ).otherwise(true)
        )

       val midTime = System.nanoTime
       println(s"${(midTime - startTime)/1e9d} secs for getting ds")
       val numGeo = 
         x.select(struct(
            col("topic"), 
              col("id"), 
              col("is_geo_located"), 
              col("lang"),
              col("linked_place_full_name_loc"), 
              col("linked_text_loc"), 
              col("place_full_name_loc"), 
              col("text_loc"), 
              col("user_description_loc"), 
              col("user_location_loc")
            ).as("geo"), 
            col("created_at")
          )
          .as[(Geolocated, Instant)]
          .mapPartitions{iter =>
            val indexes = HashMap[String, TweetIndex]()
            iter.map{case (geo, createdAt) =>
              val iKey = LuceneActor.getIndexKey(createdAt)
              implicit val holder = LuceneActor.writeHolder
              if(!indexes.contains(iKey)) {
                indexes(iKey) = LuceneActor.getIndex(iKey)
                indexes(iKey).refreshReader()
              }
              indexes(iKey).indexGeolocated(geo)
              1
            }
          }
          .reduce(_ + _)

       val endTime = System.nanoTime
       println(s"${(endTime - midTime)/1e9d} secs for geolocating ${numGeo} tweets")
  }
}

