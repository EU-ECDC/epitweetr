package org.ecdc.epitweetr.fs

import org.ecdc.epitweetr.EpitweetrActor
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
import java.nio.file.{Paths, Files}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions.{col, udf, input_file_name, explode, coalesce, when, lit, concat, struct, expr, lower}
import demy.storage.{Storage, FSNode}
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField}
import org.apache.lucene.search.{Query, TermQuery, BooleanQuery, PrefixQuery, TermRangeQuery}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.index.Term
import org.apache.lucene.search.spell.LuceneDictionary
import demy.util.{log => l}
import scala.concurrent.ExecutionContext
import org.ecdc.epitweetr.geo.Geonames.Geolocate
import scala.util.{Try, Success, Failure}
import java.nio.charset.StandardCharsets
import spray.json.JsonParser
import scala.collection.parallel.ForkJoinTaskSupport
import java.util.concurrent.ForkJoinPool

class LuceneActor(conf:Settings) extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val cnf = conf
  def receive = {
    case TopicTweetsV1(topic, ts) =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        val dateMap = ts.items.groupBy(t => t.created_at.toString.take(10))
        dateMap.foreach{case (date, tweets) =>
          val index = LuceneActor.getIndex("tweets", tweets(0).created_at)
          tweets.foreach{t => index.indexTweet(t, topic)}
        }
        dateMap.keys.toSeq
      }
      .map{dates =>
        LuceneActor.DatesProcessed("ok", dates)
      }
      .pipeTo(sender())
    case LuceneActor.GeolocateTweetsRequest(TopicTweetsV1(topic, ts)) =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        implicit val spark = LuceneActor.getSparkSession()
        implicit val storage = LuceneActor.getSparkStorage
        LuceneActor.add2Geolocate(TopicTweetsV1(topic, ts), conf.forcedGeo.map(_.items), conf.forcedGeoCodes.map(_.items), conf.topicKeyWords.map(_.items(topic)))  
      }.onComplete {
        case Success(_) =>
        case Failure(t) => println("Error during geolocalisation: " + t.getMessage)
      }
    case LuceneActor.GeolocatedTweetsCreated(GeolocatedTweets(items), dates) =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        val indexes = dates.flatMap(d => LuceneActor.getReadIndexes("tweets", Some(d), Some(d)))
        items.foreach{g =>
          var found = false 
          for(index <- indexes if !found) {
            found = index.indexGeolocated(g)
          }
        }
        items.size
      }
      .map{c =>
        EpitweetrActor.Success(s"$c geolocated properly processed")
      }
      .pipeTo(sender())
    case ts:LuceneActor.CommitRequest =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        LuceneActor.commit()
      }
      .map{c =>
        EpitweetrActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    case LuceneActor.CloseRequest =>
      implicit val holder = LuceneActor.writeHolder
      Future {
        LuceneActor.commit(closeDirectory = true)
        LuceneActor.closeSparkSession()
      }
      .map{c =>
        EpitweetrActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    case LuceneActor.SearchRequest(query, topic, from, to, max, jsonnl, caller) => 
      implicit val timeout: Timeout = conf.fsQueryTimeout.seconds //For ask property
      implicit val holder = LuceneActor.readHolder
      val indexes = LuceneActor.getReadIndexes("tweets", from, to).toSeq
      var sep = ""
      Future {
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        var left = max
        indexes
          .iterator
          .flatMap{case i => 
            i.refreshReader()
            val qb = new BooleanQuery.Builder()
            query.map{q => 
              qb.add(i.parseQuery(q), Occur.MUST)
            }
            topic.map{t =>
              qb.add(new TermQuery(new Term("topic", t)), Occur.MUST) 
            }
            qb.add(TermRangeQuery.newStringRange("created_date", from.map(d => d.toString.take(10)).getOrElse("0"), to.map(d => d.toString.take(10)).getOrElse("9"), true, true), Occur.MUST) 
            val tweets = i.searchTweets(qb.build, Some(left)).toSeq
            left = left - tweets.size
            if(left < 0)
              tweets.take(tweets.size + left)
            else 
              tweets
          }
          .map(doc => EpiSerialisation.luceneDocFormat.customWrite(doc, forceString=Set("tweet_id", "user_id")))
          .take(max)
          .foreach{line => 
            Await.result(caller ? ByteString(
              s"${sep}${line.toString}\n", 
              ByteString.UTF_8
            ), Duration.Inf)
            if(!jsonnl) sep = ","
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getCause} ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }.onComplete { case  _ =>
      }
    case LuceneActor.AggregatedRequest(collection, topic, from, to, filters, jsonnl, caller) => 
      implicit val timeout: Timeout = conf.fsBatchTimeout.seconds //For ask property
      implicit val holder = LuceneActor.readHolder
      val indexes = LuceneActor.getReadIndexes(collection, Some(from), Some(to)).toSeq
      var sep = ""
      val chunkSize = 500
                
   
      Future {
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }

        val (parSeq, parallelism) = 
          Some(indexes.toSeq)
           .map(s => (s.par, s.size))
           .get
        if(parSeq.size > 0) {
          val pool = new ForkJoinPool(parallelism)
          parSeq.tasksupport = new ForkJoinTaskSupport(pool)
          parSeq.tasksupport = new ForkJoinTaskSupport(java.util.concurrent.ForkJoinPool.commonPool)
          parSeq
            .foreach{case i =>
              val builder = StringBuilder.newBuilder
              var toAdd = chunkSize
              val qb = new BooleanQuery.Builder()
              qb.add(new TermQuery(new Term("topic", topic)), Occur.MUST) 
              qb.add(TermRangeQuery.newStringRange("created_date", from.toString.take(10), to.toString.take(10), true, true), Occur.MUST) 
              filters.foreach{case(field, value) => qb.add(new TermQuery(new Term(field, value)), Occur.MUST)}
              var first = true
              i.searchTweets(qb.build)
                .map(doc => EpiSerialisation.luceneDocFormat.write(doc))
                .foreach{line =>
                  if(first == true) {
                    first = false
                  }
                  builder ++= s"${sep}${line.toString}\n"
                  toAdd = toAdd -1
                  if(toAdd == 0) {
                    pool.synchronized {
                      Await.result(caller ? ByteString(builder.toString, ByteString.UTF_8), Duration.Inf)
                    }
                    toAdd = chunkSize
                    builder.clear
                  }
                  if(!jsonnl) sep = ","
                }

              if(builder.size > 0) {
                pool.synchronized {
                  Await.result(caller ? ByteString(builder.toString, ByteString.UTF_8), Duration.Inf)
                }
                builder.clear
              }
            }
          pool.shutdown
        }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }.onComplete { case  _ =>
      }
    /*case LuceneActor.AggregationRequest(query, from, to, columns, groupBy, filterBy, sortBy, sourceExpressions, jsonnl, caller) =>
      implicit val timeout: Timeout = conf.fsBatchTimeout.seconds //For ask property
      implicit val holder = LuceneActor.readHolder
      val spark = LuceneActor.getSparkSession()
      import spark.implicits._
      val sc = spark.sparkContext
      Future {
        val keys = LuceneActor.getReadKeys("tweets", from, to).toSeq

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
        val epiHome = conf.epiHome
        val rdd = sc.parallelize(indexHashes.toSeq)
          .repartition(conf.sparkCores.get)
          .mapPartitions{iter => 
            implicit val holder = LuceneActor.readHolder
            implicit val cnf = Settings(epiHome)
            cnf.load()
            var lastKey = ""
            var index:TweetIndex=null 
            (for((keys, hashes) <- iter; key <- keys) yield {  
              if(lastKey != key)
                index = LuceneActor.getIndex("tweets", key)
              val qb = new BooleanQuery.Builder()
              query.map{q => 
                qb.add(index.parseQuery(q), Occur.MUST)
              }
              qb.add(TermRangeQuery.newStringRange("created_date", from.toString.take(10), to.toString.take(10), true, true), Occur.MUST) 
              if(!hashes.flatMap(o => o).isEmpty) {
                val hQuery = new BooleanQuery.Builder()
                hashes.flatMap(o => o).foreach(hash => hQuery.add(new PrefixQuery(new Term("hash", hash)), Occur.SHOULD))
                qb.add(hQuery.build, Occur.MUST)
              }
              index.searchTweets(qb.build)
                .map(doc => EpiSerialisation.luceneDocFormat.write(doc).toString)
            }).flatMap(docs => docs)

          }

        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }

        Some(spark.read.schema(schemas.geoLocatedTweetSchema).json(rdd))
          .map(df => df.where(col("lang").isin(conf.languages.get.map(_.code):_*)))
          .map{
            case df if(sourceExpressions.size > 0) => 
              df.select(sourceExpressions.map(s => expr(s) ):_*)
            case df => df
          }
          .map{
            case df if(filterBy.size > 0) => 
              df.where(filterBy.map(w => expr(w)).reduce(_ && _))
            case df => df
          }
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
          .map{df =>
            df.mapPartitions{iter => 
              implicit val holder = LuceneActor.writeHolder
              implicit val cnf = Settings(epiHome)
              cnf.load()
              var lastKey = ""
              var index:TweetIndex=null 
              (for(row <- iter) yield {
                val pks = Seq("topic", "created_date", "created_hour")
                val collection = "country_counts"
                val key =  Instant.parse(s"${row.getAs[String]("created_date")}T00:00:00.000Z")
                if(lastKey != key)
                  index = LuceneActor.getIndex(collection, key)
                index.indexSparkRow(row = row, pk = pks)
                1
              })
            }.as[Int].reduce(_ + _)
          }
          .map(v => 
            Await.result(caller ? ByteString(v.toString, ByteString.UTF_8), Duration.Inf)
          ) 
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }*/
    case LuceneActor.PeriodRequest(collection) =>
      implicit val holder = LuceneActor.readHolder
      Future{
        val sPath = Paths.get(conf.fsRoot, collection)
        if(!Files.exists(sPath))
          LuceneActor.PeriodResponse(None, None)
        else {
          val keys = Files.list(sPath).iterator.asScala.toSeq.map(p => p.getFileName.toString)
          val dates = Seq(keys.min, keys.max)
            .distinct
            .flatMap(key => 
              Try (LuceneActor.getIndex(collection, key)) match {
                case Success(i) => Some(i)
                case _ => None
              }
            )
            .flatMap{index =>
              val aDates = ArrayBuffer[String]()
              val searcher = index.useSearcher()
              val dateIter =  new LuceneDictionary(searcher.getIndexReader, "created_date").getEntryIterator()
              var date = dateIter.next
              while(date != null) {
                aDates += date.utf8ToString 
                date = dateIter.next
              }
              index.releaseSearcher(searcher)
              aDates
            }
          if(dates.size> 0)
            LuceneActor.PeriodResponse(Some(dates.min), Some(dates.max))
          else
            LuceneActor.PeriodResponse(None, None)
        }
      }
      .pipeTo(sender())
    case LuceneActor.RecalculateHashRequest() =>
      implicit val holder = LuceneActor.writeHolder
      Future {
        LuceneActor.recalculateHash()
        "Done!" 
      }.pipeTo(sender())
    case b => 
      Future(EpitweetrActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}

case class IndexHolder(
  var spark:Option[SparkSession] = None,
  val dirs:HashMap[String, String] = HashMap[String, String](),
  val indexes:HashMap[String, TweetIndex] = HashMap[String, TweetIndex](),
  val writeEnabled:Boolean = false,
  var toGeolocate:ArrayBuffer[TopicTweetsV1] = ArrayBuffer[TopicTweetsV1](),
  var toAggregate:ArrayBuffer[TopicTweetsV1] = ArrayBuffer[TopicTweetsV1](),
  var geolocating:Boolean = false,
  var aggregating:Boolean = false 
)
 
object LuceneActor {
  lazy val readHolder = LuceneActor.getHolder(writeEnabled = false)
  lazy val writeHolder = LuceneActor.getHolder(writeEnabled = true)
  case class DatesProcessed(msg:String, dates:Seq[String] = Seq[String]())
  case class GeolocatedTweetsCreated (geolocated:GeolocatedTweets, dates:Seq[Instant])
  case class GeolocateTweetsRequest(items:TopicTweetsV1)
  case class CommitRequest()
  case class CloseRequest()
  case class SearchRequest(query:Option[String], topic:Option[String], from:Option[Instant], to:Option[Instant], max:Int, jsonnl:Boolean, caller:ActorRef)
  case class AggregateRequest(items:TopicTweetsV1)
  case class AggregatedRequest(collection:String, topic:String, from:Instant, to:Instant, filters:Seq[(String, String)], jsonnl:Boolean, caller:ActorRef)
  case class AggregationRequest(
    query:Option[String], 
    from:Instant, 
    to:Instant, 
    columns:Seq[String], 
    groupBy:Seq[String], 
    filterBy:Seq[String], 
    sortBy:Seq[String], 
    sourceExpressions:Seq[String], 
    jsonnl:Boolean, 
    caller:ActorRef 
  )
  case class PeriodRequest(collection:String)
  case class PeriodResponse(first:Option[String], last:Option[String])
  case class RecalculateHashRequest()
  def getHolder(writeEnabled:Boolean) = IndexHolder(writeEnabled = writeEnabled)
  def commit()(implicit holder:IndexHolder)  {
    commit(closeDirectory = true)
  }
  def commit(closeDirectory:Boolean= true)(implicit holder:IndexHolder)  {
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
  def getIndex(collection:String, forInstant:Instant)(implicit conf:Settings, holder:IndexHolder):TweetIndex = {
    getIndex(collection, getIndexKey(collection, forInstant))
  }
  def getIndex(collection:String, key:String)(implicit conf:Settings, holder:IndexHolder):TweetIndex = {
    val path = s"$collection.$key"
    holder.dirs.synchronized {
      if(!holder.dirs.contains(path)) {
        conf.load()
        holder.dirs(path) = Paths.get(conf.epiHome, "fs", collection,  key).toString()
      }
    }
    holder.dirs(path).synchronized {
      if(holder.indexes.get(path).isEmpty || !holder.indexes(path).isOpen) {
        holder.indexes(path) = TweetIndex(holder.dirs(path), holder.writeEnabled)
      }
    }
    holder.indexes(path)
  }

  def getIndexKey(collection:String, forInstant:Instant) = {
    val timing = collection match {
      case "tweets" => "week"
      case "country_counts" => "week"
      case "topwords" => "week"
      case "geolocated" => "week"
      case _ => throw new Exception(s"unknown collection $collection")
    }
    timing match {
      case "week" =>
        Some(LocalDateTime.ofInstant(forInstant, ZoneOffset.UTC))
          .map(utc => s"${utc.get(IsoFields.WEEK_BASED_YEAR)*100 + utc.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)}")
          .map(s => s"${s.take(4)}.${s.substring(4)}")
          .get
      case _ => throw new Exception(s"Unknon grouping for collection $collection")
    }
  }    

  def getReadKeys(collection:String, from:Option[Instant], to:Option[Instant])(implicit conf:Settings, holder:IndexHolder) = 
     (from, to) match {
       case(Some(f), Some(t)) => 
         Range(0, ChronoUnit.DAYS.between(f, t.plus(1, ChronoUnit.DAYS)).toInt)
           .map(i => f.plus(i,  ChronoUnit.DAYS))
           .map(i => getIndexKey(collection, i))
           .distinct
           .filter(key => Paths.get(conf.epiHome, "fs", collection ,key).toFile.exists())
           .sortWith(_ > _)
        case _ =>
          val sPath = Paths.get(conf.fsRoot, collection)
          if(!Files.exists(sPath))
            Seq[String]()
          else 
            Files.list(sPath)
              .iterator
              .asScala
              .toSeq.map(p => p.getFileName.toString)
              .takeWhile{p =>
                (from.isEmpty || p >= getIndexKey(collection, from.get)) &&
                (to.isEmpty || p <= getIndexKey(collection, to.get))
              }
              .sortWith(_ > _)
     }
  
  def getReadIndexes(collection:String, from:Option[Instant], to:Option[Instant])(implicit conf:Settings, holder:IndexHolder) = 
    getReadKeys(collection, from, to).map(key => getIndex(collection, key))

  def add2Geolocate(tweets:TopicTweetsV1, forcedGeo:Option[Map[String, String]], forcedGeoCodes:Option[Map[String, String]], topics:Option[Set[String]])
    (implicit conf:Settings, holder:IndexHolder, ec: ExecutionContext) = 
  {
    val toGeo = holder.toGeolocate.synchronized { 
      holder.toGeolocate += tweets
      if(!holder.geolocating) {
        println(s"Go geo!!!!!! ${holder.toGeolocate.size}")
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
        LuceneActor.geolocateTweets(toGeo, forcedGeo, forcedGeoCodes, topics)
        holder.geolocating = false
        LuceneActor.add2Aggregate(toGeo) 
      }
    }.onComplete{
       case scala.util.Success(_) =>
       case scala.util.Failure(t) =>
         holder.toGeolocate.synchronized { 
           println(s"Error during geolocalisation: Retrying on next request ${t.getMessage} ${t.getStackTrace.mkString("\n")}" )
           holder.toGeolocate ++= toGeo
           holder.geolocating = false
       }
    }
  }
  def add2Aggregate(tweets:ArrayBuffer[TopicTweetsV1])(implicit conf:Settings, holder:IndexHolder, ec: ExecutionContext) = {
    val toAggr = holder.toAggregate.synchronized { 
      holder.toAggregate ++= tweets
      if(!holder.aggregating) {
        println(s"Go Aggr!!!!!! ${holder.toAggregate.size}")
        holder.aggregating = true
        val r =  holder.toAggregate.clone
        holder.toAggregate.clear
        r
      } else {
        ArrayBuffer[TopicTweetsV1]()
      }
    }
    
    Future{
      if(toAggr.size > 0) {
        implicit val spark = LuceneActor.getSparkSession()
        implicit val storage = LuceneActor.getSparkStorage
        Files.list(Paths.get(conf.collectionPath)).iterator.asScala.foreach{p =>
           val content = Files.lines(p, StandardCharsets.UTF_8).iterator.asScala.mkString("\n")
           Some(EpiSerialisation.collectionFormat.read(JsonParser(content)))
            .map{
             case collection =>
               LuceneActor.aggregateTweets(toAggr, collection)
           }
        } 
        holder.aggregating = false
      }
    }.onComplete{
       case scala.util.Success(_) => 
       case scala.util.Failure(t) =>
         holder.toAggregate.synchronized { 
           println(s"Error during aggregating: Retrying on next request ${t.getMessage} ${t.getStackTrace.mkString("\n")} ")
           holder.toAggregate ++= toAggr
           holder.aggregating = false
       }
    }
  }

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
  def geolocateTweets(
    tweets:ArrayBuffer[TopicTweetsV1], 
    forcedGeo:Option[Map[String, String]], 
    forcedGeoCodes:Option[Map[String, String]], 
    topics:Option[Set[String]]
  )(implicit spark:SparkSession, conf:Settings, storage:Storage) {
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
          , nBefore = conf.geoNBefore
          , nAfter = conf.geoNAfter
          , tokenizerRegex = conf.splitter
          , langs = conf.languages.get
          , geonames = conf.geonames
          , reuseGeoIndex = true
          , langIndexPath=conf.langIndexPath
          , reuseLangIndex = true
          , forcedGeo = forcedGeo
          , forcedGeoCodes = forcedGeoCodes
          , closestTo = topics
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
          .as[(GeolocatedTweet, Instant)]
          .mapPartitions{iter =>
            val indexes = HashMap[String, TweetIndex]()
            iter.map{case (geo, createdAt) =>
              val iKey = LuceneActor.getIndexKey("tweets", createdAt)
              implicit val holder = LuceneActor.writeHolder
              if(!indexes.contains(iKey)) {
                indexes(iKey) = LuceneActor.getIndex("tweets", iKey)
                indexes(iKey).refreshReader()
              }
              indexes(iKey).indexGeolocated(geo)
              1
            }
          }
          .collect
          .sum //TODO: Find a way to avoid collect and manage 0 rows (reduce will throw an error on that case

       val endTime = System.nanoTime
       println(s"${(endTime - midTime)/1e9d} secs for geolocating ${numGeo} tweets")
  }
  def aggregateTweets(tweets:ArrayBuffer[TopicTweetsV1], collection:Collection)(implicit spark:SparkSession, conf:Settings, storage:Storage) {
    import spark.implicits._
    val startTime = System.nanoTime
    
    implicit val holder = LuceneActor.readHolder
    val sc = spark.sparkContext
    val sorted = tweets
      .flatMap{case TopicTweetsV1(topic, tts) => tts.items.map(t => (topic, t.tweet_id, LuceneActor.getIndexKey(collection.name, t.created_at)))}
      .sortWith(_._3 < _._3)
    val epiHome = conf.epiHome
    val aggr = collection.aggregation
    val columns = aggr.columns.map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val groupBy = aggr.groupBy.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val filterBy = aggr.filterBy.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val sortBy = aggr.sortBy.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val sourceExp = aggr.sourceExpressions.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val pks = collection.pks
    val collName = collection.name
    val dateCol = collection.dateCol
    Some(sorted.toDS)
      .map(df => df.repartition(conf.sparkCores.get))
      .map{df => df.mapPartitions{iter => 
        implicit val holder = LuceneActor.writeHolder
        implicit val conf = Settings(epiHome)
        conf.load()
        var lastKey = ""
        var index:TweetIndex=null 
        (for((topic, id, key) <- iter) yield {  
          if(lastKey != key) {
            index = LuceneActor.getIndex("tweets", key)
            index.refreshReader
          }
          index.searchTweet(id, topic) match {
            case Some(s) => Some(s)
            case _ => 
              println(s"Cannot find tweet to aggregate $key, $id, $topic")
              None
          }
        }).flatMap(ot => ot)
      }}
      .map(rdd => spark.read.schema(schemas.geoLocatedTweetSchema).json(rdd))
      .map{df => df.where(col("lang").isin(conf.languages.get.map(_.code):_*))}
      .map{
        case df if(sourceExp.size > 0) => 
          df.select(sourceExp.map(s => expr(s) ):_*)
        case df => df
      }
      .map{
        case df if(filterBy.size > 0) => 
          df.where(filterBy.map(w => expr(w)).reduce(_ && _))
        case df => df
      }
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
      .map{df =>
        //println(df.collect.size)
        df.mapPartitions{iter => 
          implicit val holder = LuceneActor.writeHolder
          implicit val conf = Settings(epiHome)
          conf.load()
          var lastKey = ""
          var index:TweetIndex=null 
          (for(row <- iter) yield {
            val key =  Instant.parse(s"${row.getAs[String](dateCol)}T00:00:00.000Z")
            if(lastKey != key)
              index = LuceneActor.getIndex(collName, key)
            index.indexSparkRow(row = row, pk = pks, aggr = collection.aggr)
            1
          })
        }.as[Int].reduce(_ + _)
      }.map{numAggr =>
        val endTime = System.nanoTime
        println(s"${(endTime - startTime)/1e9d} secs for aggregating ${numAggr} tweets in ${collection.name}")
      }
  }
  def recalculateHash()(implicit holder:IndexHolder, conf:Settings){
    Files.list(Paths.get(conf.collectionPath)).iterator.asScala.foreach{p =>
      Some(Files.lines(p, StandardCharsets.UTF_8).iterator.asScala.mkString("\n"))
         .map(content => EpiSerialisation.collectionFormat.read(JsonParser(content)))
         .map{ case col =>
           l.msg(f"Recalculating hashes for ${col.name}")
           val pkName = col.pks.mkString("_")
           val ind  = LuceneActor.getReadIndexes(collection = col.name, from = None, to = None)
           ind.foreach{i => 
             i.searchTweets(new org.apache.lucene.search.MatchAllDocsQuery(), max = None)
               .foreach{ case doc =>
                  i.updateHash(doc = doc, pkName = pkName) 
               }
           }
           l.msg(f"Recalculating hashes for ${col.name} done!!")

         }

    }
    LuceneActor.commit()
  }

}

