package org.ecdc.epitweetr.geo

import org.ecdc.twitter.{JavaBridge, Language}
import Geonames.Geolocate
import demy.util.{log => l, util}
import org.ecdc.epitweetr.{Settings, EpitweetrActor}
import org.ecdc.epitweetr.fs.{TextToGeo, TaggedAlert, AlertRun}
import akka.pattern.{ask, pipe}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.actor.ActorRef
import akka.Done
import akka.util.ByteString
import akka.util.{Timeout}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions.{col, lit, array_join, udf}
import demy.storage.{Storage, FSNode}
import scala.concurrent.ExecutionContext
import scala.util.{Try, Success, Failure}
import java.nio.charset.StandardCharsets
import spray.json.JsonParser
import scala.util.Random

class AlertActor(conf:Settings) extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val cnf = conf
  def receive = {
    case AlertActor.ClassifyAlertsRequest(alerts, runs, train, jsonnl, caller) => 
      implicit val timeout: Timeout = conf.fsQueryTimeout.seconds //For ask property
      implicit val s = GeonamesActor.getSparkSession
      implicit val st = GeonamesActor.getSparkStorage
      import s.implicits._
      //Ensuring languages models are up to date

      var sep = ""
      Future {
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        implicit val s = GeonamesActor.getSparkSession
        implicit val st = GeonamesActor.getSparkStorage
        import s.implicits._
        
        val r = new Random(197912)
        import s.implicits._
        val ret = Seq("{\"todo\":\"Bien!!\"}")
        l.msg(s"todo bien")
        

        ret
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
    }  
    case b => 
      Future(EpitweetrActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}

 
object AlertActor {
  var spark:Option[SparkSession] = None
  case class ClassifyAlertsRequest(
    alerts:Seq[TaggedAlert], 
    runs:Seq[AlertRun], 
    train:Boolean, 
    jsonnl:Boolean, 
    caller:ActorRef
  )
  def closeSparkSession() = {
    spark match{
      case Some(spark) =>
        spark.stop()
      case _ =>
    }
  }

  def getSparkSession()(implicit conf:Settings) = {
    GeonamesActor.synchronized {
      conf.load()
      if(GeonamesActor.spark.isEmpty) {
        GeonamesActor.spark =  Some(JavaBridge.getSparkSession(conf.sparkCores.getOrElse(0))) 
      }
    }
    GeonamesActor.spark.get
  }
  def getSparkStorage = Storage.getSparkStorage
}



