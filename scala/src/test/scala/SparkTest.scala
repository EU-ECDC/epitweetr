package org.ecdc.epitweetr.test

import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SharedSpark {
  var baseSpark:Option[SparkSession] = None
  def getSpark = baseSpark match { case Some(spark) => spark case _ => throw new Exception("spark context has not been initializedi yet")}
  def init { 
     Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
     Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
     Logger.getLogger("akka").setLevel(Level.ERROR)
     
     baseSpark = Some(SparkSession.builder()
        .master("local[*]")
        //.config(conf)
        .appName("test")
        .getOrCreate())
    baseSpark.get.sparkContext.setLogLevel("WARN")
  }
  def stop { getSpark.stop }
}

class SparkTest  extends UnitTest 
  with BeforeAndAfterAll 
  with geo.GeoTrainingTest
  with SettingsTest
  with LanguageTest
  with geo.GeonamesTest
{
  override def beforeAll() {
    SharedSpark.init
    println("Spark Session creted!")
  }

  override def afterAll() {
    SharedSpark.stop
  }  
}

