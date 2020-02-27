package demy.mllib.test

import org.apache.spark.sql.SparkSession
import org.scalatest._

object SharedSpark {
  var baseSpark:Option[SparkSession] = None
  def getSpark = baseSpark match { case Some(spark) => spark case _ => throw new Exception("spark context has not been initializedi yet")}
  def init { 
     baseSpark = Some(SparkSession.builder()
        .master("local[*]")
        //.config(conf)
        .appName("test")
        .getOrCreate())
  }
  def stop { getSpark.stop }
}
class SparkTest  extends UnitTest with BeforeAndAfterAll
{
  override def beforeAll() {
    SharedSpark.init
    println("Spark Session creted!")
  }

  override def afterAll() {
    SharedSpark.stop
  }  
}

