package demy

import org.apache.spark.sql.SparkSession
import demy.storage.{Storage, FSNode}

trait Application {
  val defaultConfValues:Map[String, String]
  def run(spark:SparkSession, sparkStorage:Storage, configuration:Configuration)

  def main(args: Array[String]) {
    var master:Option[String] = None
    var appName:Option[String] = None
    var config:Option[String] = None

    //println(args.toSeq) // => (master, local[*])
    var printUsage = false
    args.drop(1).foreach{s =>
      if(s.indexOf("=") match {case i if i<=0 || i==s.size-1 => true case _ => false })
        printUsage = true
      else {
        val (prop, value) = (s.slice(0, s.indexOf("=")).toLowerCase, s.substring(s.indexOf("=")+1))
        //println(s"($prop, $value)") // => (config, devscripts/DEV.json)
        if(prop == "master") master=Some(value)
        else if(prop == "appname") appName = Some(value)
        else if(prop == "config") config = Some(value)
        else printUsage = true
      }
    }
    if(printUsage) {
      println("""
	  This appication sould be call as follows:
	    java app.jar "ApplicationClassName" master={master url} appname={appName} config={demy app config json}
	    or
	    spark-sumbit ... app.jar appName={appName} config={config}
	""")
    } else {
      val spark = getSparkSession(appName, master)
      val storage = Storage.getSparkStorage
      val configuration =
        config match {
          case Some(confPath) => Configuration(Some(storage.getNode(path = confPath)), this.defaultConfValues)
          case _ => Configuration(defaultValues = this.defaultConfValues)
        }
      run(spark=spark, sparkStorage=storage, configuration = configuration)
    }

  }
  def getSparkSession(appName:Option[String]=None, master:Option[String]=None) = {
    (((
      SparkSession.builder
    ) match {case b => appName match {case Some(s) =>b.appName(s) case _ => b} }
    ) match {case b => master match {case Some(s) => b.master(s) case _ => b} }
    ).getOrCreate()
  }
}
