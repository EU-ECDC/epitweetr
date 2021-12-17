package org.ecdc.epitweetr.alert

import org.ecdc.epitweetr.fs.{AlertClassification}
import org.apache.spark.ml.classification.{Classifier, ClassificationModel, OneVsRest, OneVsRestModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import demy.util.{log => l, util}
import demy.mllib.linalg.implicits._
import org.ecdc.epitweetr.{Settings, EpitweetrActor}
import org.ecdc.twitter.{Language}
import org.ecdc.twitter.Language.LangTools
import org.ecdc.epitweetr.fs.{TextToGeo, TaggedAlert, AlertRun}
import akka.pattern.{ask, pipe}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.Try
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.actor.ActorRef
import akka.Done
import akka.util.{ByteString, Timeout}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions.{col, lit, array_join, udf, concat}
import org.apache.spark.ml.linalg.{Vectors, DenseVector, Vector=>MLVector}
import demy.storage.{Storage, FSNode}
import scala.util.Random

class AlertActor(conf:Settings) extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val cnf = conf
  def receive = {
    case AlertActor.ClassifyAlertsRequest(alerts, oRuns) => 
      implicit val timeout: Timeout = conf.fsQueryTimeout.seconds //For ask property
      implicit val s = conf.getSparkSession
      implicit val st = conf.getSparkStorage
      import s.implicits._
      //Ensuring languages models are up to date

      var sep = ""
      Future {
        implicit val s = conf.getSparkSession
        implicit val st = conf.getSparkStorage
        import s.implicits._
        
	    val alertsdf =  AlertActor.alerts2modeldf(alerts = alerts, reuseLabels = oRuns.isEmpty)
      val retRuns = 
	      oRuns.map{case runs =>
	        val newRuns = 
	          runs
	            .map{r =>
	              if(!r.active.getOrElse(true))
	                r
	              else {
	                l.msg(s"Running ${r.models} on ${alerts.size} alerts ${r.runs} times, customized with ${r.custom_parameters}")
                        AlertActor.trainTest(alertsdf = alertsdf, run = r, trainRatio = 0.75)
	                  .setCount(alerts.size)
	              }
              }
	            .sortWith{(a, b) => 
	              (a.f1score - (if(!a.active.getOrElse(true)) 1.0 else 0.0)) > (b.f1score - (if(!b.active.getOrElse(true)) 1.0 else 0.0))
	            }
	            .zipWithIndex
	            .map{case (r, i) => r.setRanking(i)}
                if(newRuns.size > 0 && newRuns(0).active.getOrElse(true)) {
                  l.msg(s"Retraining with full data for best run ${runs(0).models}, ${runs(0).custom_parameters}")
	                AlertActor.finalTrain(alertsdf = alertsdf, run = runs(0))
	              } else {
                  l.msg(s"No training was performed since ")
	              }
	              newRuns
	      }

        val retAlerts = AlertActor.classifyAlerts(alertsdf).collect.toSeq
	      AlertClassification(retAlerts, retRuns)
      }.recover {
        case e: Exception =>
          EpitweetrActor.Failure(s"[Job-error]: ${e.getCause} ${e.getMessage}: ${e.getStackTrace.mkString("\n")}")
      }.pipeTo(sender())
    case b => 
      Future(EpitweetrActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}

 
object AlertActor {
  case class ClassifyAlertsRequest(
    alerts:Seq[TaggedAlert], 
    runs:Option[Seq[AlertRun]], 
  )
  def getModel[F, E <: Classifier[F, E, M], M <: ClassificationModel[F, M]](run:AlertRun) = {
    val classifier = 
      Try(
        Class.forName(s"org.apache.spark.ml.classification.${run.models}")
          .getConstructor()
          .newInstance()
          .asInstanceOf[Classifier[F, E, M]]
      ).getOrElse(
        throw new Exception(s"Cannot create an alert classifier for class org.apache.spark.ml.classification.${run.models}. The selected class must inherit from https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/classification/Classifier.html")
      )
    val params = classifier.params.map{p => ( 
      p.name,
      (p.getClass.getName.split("\\.").last match {case "Param" => "String" case a => a }).replaceAll("Param", ""),
      if(classifier.hasDefault(p)) classifier.getDefault(p) else "",
      p.doc
    )}
    //Setting custom parameters
    run.custom_parameters.map(p => p.foreach{case (name, strVal) => 
      if(classifier.hasParam(name))
        classifier.getParam(name) match {
          case param =>
           if((param.getClass.getName.endsWith("DoubleParam") && Try(classifier.set(param, strVal.toDouble)).isSuccess)
               || (param.getClass.getName.endsWith("IntParam") && Try(classifier.set(param, strVal.toInt)).isSuccess)
               || (param.getClass.getName.endsWith("param.Param") && Try(classifier.set(param, strVal)).isSuccess)
           ) {} else
             throw new Exception(
               s"Could not set the value $strVal to parameter $name. This is the doc\n: ${params.filter(_._1 == name).map{
                 case (name, dtype, default, doc) => s"$name[$dtype]($default):$doc"}.mkString(",")}"
             )
      } else {
        throw new Exception(s"Model ${run.models} does not contains a parameter $name, available parameters are ${params.map{case(name, dtype, default, doc) => s"$name[$dtype]($default)"}.mkString(", ")}")
      }
    })
    //Creating the one vs Rest classifier
    new OneVsRest()
      .setClassifier(classifier)
      .setFeaturesCol("features")
      .setLabelCol("category_vector")
      .setPredictionCol("predicted_vector")
  }

  def classificationPath()(implicit conf:Settings) = s"${conf.epiHome}/alert-ml"
  def labelIndexerPath()(implicit conf:Settings) = s"${AlertActor.classificationPath()}/label-indexer.ml"
  def alertClassifierPath()(implicit conf:Settings) = s"${AlertActor.classificationPath()}/classifier.ml"
  def ensureFolderExists()(implicit conf:Settings) = {
    val st = conf.getSparkStorage
    st.ensurePathExists(AlertActor.classificationPath)
  }

  def getLabelIndexer()(implicit conf:Settings) = StringIndexerModel.load(AlertActor.labelIndexerPath()) 
  
  def getAlertClassifier()(implicit conf:Settings) = {
    val storage = conf.getSparkStorage
    if(!storage.getNode(AlertActor.alertClassifierPath()).exists)
      throw new Exception(s"Cannot find alert classification model. Please train alerts")
    else
      OneVsRestModel.load(AlertActor.alertClassifierPath()) 
  }
  
  def fitLabelIndexer(alerts:Seq[TaggedAlert])(implicit conf:Settings)  = {
    val spark = conf.getSparkSession
    val storage = conf.getSparkStorage
    import spark.implicits._
    AlertActor.ensureFolderExists()
    assert(alerts.map(_.given_category).distinct.size > 1)
    
    new StringIndexer()
      .setInputCol("given_category")
      .setOutputCol("category_vector")
      .setHandleInvalid("keep")
      .fit(alerts.toDS)
      .write.overwrite()
      .save(AlertActor.labelIndexerPath())

       getLabelIndexer()
   }

  def finalTrain(alertsdf:DataFrame, run:AlertRun)(implicit conf:Settings) = {
    val r = new Random(20121205)
    val model = AlertActor.getModel(run)
    AlertActor.ensureFolderExists()
    model.fit(alertsdf.where(col("given_category")=!=lit("?")))
     .write.overwrite()
     .save(AlertActor.alertClassifierPath())
    
    getAlertClassifier()
  }

  def classifyAlerts(alerts:Seq[TaggedAlert])(implicit conf:Settings):Seq[TaggedAlert] = {
    val df = AlertActor.alerts2modeldf(alerts, reuseLabels = true) 
    classifyAlerts(df)
      .collect
      .toSeq
  }
  def classifyAlerts(alertdf:DataFrame)(implicit conf:Settings) = {
    val spark = conf.getSparkSession
    import spark.implicits._
    val model = getAlertClassifier()
    val predicted = model.transform(alertdf)

    val labelIndexer = AlertActor.getLabelIndexer()
    val labelConverter = new IndexToString()
      .setInputCol("predicted_vector")
      .setOutputCol("epitweetr_category")
      .setLabels(labelIndexer.labelsArray(0))

    labelConverter.transform(predicted)
      .select("id", "date", "topic", "country", "number_of_tweets", "topwords", "toptweets", "given_category", "epitweetr_category")
      .as[TaggedAlert]
  }
  def trainTest(alertsdf:DataFrame, run:AlertRun, trainRatio:Double)(implicit conf:Settings) = {
    val r = new Random(20121205)
    val joinedPredictions = 
      Seq.range(0, run.runs)
        .map(i => r.nextLong)
        .map{seed =>
           val Array(training, test) = alertsdf.randomSplit(Array(trainRatio, 1-trainRatio), seed)
           val model = AlertActor.getModel(run)
           val m = model.fit(training.where(col("given_category")=!=lit("?")))
           m.transform(test)
         }
         .reduce(_.union(_))
     
     val evaluator = 
       new MulticlassClassificationEvaluator()
         .setLabelCol("category_vector")
         .setPredictionCol("predicted_vector")
           
     AlertRun(
       ranking = 0,
       models = run.models,
       alerts = None,
       runs = run.runs,
       f1score = evaluator.setMetricName("f1").evaluate(joinedPredictions),
       accuracy = evaluator.setMetricName("accuracy").evaluate(joinedPredictions), 
       precision_by_class = evaluator.setMetricName("precisionByLabel").evaluate(joinedPredictions),
       sensitivity_by_class = evaluator.setMetricName("recallByLabel").evaluate(joinedPredictions),
       fscore_by_class = evaluator.setMetricName("fMeasureByLabel").evaluate(joinedPredictions),
       last_run = {
         val sdfDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
         val now = new java.util.Date()
         Some(sdfDate.format(now))
       },
       active = run.active,
       documentation = run.documentation,
       custom_parameters = run.custom_parameters 
     )
  }
  def alerts2modeldf(alerts:Seq[TaggedAlert], reuseLabels:Boolean)(implicit conf:Settings) =  {
     val spark = conf.getSparkSession
     implicit val storage = conf.getSparkStorage
     import spark.implicits._
     val indexer = 
       if(reuseLabels)
         AlertActor.getLabelIndexer()
       else 
         AlertActor.fitLabelIndexer(alerts)

     
     //Preparing Data
     Some(alerts)
       //Finding top words language
       .map{ case(alerts) => (alerts, 
         alerts.map{ta => 
           ta.toptweets
             .map{case (lang, tweets) => (lang, tweets.size)}
             .reduceOption((p1, p2) => (p1, p2) match {case ((lang1, count1), (lang2, count2)) => if(count1 > count2) p1 else p2})
             .map{case(lang, count) => lang}
             .getOrElse("en")
           }
       )}
       //Transforming into data frame
       .map{case (alerts, topwordlangs) => 
          alerts.zip(topwordlangs).map{
            case (TaggedAlert(id, date, topic, country, number_of_tweets, topwords, toptweets, given_category, _), twlang)
               => (id, date, topic, country, number_of_tweets, topwords, twlang, toptweets, given_category.orElse(Some("?")))
          }.toDF("id", "date", "topic", "country", "number_of_tweets", "topwords", "twlang", "toptweets", "given_category")
       }
       //Text vectorisation
       .map{df => 
         val langs = conf.languages.get.map(_.code)
         val baseCols = Seq("id", "date", "topic", "country", "number_of_tweets", "topwords", "twlang", "given_category", "toptweets")
         //Splitting tweets in different columns by language
         df.select((
           baseCols.map(c => col(c)) ++  
           Seq(lit("en").as("topic_lang"), lit("en").as("country_lang")) ++
           langs.map(lang => array_join(col("toptweets").getItem(lang), "\n").as(s"top_$lang")) ++ 
           langs.map(lang => lit(lang).as(s"toplang_$lang"))) 
           :_*
         )
	       //Vectorizing all columns by language
         .vectorize(
           languages = conf.languages.get, 
           reuseIndex = true, 
           indexPath = conf.langIndexPath, 
           textLangCols = Map("topic" -> Some("topic_lang"), "country"-> Some("country_lang"), "topwords"-> Some("twlang")) ++ Map(langs.map(lang => (s"top_$lang", Some(s"toplang_$lang"))):_*), 
           tokenizerRegex = conf.splitter
         )
	       //Adding all vectors on a single vector (translation to english of all vectors could be interesting for future)
         .select((
           baseCols.map(c => col(c)) ++
           Seq(
	             udf((row:Seq[Row]) => {
                 row.map(vectors => vectors.getAs[MLVector](0)).reduceOption(_.sum(_)).getOrElse(null)
               })
               .apply(
       	         concat(
       	           (Seq("topic_vec", "country_vec", "topwords_vec") ++ 
       	             langs.map(lang => s"top_${lang}_vec")
                   ).map(c => col(c))
       	           :_*
       	         )
               ).as("word_vec"),
             udf((count:Int)=> 1 - Math.pow(Math.E, -1.0*count/1000.0)).apply(col("number_of_tweets")).as("adjusted_count")
	         )):_*
         )
	       //Removing potentially empty alerts
         //.where(col("word_vec").isNotNull)
       }
       //Indexing text categories as multiclass vector
       .map{df => 
         indexer.transform(df)}
       //Joining vectors with the numver of tweets
       .map{df => 
           (new VectorAssembler())
             .setInputCols(Array("word_vec", "adjusted_count"))
             .setOutputCol("features")
             .transform(df)
        }
	.get
	.select("id", "date", "topic", "country", "number_of_tweets", "topwords", "toptweets", "given_category", "features", "category_vector")
	.as[(String, String, String, String, Int, String, Map[String, Seq[String]], String, MLVector, Double)]
	.toDF
  }
}



