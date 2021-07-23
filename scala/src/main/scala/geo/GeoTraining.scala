package org.ecdc.epitweetr.geo

import demy.storage.{Storage}
import demy.util.{log => l, util}
import org.ecdc.epitweetr.Settings
import org.ecdc.twitter.Language
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, ArrayType, StringType}
import org.apache.spark.sql.functions.{col, lit, udf, concat, array_join, struct, sum}
import org.apache.spark.ml.linalg.{Vectors, DenseVector, Vector=>MLVector}
import demy.mllib.linalg.implicits._
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import scala.util.Random
import org.ecdc.twitter.Language.LangTools

case class TaggedChunk(tokens:Seq[String], isEntity:Boolean) {
  def split(that:TaggedChunk, tryed:Int = 0):Seq[TaggedChunk] = {
    if(!this.isEntity && that.isEntity) {
      var start = 0
      var indexes = this.findInText(that.tokens, start, caseInsensitive = tryed > 0)
      var ret = Iterator.continually{
          indexes.map{case (iFrom, iTo, isEntity) =>
            val head = if(iFrom > start) {
                Seq(TaggedChunk(this.tokens.slice(start, iFrom), false))
              } else {
                Seq[TaggedChunk]()
              }
            start = iTo
            indexes = this.findInText(that.tokens, start, caseInsensitive = tryed > 0)
            head :+ TaggedChunk(this.tokens.slice(iFrom, iTo), isEntity) 
          }
        }.takeWhile(!_.isEmpty)
        .flatMap(e => e)
        .flatMap(e => e)
        .toSeq
      if(ret.filter(_.isEntity).size == 0 && tryed == 0)
        ret = this.split(that, tryed = 1)
      ret 
    } else
      Seq(this)
  }

  def findInText(entity:Seq[String], start:Int, caseInsensitive:Boolean) = {
    if(start >= this.tokens.size)
      None
    else {
      val index = this.indexOf(entity, start, caseInsensitive)
      if(!index.isEmpty)
        Some((index.get, index.get + entity.size, true))
      else 
        Some((start, this.tokens.size, false))
    }
  }

  def indexOf(entity:Seq[String], start:Int, caseInsensitive:Boolean):Option[Int] = {
    val matches = for(i <- Iterator.range(start, this.tokens.size)) yield {
      var matching = 0
      if(i + entity.size <= this.tokens.size) {
        for(j <- Iterator.range(0, entity.size)) {
          if((!caseInsensitive && this.tokens(i+j) == entity(j))
              || (caseInsensitive && this.tokens(i+j).toLowerCase == entity(j).toLowerCase)
            )
            matching = matching + 1
        }
      }
      if(matching == entity.size)
        Some(i)
      else
        None
    }
    matches.toSeq.flatMap(e => e).headOption
  }
}
object TaggedChunk {
  def apply(chunk:String, isEntity:Boolean, splitter:String):TaggedChunk = TaggedChunk(tokens = chunk.split(splitter).filter(_.size > 0), isEntity = isEntity)
  def fromBIO(tokens:Seq[String], bioTags:Seq[String]) = {
    if(tokens.size != bioTags.size) {
      l.msg(s"Unexpectedly, $tokens and $bioTags have different length \n${tokens.size} and ${bioTags.size}, maybe after split some new stop words arose and where elimitated")
      Iterator[TaggedChunk]()
    }
    else {
      var nextStart = 0
      for{
        i <- Iterator.range(0, tokens.size)
        if i == nextStart
      } yield {
        if(bioTags(i) == "O") {
          nextStart = Iterator.range(i+1, tokens.size).dropWhile(j => bioTags(j) != "B").toSeq.headOption.getOrElse(tokens.size)
          TaggedChunk(tokens = tokens.slice(i, nextStart), isEntity = false) 
        } else {
          nextStart = Iterator.range(i+1, tokens.size).dropWhile(j => bioTags(j) == "I").toSeq.headOption.getOrElse(tokens.size)
          TaggedChunk(tokens = tokens.slice(i, nextStart), isEntity = true) 
        }
      }
    }
  }
}
case class ConfusionMatrix(tp:Int, fp:Int, tn:Int, fn:Int) {
  def sum(that:ConfusionMatrix) = ConfusionMatrix(tp = this.tp + that.tp, fp = this.fp + that.fp, tn = this.tn + that.tn, fn = this.fn + that.fn) 
}
case class TestResult(test:String, text:String, tagged:Seq[String], predicted:Seq[String], tp:Int, fp:Int, tn:Int, fn:Int) {
  def sum(that:ConfusionMatrix) = ConfusionMatrix(tp = this.tp + that.tp, fp = this.fp + that.fp, tn = this.tn + that.tn, fn = this.fn + that.fn) 
}
object ConfusionMatrix {
  def apply(tagged:TaggedText, predicted:TaggedText):ConfusionMatrix = {
    val predictedEntities = predicted.taggedChunks.filter(_.isEntity).toSet
    val taggedEntities = tagged.taggedChunks.filter(_.isEntity).toSet
    val isTP = predictedEntities.size > 0 && taggedEntities == predictedEntities
    val isFP = predictedEntities.size > 0  && taggedEntities != predictedEntities
    val isTN = predictedEntities.size == 0   && taggedEntities.size == 0
    val isFN = predictedEntities.size == 0 && taggedEntities.size > 0
    ConfusionMatrix(
      tp = if(isTP) 1 else 0,
      fp = if(isFP) 1 else 0,
      tn = if(isTN) 1 else 0,
      fn = if(isFN) 1 else 0
    )
  } 
}

case class TaggedText(id:String, taggedChunks:Seq[TaggedChunk], lang:Option[String]) {
  def mergeWith(that:TaggedText) = {
    assert(this.id == that.id)
    val thatPositives = that.taggedChunks.filter(_.isEntity)
    TaggedText(
      id = this.id, 
      taggedChunks = this.taggedChunks.flatMap(tc => thatPositives.foldLeft(Seq(tc))((curr, iter)=> curr.flatMap(c => c.split(iter)))),
      lang = this.lang
    )
  }
  def toBIO()(implicit conf:Settings):Iterator[BIOAnnotation] = toBIO(nBefore = conf.geoNBefore, nAfter = conf.geoNAfter)
  def toBIO(nBefore:Int, nAfter:Int):Iterator[BIOAnnotation]= {
    for{i <- Iterator.range(0, this.taggedChunks.size)
      j <- Iterator.range(0, this.taggedChunks(i).tokens.size) } 
       yield {
        BIOAnnotation(
          tag = 
            if(this.taggedChunks(i).isEntity && j == 0) "B"
            else if(this.taggedChunks(i).isEntity) "I"
            else "O",
          token = this.taggedChunks(i).tokens(j),
          before = {
            var count = nBefore
            for{ii <- Seq.range(0, this.taggedChunks.size).reverse
              jj <- Seq.range(0, this.taggedChunks(ii).tokens.size).reverse 
              if count > 0 && (ii < i || i == ii && jj < j)
            } yield {
              count = count - 1
              this.taggedChunks(ii).tokens(jj)
            }
          },
          after = {
            var count = nAfter
            for{ii <- Seq.range(0, this.taggedChunks.size)
              jj <- Seq.range(0, this.taggedChunks(ii).tokens.size)
              if count > 0 && (ii > i || (i == ii && jj > j))
            } yield {
              count = count - 1
              this.taggedChunks(ii).tokens(jj)
            }
          },
          lang = lang
        )
    }
  }

  def getTokens = taggedChunks.flatMap(tc => tc.tokens)
}
object TaggedText {
  def apply(id:String, lang:Option[String], tokens:Seq[String], bioTags:Seq[String]):TaggedText = {
    val chunks = TaggedChunk.fromBIO(tokens = tokens, bioTags = bioTags )
    TaggedText(id = id, taggedChunks = chunks.toSeq, lang = lang)
  }
}

case class BIOAnnotation(tag:String, token:String, before:Seq[String], after:Seq[String], lang:Option[String]) {
  def forceLangTo(newLang:Option[String]) = BIOAnnotation(tag = tag, token=token, before = before, after = after, lang = newLang)
}

case class GeoTrainings(items:Seq[GeoTraining])

case class GeoTrainingSource(value:String)
object GeoTrainingSource {
  def apply(value:String):GeoTrainingSource = value.toLowerCase.replace(" ", "-") match {
    case "tweet" => GeoTrainingSource.tweet
    case "epitweetr-model" => GeoTrainingSource.epitweetrModel
    case "epitweetr-database" => GeoTrainingSource.epitweetrDatabase
    case _ => GeoTrainingSource.manual
  }
  val tweet = new GeoTrainingSource("tweet") 
  val epitweetrModel = new GeoTrainingSource("epitweetr-model") 
  val epitweetrDatabase = new GeoTrainingSource("epitweetr-database") 
  val manual = new GeoTrainingSource("manual")

}

case class GeoTrainingPart(value:String)
object GeoTrainingPart {
  def apply(value:String) = value.toLowerCase.replace(" ", "-") match {
    case "text" => GeoTrainingPart.text
    case "user-location" => GeoTrainingPart.userLocation
    case "user-description" => GeoTrainingPart.userDescription
    case _ => new GeoTrainingPart(value)
  }
  val text = new GeoTrainingPart("text") 
  val userLocation = new GeoTrainingPart("user-location") 
  val userDescription = new GeoTrainingPart("user-description")
}
case class GeoTraining(
  category:String,  
  text:String,  
  locationInText:Option[String], 
  isLocation:Option[Boolean],  
  forcedLocationCode:Option[String], 
  forcedLocationName:Option[String], 
  source:GeoTrainingSource,
  tweetId:Option[String],
  lang:Option[String],
  tweetPart:Option[GeoTrainingPart], 
  foundLocation:Option[String], 
  foundLocationCode:Option[String], 
  foundCountryCode:Option[String]
) {
  def toTaggedText(splitter:String) = {
    isLocation.map{isLocationAnnotation =>
      if(!isLocationAnnotation)
        TaggedText(id = this.id(), taggedChunks = Seq(TaggedChunk(chunk = text, isEntity = false, splitter = splitter)), lang = lang)
      else if(locationInText.isEmpty || locationInText.get.trim.size == 0) 
        TaggedText(id = this.id(), taggedChunks = Seq(TaggedChunk(chunk = text, isEntity = true, splitter = splitter)), lang = lang)
      else {//is Location and a text has been annotated
        TaggedText(
          id = this.id(), 
          taggedChunks = TaggedChunk(chunk = text, isEntity = false, splitter = splitter).split(TaggedChunk(chunk = locationInText.get, isEntity = true, splitter = splitter)),
          lang = lang
        )
      }
    }
  }
  
  def id() = {
    val md = java.security.MessageDigest.getInstance("SHA-1");
    val enc = new sun.misc.BASE64Encoder()
    enc.encode(md.digest(s"$category.$text.$tweetId.$tweetPart".getBytes))
  }  
}
object GeoTraining {
  def toTaggedText(annotations:Seq[GeoTraining])(implicit conf:Settings):Seq[TaggedText] = toTaggedText(annotations = annotations, splitter = conf.splitter, nBefore = conf.geoNBefore, nAfter = conf.geoNAfter)
  def toTaggedText(annotations:Seq[GeoTraining], splitter:String, nBefore:Int, nAfter:Int):Seq[TaggedText] = { 
    annotations.map(g => g.toTaggedText(splitter))
      .flatMap(d => d)
      .groupBy(t => t.id)
      .mapValues(geos => geos.reduce((t1, t2) => t1.mergeWith(t2)))
      .values
      .toSeq
  }

  def getTrainingTestSets(annotations:Seq[GeoTraining], trainingRatio:Double, splitter:String, nBefore:Int, nAfter:Int) = { 
    val annotated = annotations.filter(gt => !gt.isLocation.isEmpty)
    val l = 10000
    Seq(
      (
        "Tweet texts", 
        annotated.filter(gt => gt.source == GeoTrainingSource.tweet && gt.tweetPart == Some(GeoTrainingPart.text) && Math.abs(gt.id().hashCode) % l < trainingRatio * l),
        annotated.filter(gt => gt.source != GeoTrainingSource.tweet || gt.tweetPart != Some(GeoTrainingPart.text) || Math.abs(gt.id().hashCode) % l >= trainingRatio * l)
      ),(
        "Tweet location", 
        annotated.filter(gt => gt.source == GeoTrainingSource.tweet  && gt.tweetPart == Some(GeoTrainingPart.userLocation) && Math.abs(gt.id().hashCode) % l < trainingRatio * l),
        annotated.filter(gt => gt.source != GeoTrainingSource.tweet  || gt.tweetPart != Some(GeoTrainingPart.userLocation) || Math.abs(gt.id().hashCode) % l >= trainingRatio * l)
      ),(
        "Tweet user description", 
        annotated.filter(gt => gt.source == GeoTrainingSource.tweet && gt.tweetPart == Some(GeoTrainingPart.userDescription) && Math.abs(gt.id().hashCode) % l < trainingRatio * l),
        annotated.filter(gt => gt.source != GeoTrainingSource.tweet || gt.tweetPart != Some(GeoTrainingPart.userDescription) || Math.abs(gt.id().hashCode) % l >= trainingRatio * l)
      )/*,(
        "Model Location", 
        annotated.filter(gt => gt.source == GeoTrainingSource.epitweetrModel && gt.category.toLowerCase == "location" && Math.abs(gt.id().hashCode) % l < trainingRatio * l),
        annotated.filter(gt => gt.source != GeoTrainingSource.epitweetrModel || gt.category.toLowerCase != "location" || Math.abs(gt.id().hashCode) % l >= trainingRatio * l)
      )*/
    ).map{case(t, test, train) => 
      (
        t, 
        GeoTraining.toTaggedText(test, splitter, nBefore, nAfter), 
        GeoTraining.toTaggedText(train, splitter, nBefore, nAfter)
      )}
  }

  def BIO2TrainData(bio:Seq[BIOAnnotation])(implicit storage:Storage, conf:Settings, spark:SparkSession):DataFrame = {
    BIO2TrainData(bio = bio, languages = conf.languages.get, splitter = conf.splitter, langIndexPath = conf.langIndexPath, nBefore = conf.geoNBefore, nAfter = conf.geoNAfter)
  }
  def BIO2TrainData(bio:Seq[BIOAnnotation], languages:Seq[Language], splitter:String, langIndexPath:String, nBefore:Int, nAfter:Int)(implicit storage:Storage, spark:SparkSession):DataFrame = {
    import spark.implicits._
    bio.toDS
    .select(
      col("tag"), 
      col("token"), 
      array_join(col("before"), " ").as("before"),
      array_join(col("after"), " ").as("after"),
      col("lang")
    )
    .vectorize(
      languages = languages, 
      reuseIndex = true, 
      indexPath = langIndexPath, 
      textLangCols = Map("token"->Some("lang"), "before" -> Some("lang"), "after" -> Some("lang")), 
      tokenizerRegex = splitter
    )
    .select("tag", "token", "before", "after", "lang", "token_vec", "before_vec", "after_vec")
    .where(udf((rvector:Seq[Row], rbefore:Seq[Row], rafter:Seq[Row], token:String, tBefore:String, tAfter:String) => {
      //excluding some weird situations where token has no vector TODO:Find why 
      if(rvector.size == 0) {
        l.msg(s"ignoring -->token<-- within in ${tBefore} -->${token}<-- ${tAfter}... if you do not see this message for too many rows you are safe to go")
        false
      } else if(//excluding when there are no vectors found for the current language
          (rvector.map(r => r.getAs[DenseVector]("vector"))
            ++ rbefore.map(r => r.getAs[DenseVector]("vector"))
            ++ rafter.map(r => r.getAs[DenseVector]("vector"))
          ).forall(v => v.values.forall(_ == 1.0))
        ) {
        false
      }
      else
        true
      }).apply(col("token_vec"), col("before_vec"), col("after_vec"), col("token"), col("before"), col("after"))
    )
    .select(col("token"), col("tag"),col("lang"), 
      udf((tag:String) => if(tag == "B") 1.0 else 0.0).apply(col("tag")).as("BLabel"),
      udf((tag:String) => if(tag == "I") 1.0 else 0.0).apply(col("tag")).as("ILabel"),
      udf((tag:String) => if(tag == "O") 1.0 else 0.0).apply(col("tag")).as("OLabel"),
      udf((rvector:Seq[Row], rbefore:Seq[Row], rafter:Seq[Row], nBefore:Int, nAfter:Int) => {
        val vector = rvector(0).getAs[DenseVector]("vector")
        val before = rbefore.map(r => r.getAs[DenseVector]("vector"))
        val after = rafter.map(r => r.getAs[DenseVector]("vector"))
        GeoTraining.getContextVector(vector, before.iterator, after.iterator, nBefore, nAfter)
      }).apply(col("token_vec"), col("before_vec"), col("after_vec"), lit(nBefore), lit(nAfter)).as("vector")
    ) 
  }

  def trainedLanguages()(implicit conf:Settings, storage:Storage) = {
    conf.languages.get.filter{lang => 
       Seq("B", "I")
         .forall(t => /*!l.areVectorsNew() &&*/ storage.getNode(s"${lang.modelPath}.${t}").exists)
    }
  }
  def getModels(testId:Option[String]=None, languages:Seq[Language])(implicit storage:Storage) = { 
    languages
      .flatMap(lang => Seq("B", "I").map(t => (t, lang)))
      .flatMap{case (t, lang) => 
         val path = s"${lang.modelPath}.${t}${testId.map(tid => "."+tid.toLowerCase.replace(" ", "-")).getOrElse("")}"
         if(storage.getNode(path).exists && !lang.areVectorsNew()) 
         {
           //l.msg(s"loading $path") 
             Some(s"$t.${lang.code}", LinearSVCModel.load(path))
         } else {
            throw new Exception(s"Cannot get a tagging model in $path or vectors are new. Please retrain")
         }
      }.toMap
  }

  def trainModels(annotations:Seq[BIOAnnotation])(implicit storage:Storage, spark:SparkSession, conf:Settings):Map[String, LinearSVCModel] = {
    trainModels(annotations = annotations, testId = None) 

  }

  def trainModels(annotations:Seq[BIOAnnotation], testId:Option[String])(implicit storage:Storage, spark:SparkSession, conf:Settings):Map[String, LinearSVCModel] = {
    trainModels(annotations = annotations, testId = testId, languages = conf.languages.get, splitter = conf.splitter, nBefore = conf.geoNBefore, nAfter = conf.geoNAfter, langIndexPath = conf.langIndexPath) 
  }

  def trainModels(annotations:Seq[BIOAnnotation], testId:Option[String]=None, languages:Seq[Language], splitter:String, nBefore:Int, nAfter:Int, langIndexPath:String)
    (implicit storage:Storage, spark:SparkSession):Map[String, LinearSVCModel] = {
    val models = 
      languages
        .flatMap(lang => Seq("B", "I").map(t => (t, lang)))
        .flatMap{case (t, lang) => 
           val path = s"${lang.modelPath}.${t}${testId.map(tid => "."+tid.toLowerCase.replace(" ", "-")).getOrElse("")}"
           import spark.implicits._
           val trainData = {
             //util.checkpoint(
               //ds = {
                 val langAnnotations = annotations.filter(bio => bio.lang.isEmpty || bio.lang == Some(lang.code)).map(bio => bio.forceLangTo(Some(lang.code)))
                 GeoTraining.BIO2TrainData(bio = rescale(langAnnotations), languages = languages, splitter = splitter, langIndexPath = langIndexPath, nBefore = nBefore, nAfter = nAfter)
               //},
               //path = storage.getTmpNode(sandboxed = true),
               //reuse = false
             }.cache
           //trainData.show(1000)
           val model = new LinearSVC().setFeaturesCol("vector").setLabelCol(s"${t}Label")
           val ret = if(trainData.count > 0) {
             //trainData.select(sum("BLabel"), sum("ILabel"), sum("OLabel")).show
             l.msg(s"Training model for $path")
             val trained = model.fit(trainData)
             trained.write.overwrite().save(path)
             //Setting update timestamp
             storage.isUnchanged(path = Some(path), checkPath = Some(s"${path}.stamp"), updateStamp = true)  
             Some(s"$t.${lang.code}", trained)
           } else {
             None
           }
           trainData.unpersist
           //storage.removeMarkedFiles()
           ret
        }
        .toMap
    models
  }
  def rescale(annotations:Seq[BIOAnnotation]) = {
    val all = annotations 
    //rescaling to avoid getting to imbalanced classes
    val bb = all.filter(n => n.tag == "B")
    val ii = all.filter(n => n.tag == "I")
    val oo = all.filter(n => n.tag == "O")
    val toTrain = 
      if(oo.size > 2*(bb.size + ii.size)) {
        val r = new Random(1234)
        val newO = r.shuffle(oo).take(2*(bb.size + ii.size))
        l.msg(s"limiting O:(${oo.size}) vs  B+I = ${(bb.size + ii.size)} to ${newO.size}")
        bb ++ ii ++ newO
      }
      else {
        //l.msg(s"no need to split  O:(${oo.size}) vs  B+I = ${(bb.size + ii.size)}")
        all
      }
    toTrain

  }
  def splitTrainEvaluate(
    annotations:Seq[GeoTraining],
    trainingRatio:Double
  ) (implicit spark:SparkSession, storage:Storage, conf:Settings) = {
    import spark.implicits._
    val tests = GeoTraining.getTrainingTestSets(annotations = annotations, trainingRatio = trainingRatio, splitter = conf.splitter, nBefore = conf.geoNBefore, nAfter = conf.geoNAfter) 
    val testNames = tests.map(_._1) 
    val testSets =   tests.map(_._2)
    val trainingSets = tests.map(_._3)
    (for(i <- Iterator.range(0, testNames.size)) yield {
      val testName = testNames(i) 
      val models = GeoTraining.trainModels(annotations = trainingSets(i).flatMap(tt => tt.toBIO(nBefore = conf.geoNBefore, nAfter = conf.geoNAfter)), testId = Some(testName))
      val testTexts = testSets(i)
        .toDS
        .select(udf((id:String, taggedChunks:Seq[TaggedChunk], lang:String) => TaggedText(id, taggedChunks, if(lang==null)None else Some(lang))).apply(col("id"), col("taggedChunks"), col("lang")).as("tagged"))
        .select(
          col("tagged"),
          udf((lang:String) => lang).apply(col("tagged.lang")).as("lang"),
          udf((taggedText:TaggedText)=>taggedText.getTokens).apply(col("tagged")).as("tokens")
        )
        .withColumn("text", array_join(col("tokens"), " "))
        .vectorize(
          languages = conf.languages.get, 
          reuseIndex = true, 
          indexPath = conf.langIndexPath, 
          textLangCols = Map("text"->Some("lang")), 
          tokenizerRegex = conf.splitter
        )

      GeoTraining.predictBIO(
        df = testTexts, 
        vectorsCols = Seq("text_vec"), 
        langCols = Seq("lang"), 
        tagCols = Seq("text_tag"), 
        models = models, 
        nBefore = conf.geoNBefore, 
        nAfter = conf.geoNAfter
      )
      .withColumn(
        "predicted", 
        udf((id:String, lang:String, tokens:Seq[String], bioTags:Seq[String]) => 
           TaggedText(id, if(lang == null) None else Some(lang), tokens, bioTags) 
        ).apply(col("tagged.id"), col("lang"), col("tokens"), col("text_tag"))
      ).withColumn("confMatrix", udf((tagged:TaggedText, predicted:TaggedText) => ConfusionMatrix(tagged = tagged, predicted = predicted)).apply(col("tagged"), col("predicted")))
      .select(
        lit(testName).as("test"),
        col("text"),
        udf((tagged:TaggedText) => tagged.taggedChunks.filter(_.isEntity).map(_.tokens.mkString(" "))).apply(col("tagged")).as("tagged"),
        udf((predicted:TaggedText) => predicted.taggedChunks.filter(_.isEntity).map(_.tokens.mkString(" "))).apply(col("predicted")).as("predicted"),
        col("confMatrix.tp"),
        col("confMatrix.fp"),
        col("confMatrix.tn"),
        col("confMatrix.fn"),
      ).as[TestResult]
    }).reduce(_.union(_))
  }

  def getContextVectors(vectors:Seq[DenseVector], nBefore:Int, nAfter:Int) = {
    var vSize = -1
    var vNone = null.asInstanceOf[DenseVector]
    var vBefore = null.asInstanceOf[DenseVector]
    var vAfter = null.asInstanceOf[DenseVector]
    vectors.iterator.zipWithIndex.map{case (v, i) => 
      val before = Iterator.range(if(i - nBefore < 0) 0 else i - nBefore, i).toSeq.reverse.iterator.map(j => vectors(j))
      val after = Iterator.range(i + 1, if(i + 1 + nAfter > vectors.size) vectors.size else i + nAfter +1).map(j => vectors(j))
      getContextVector(v, before, after, nBefore, nAfter)
    }
  }

  def getContextVector(vector:DenseVector, before:Iterator[DenseVector], after:Iterator[DenseVector], nBefore:Int, nAfter:Int) = {
    val vSize = vector.size
    val vBefore = before.zipWithIndex.map{case (v, i) => v.scale(1.0 - i.toDouble/nBefore)}.foldLeft(Vectors.dense(Array.fill(vSize)(0.0)))((curr, iter)=> curr.sum(iter))
    val vAfter = after.zipWithIndex.map{case (v, i) => v.scale(1.0 - i.toDouble/nAfter)}.foldLeft(Vectors.dense(Array.fill(vSize)(0.0)))((curr, iter)=> curr.sum(iter))
    new DenseVector(vector.values ++ vAfter.sum(vBefore).asInstanceOf[DenseVector].values)
    //vector.sum(vAfter.sum(vBefore))
  }

  def predictBIO(df:DataFrame, vectorsCols:Seq[String], langCols:Seq[String], tagCols:Seq[String])(implicit storage:Storage, conf:Settings):DataFrame =  {
    val models = GeoTraining.getModels(languages = conf.languages.get)
    predictBIO(df = df, vectorsCols = vectorsCols, langCols = langCols, tagCols = tagCols, models = models, nBefore = conf.geoNBefore, nAfter = conf.geoNAfter)
  }
  def predictBIO(df:DataFrame, vectorsCols:Seq[String], langCols:Seq[String], tagCols:Seq[String], languages:Seq[Language], langIndexPath:String, splitter:String, nBefore:Int, nAfter:Int)
  (implicit storage:Storage):DataFrame =  {
    val models = GeoTraining.getModels(languages = languages)
    predictBIO(df = df, vectorsCols = vectorsCols, langCols = langCols, tagCols = tagCols, models = models, nBefore = nBefore, nAfter = nAfter)
  }

  def predictBIO(df:DataFrame, vectorsCols:Seq[String], langCols:Seq[String], tagCols:Seq[String], models:Map[String, LinearSVCModel], nBefore:Int, nAfter:Int):DataFrame =  {
    val spark = df.sparkSession
    val bModels = spark.sparkContext.broadcast(models)
    assert(vectorsCols.size == langCols.size)
    Some(
      df.rdd.mapPartitions{iter => 
        val rawPredict = 
          bModels.value.mapValues{model => 
            val method = model.getClass.getDeclaredMethod("predictRaw", classOf[MLVector])
            method.setAccessible(true) 
            (model, method)
          }

        iter.map{row =>
           Some(
             vectorsCols.zip(langCols)
               .map{case (vectorsCol, langCodeCol) =>
                 Some((row.getAs[String](langCodeCol), row.getAs[Seq[Row]](vectorsCol).map(r => r.getAs[DenseVector]("vector"))))
                   .map{case(langCode, vectors) =>
                     if(!rawPredict.contains(s"B.$langCode")) {
                       vectors.zipWithIndex.map{case(_, i) => if(i==0) "B" else "I"}
                     }
                     else 
                       GeoTraining.getContextVectors(vectors, nBefore, nAfter).map{vector => 
                         Seq("B", "I").flatMap{ t =>
                           rawPredict.get(s"$t.$langCode")
                             .map{case (model, method) =>
                               if(vector == null)
                                 throw new Exception("Null vectors are not supported anymore")
                               else
                                 Some(method.invoke(model, vector).asInstanceOf[DenseVector])
                                   .map{scores =>
                                     val (yes, no) = (scores(1), scores(0))
                                     if(no>=0 && yes>=0) yes/(no + yes)
                                     else if(no>=0 && yes<=0) 0.5 - Math.atan(no-yes)/Math.PI
                                     else if(no<=0 && yes>=0) 0.5 + Math.atan(yes-no)/Math.PI
                                     else no/(no + yes)
                                   }
                                   .get
                             }
                             .map{tScore =>  /*println(t, tScore);*/(t, tScore)}
                           }.foldLeft(("O", 0.4999))((curr, iter)=> if(curr._2 > iter._2) curr else iter )
                         }
                         .map(_._1)
                         .toSeq
                   }
                   .get
               }
             )
             .map(tags => Row.fromSeq(row.toSeq ++ tags))
             .get
        }
      })
      .map{rdd => 
        spark.createDataFrame(rdd, StructType(fields = df.schema.fields ++ tagCols.map(colName => StructField(colName , ArrayType(StringType, true), true))))
      }
      .get
  }

  def predictEntity(df:DataFrame, textCols:Seq[String], vectorsCols:Seq[String], langCols:Seq[String], entCols:Seq[String])(implicit storage:Storage, conf:Settings):DataFrame =  {
    val models = GeoTraining.getModels(languages = conf.languages.get)
    predictEntity(df = df, textCols = textCols, vectorsCols = vectorsCols, langCols = langCols, entCols = entCols, models = models, nBefore = conf.geoNBefore, nAfter = conf.geoNAfter, splitter = conf.splitter)

  }
  def predictEntity(df:DataFrame, textCols: Seq[String], vectorsCols:Seq[String], langCols:Seq[String], entCols:Seq[String], languages:Seq[Language], langIndexPath:String, splitter:String, nBefore:Int, nAfter:Int)(implicit storage:Storage):DataFrame =  {
    val models = GeoTraining.getModels(languages = languages)
    predictEntity(df = df, textCols = textCols, vectorsCols = vectorsCols, langCols = langCols, entCols = entCols, models = models, nBefore = nBefore, nAfter = nAfter, splitter = splitter)
  }
  def predictEntity(df:DataFrame, textCols: Seq[String], vectorsCols:Seq[String], langCols:Seq[String], entCols:Seq[String], models:Map[String, LinearSVCModel], nBefore:Int, nAfter:Int, splitter:String)
    :DataFrame =  {
     val bioCols = entCols.map(c => s"${c}_bio")
   
     val df0 = predictBIO(df = df, vectorsCols = vectorsCols, langCols = langCols, tagCols = bioCols , models = models, nAfter=nAfter, nBefore = nBefore)
      .select(
        Seq(col("*")) ++ 
          textCols.zip(bioCols.zip(entCols)).map{case (textCol, (bioCol, entCol)) =>
            udf((text:String, bioTags:Seq[String], splitter:String) => 
               if(text == null) 
                 null
               else
                 TaggedChunk.fromBIO(text.split(splitter).filter(_.size > 0), bioTags)
                   .filter(c => c.isEntity)
                   .map(c => c.tokens.mkString(" "))
                   .toSeq
                   .headOption
                   .getOrElse(null)
            ).apply(col(textCol), col(bioCol), lit(splitter)).as(entCol)
          } :_*
      )
      //df0.show(1000)
      df0.drop(bioCols :_*)
  }
}
