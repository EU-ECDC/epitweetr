package org.ecdc.epitweetr.test

import org.apache.spark.ml.linalg.{Vectors, DenseVector, Vector=>MLVector}
import org.apache.spark.sql.{Row}
import demy.storage.{Storage}
import org.ecdc.twitter._
import org.ecdc.epitweetr.Settings
import demy.mllib.linalg.implicits._

trait LanguageTest extends UnitTest {
   "Languages" should "vectorize common words to not null vectors" in {
     assume(!sys.env.get("EPI_HOME").isEmpty)
     implicit val conf = Settings(sys.env("EPI_HOME"))
     conf.load
     implicit val storage = Storage.getSparkStorage  
     implicit val spark = getSpark
     import spark.implicits._
     val Array(france) = Language.vectorizeText(
       text = Seq("Francia")
       , lang = Seq("es")
       , langs = conf.languages.get
       , indexPath = conf.langIndexPath
     ).select("text_vec")
     .take(1)
     .map(r => r.getAs[Seq[Row]]("text_vec")(0).getAs[DenseVector]("vector"))
     
     assert(france != null)
   }
   it should "be case sensitive" in {
     implicit val conf = Settings(sys.env("EPI_HOME"))
     conf.load
     implicit val storage = Storage.getSparkStorage  
     implicit val spark = getSpark
     import spark.implicits._
     val Array(fr1, fr2) = Language.vectorizeText(
       text = Seq("Francia", "francia")
       , lang = Seq("es", "es")
       , langs = conf.languages.get
       , indexPath = conf.langIndexPath
     ).select("text_vec")
     .take(2)
     .map(r => r.getAs[Seq[Row]]("text_vec")(0).getAs[DenseVector]("vector"))
     
     assert(fr1 != null && fr2 != null && fr1.cosineSimilarity(fr2) < 0.999)

   }
   it should "produce default vectors when not found" in {
     implicit val conf = Settings(sys.env("EPI_HOME"))
     conf.load
     implicit val storage = Storage.getSparkStorage  
     implicit val spark = getSpark
     import spark.implicits._
     val df =  Language.vectorizeText(
       text = Seq("LLJKHTDFGXSRFGVjhfgghfgsklsjh45678é")
       , lang = Seq("es")
       , langs = conf.languages.get
       , indexPath = conf.langIndexPath
     ).select("text_vec")
     val Array(nn) = df
     .take(1)
     .map(r => r.getAs[Seq[Row]]("text_vec")(0).getAs[DenseVector]("vector"))
     assert(nn != null && conf.languages.get(0).getUnknownVector(conf.languages.get(0).getVectorsSize()).cosineSimilarity(nn) > 0.999)
   }


}
