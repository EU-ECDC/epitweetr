package org.ecdc.epitweetr.test.geo

import org.apache.spark.ml.linalg.{Vectors, DenseVector, Vector=>MLVector}
import org.apache.spark.sql.{Row}
import demy.storage.{Storage}
import org.ecdc.epitweetr.geo._
import org.ecdc.epitweetr.test.UnitTest
import org.ecdc.epitweetr.Settings
import demy.mllib.linalg.implicits._
import Geonames.Geolocate

trait GeonamesTest extends UnitTest {
   "Geonames" should "find places without vectors" in {
     assume(!sys.env.get("EPI_HOME").isEmpty)
     implicit val conf = Settings(sys.env("EPI_HOME"))
     conf.load
     implicit val storage = Storage.getSparkStorage  
     implicit val spark = getSpark
     import spark.implicits._
     val geonames = conf.geonames
     val simpleText =  geonames.geolocateText(text=Seq("Viva Chile"), reuseGeoIndex = true, maxLevDistance = 0, minScore = 0, nBefore = 4, nAfter = 4)
     simpleText.show
     assert(simpleText.count > 0)
   }
   it should "find places with vectors" in {
     implicit val conf = Settings(sys.env("EPI_HOME"))
     conf.load
     implicit val storage = Storage.getSparkStorage  
     implicit val spark = getSpark
     import spark.implicits._
     val geonames = conf.geonames
     val vectorText =  
       geonames.geolocateText(
         text=Seq("Siempre he pensado que los pajaros de Santiago de Chile son los mas bellos"), 
         lang = Seq("es"), 
         reuseGeoIndex = true, 
         maxLevDistance = 0, 
         minScore = 0, 
         nBefore = 4, 
         nAfter = 4, 
         langs = conf.languages.get.filter(_.code == "es"), 
         langIndexPath = conf.langIndexPath
       )
     vectorText.show
     assert(vectorText.count > 0)
   }


}

