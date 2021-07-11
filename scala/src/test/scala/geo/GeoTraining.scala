package org.ecdc.epitweetr.test.geo
import demy.storage.Storage
import demy.util.{log => l}
import org.ecdc.twitter.JavaBridge
import org.apache.spark.sql.{SparkSession, Dataset}

import org.ecdc.epitweetr.test.UnitTest
import org.ecdc.epitweetr.geo._
import org.ecdc.epitweetr.Settings

trait GeoTrainingTest extends UnitTest {
  lazy val s = Settings.defaultSplitter
  lazy val trainingSet = {
    val spark = getSpark
    import spark.implicits._

    Seq(
      GeoTraining(category = "Text", text = "Se caen los edifivion en Los Angeles", locationInText = Some("Los Angeles"), isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = Some("001"), lang = Some("es"), tweetPart = Some(GeoTrainingPart.text), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Text", text = "Lo mejor del mundo es reir sin saber porqué", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = Some("002"), lang = Some("es"), tweetPart = Some(GeoTrainingPart.text), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Text", text = "No sé si prefiero Australia o Nueva Zelandia", locationInText = Some("Australia"), isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = Some("003"), lang = Some("es"), tweetPart = Some(GeoTrainingPart.text), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Text", text = "No sé si prefiero Australia o Nueva Zelandia", locationInText = Some("Nueva Zelandia"), isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = Some("003"), lang = Some("es"), tweetPart = Some(GeoTrainingPart.text), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Text", text = "Soy una persona muy especial", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = None, lang = Some("es"), tweetPart = Some(GeoTrainingPart.userDescription), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Text", text = "Ciclista viviendo en Paris", locationInText = Some("Paris"), isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = None, lang = Some("es"), tweetPart = Some(GeoTrainingPart.userDescription), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Text", text = "Ciclista viviendo en Chantiago", locationInText = Some("Paris"), isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = None, lang = Some("es"), tweetPart = Some(GeoTrainingPart.userDescription), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Asnières sur Seine", locationInText = None, isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = None, lang = Some("es"), tweetPart = Some(GeoTrainingPart.userLocation), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "La via lactea", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.tweet, tweetId = None, lang = Some("es"), tweetPart = Some(GeoTrainingPart.userLocation), foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Perro", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Amigo", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Juan", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Voy", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Siempre", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "y", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "a", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "jugar", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Yo", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Mendoza", locationInText = None, isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Mozambique", locationInText = None, isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "China", locationInText = None, isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Estados unidos de América", locationInText = None, isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "USA", locationInText = None, isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Location", text = "Ciudad de México", locationInText = None, isLocation = Some(true), forcedLocationCode = None, forcedLocationName = None, source = GeoTrainingSource.epitweetrModel, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Person", text = "Barack Obama", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = Some("United States"), source = GeoTrainingSource.manual, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Person", text = "Emmanuel Macron", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = Some("Francia"), source = GeoTrainingSource.manual, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Demonym", text = "Chileno", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = Some("Chile"), source = GeoTrainingSource.manual, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Demonym", text = "Santiaguino", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = Some("Santiago de Chile"), source = GeoTrainingSource.manual, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
      GeoTraining(category = "Demonym", text = "Francés", locationInText = None, isLocation = Some(false), forcedLocationCode = None, forcedLocationName = Some("Francia"), source = GeoTrainingSource.manual, tweetId = None, lang = Some("es"), tweetPart = None, foundLocation = None, foundLocationCode = None, foundCountryCode = None),
    )
  }
  
  


  "Tagged Chunk" should "keep existing entity" in {
    assert(TaggedChunk("I am an entity", true, s).split(TaggedChunk("entity", true, s)) == Seq(TaggedChunk("I am an entity", true, s)))
  }
  it should "ignore no whole words matches" in {
    assert(TaggedChunk("Esto es chilenidad", false, s).split(TaggedChunk("chile", true, s)) == Seq(TaggedChunk("Esto es chilenidad", false, s)))
  }
  it should "ignore no entities" in {
    assert(TaggedChunk("I am not an entity", false, s).split(TaggedChunk("entity", false, s)) == Seq(TaggedChunk("I am not an entity", false, s)))
  }
  it should "find one full text" in {
    assert(TaggedChunk("This is a country", false, s).split(TaggedChunk("This is a country", true, s)) == Seq(TaggedChunk("This is a country", true, s)))
  }
  it should "find one inner text" in {
    assert(TaggedChunk("This is a beautiful country of people", false, s).split(TaggedChunk("beautiful country", true, s)) == 
      Seq(
        TaggedChunk("This is a", false, s),
        TaggedChunk("beautiful country", true, s),
        TaggedChunk("of people", false, s)
      ))
  }
  it should "find no occurrences" in {
    assert(TaggedChunk("This is not a country", false, s).split(TaggedChunk("This is a country", true, s)) == Seq(TaggedChunk("This is not a country", false, s)))
  }
  it should "find upper case occurrences" in {
    assert(TaggedChunk("This is a country", false, s).split(TaggedChunk("A COUNTRY", true, s)) == 
      Seq(
        TaggedChunk("This is", false, s),
        TaggedChunk("a country", true, s)
      ))
  }
  it should "find lower case occurrences" in {
    assert(TaggedChunk("This is A COUNTRY", false, s).split(TaggedChunk("a country", true, s)) == 
      Seq(
        TaggedChunk("This is", false, s),
        TaggedChunk("A COUNTRY", true, s)
      ))
  }
  it should "find multiple inner occurrences" in {
    assert(TaggedChunk("Te llamas Juan Fernandez o Juan Fernandez Mellado", false, s).split(TaggedChunk("Juan Fernandez", true, s)) == 
      Seq(
        TaggedChunk("Te llamas", false, s),
        TaggedChunk("Juan Fernandez", true, s),
        TaggedChunk("o", false, s),
        TaggedChunk("Juan Fernandez", true, s),
        TaggedChunk("Mellado", false, s)
      ))
  }
  it should "find multiple inner adyacent occurrences" in {
    assert(TaggedChunk("Te llamas Juan Fernandez Juan Fernandez Mellado", false, s).split(TaggedChunk("Juan Fernandez", true, s)) == 
      Seq(
        TaggedChunk("Te llamas", false, s),
        TaggedChunk("Juan Fernandez", true, s),
        TaggedChunk("Juan Fernandez", true, s),
        TaggedChunk("Mellado", false, s)
      ))
  }
  it should "multiple border occurrences" in {
    assert(TaggedChunk("Juan Fernandez te llamas o Mellado Juan Fernandez", false, s).split(TaggedChunk("Juan Fernandez", true, s)) == 
      Seq(
        TaggedChunk("Juan Fernandez", true, s),
        TaggedChunk("te llamas o Mellado", false, s),
        TaggedChunk("Juan Fernandez", true, s)
      ))
  }

  "TaggedText" should "merge entities with other TaggedText" in {
    assert(
      TaggedText(id = "1", lang = Some("es"), taggedChunks = Seq(
        TaggedChunk("Personalmente prefiero", false, s),
        TaggedChunk("Valdivia", true, s),
        TaggedChunk("que Santiago de Chile", false, s)
      )).mergeWith(
        TaggedText(id = "1", lang = Some("es"), taggedChunks = Seq(
          TaggedChunk("Personalmente prefiero Valdivia que", false, s),
          TaggedChunk("Santiago de Chile", true, s)
      ))) ==
        TaggedText(id = "1", lang = Some("es"), taggedChunks = Seq(
          TaggedChunk("Personalmente prefiero", false, s),
          TaggedChunk("Valdivia", true, s),
          TaggedChunk("que", false, s),
          TaggedChunk("Santiago de Chile", true, s)
        ))
    )
  }
  it should "produce an iterator of BIOAnnotations" in {
    assert(
      TaggedText(id = "1", lang = Some("es"),  taggedChunks = Seq(
        TaggedChunk("Personalmente prefiero", false, s),
        TaggedChunk("Valdivia", true, s),
        TaggedChunk("que", false, s),
        TaggedChunk("Santiago de Chile", true, s)
      )).toBIO(2, 2).isInstanceOf[Iterator[BIOAnnotation]]
    )
  }
  it should "ignore a text with non word characters when converting to BIO" in {
    assert(
      TaggedText(id = "1", lang = Some("es"),  taggedChunks = Seq(
        TaggedChunk("?", false, s),
      )).toBIO(2, 2).size == 0
    )
  }
  it should "produce right BIOAnnotations" in {
    assert(
      TaggedText(id = "1", lang = Some("es"), taggedChunks = Seq(
          TaggedChunk("Personalmente prefiero", false, s),
          TaggedChunk("Valdivia", true, s),
          TaggedChunk("que", false, s),
          TaggedChunk("Santiago de Chile", true, s)
        ), 
      ).toBIO(2, 2).toSeq ==
        Seq(
          BIOAnnotation("O", "Personalmente", Seq(), Seq("prefiero", "Valdivia"), Some("es")),
          BIOAnnotation("O", "prefiero", Seq("Personalmente"), Seq("Valdivia", "que"), Some("es")),
          BIOAnnotation("B", "Valdivia", Seq("prefiero", "Personalmente"), Seq("que", "Santiago"), Some("es")),
          BIOAnnotation("O", "que", Seq("Valdivia", "prefiero"), Seq("Santiago", "de"), Some("es")),
          BIOAnnotation("B", "Santiago", Seq("que", "Valdivia"), Seq("de", "Chile"), Some("es")),
          BIOAnnotation("I", "de", Seq("Santiago", "que"), Seq("Chile"), Some("es")),
          BIOAnnotation("I", "Chile", Seq("de", "Santiago"), Seq(), Some("es"))
        )
    )
  }
  it should "build right Tagged text from BIO annotations" in {
    assert(
      TaggedText(id = "1", lang = Some("es"), taggedChunks = Seq(
          TaggedChunk("Personalmente prefiero", false, s),
          TaggedChunk("Valdivia", true, s),
          TaggedChunk("que", false, s),
          TaggedChunk("Santiago de Chile", true, s)
        ), 
      ) ==
        TaggedText(
          id = "1",
          lang = Some("es"),
          tokens = Seq("Personalmente","prefiero", "Valdivia",  "que","Santiago", "de", "Chile"),
          bioTags = Seq("O", "O", "B", "O", "B", "I", "I")
        )
    )
  }
  lazy val bio = {
    GeoTraining.toTaggedText(trainingSet, splitter = s, nBefore = 3, nAfter = 3).flatMap(tt => tt.toBIO(3, 3))
  }

  "Geotraining test dataset" should " produce a BIO annotation for each token" in {
    val sp = s
    val tokens  = trainingSet.groupBy(gt => gt.id()).mapValues(geos => geos.reduce((a, b) => b)).values.flatMap(gt => gt.text.split(sp).filter(_.size > 0))
    assert(tokens.size == bio.size)
  }

  "Geotraining tools" should "build training/test dataset from GeoTraining datasets" in {
     val seqs = GeoTraining.getTrainingTestSets(annotations = trainingSet, trainingRatio=0.8, splitter = s, nBefore = 10, nAfter = 2)
     //seqs.foreach(s => println(s"${s._1}, ${s._2.count} ${s._3.count}"))
     seqs.size > 0
   }
   it should "evaluate models from GeoTraining datasets" in {
     assume(!sys.env.get("EPI_HOME").isEmpty)
     implicit val conf = Settings(sys.env("EPI_HOME"))
     conf.load
     implicit val storage = Storage.getSparkStorage  
     implicit val spark = getSpark
     import spark.implicits._
     val evaluation = GeoTraining.splitTrainEvaluate(annotations = trainingSet, trainingRatio = 0.5).collect
     val results = evaluation.map(_._5).reduce(_.sum(_))
     l.msg(results)
     assert(evaluation.size > 0)
   }
   it should "evaluate models for geolocating" in {
     assume(!sys.env.get("EPI_HOME").isEmpty)
     implicit val conf = Settings(sys.env("EPI_HOME"))
     conf.load
     implicit val spark = getSpark
     import spark.implicits._
     implicit val storage = Storage.getSparkStorage  
     val models = GeoTraining.trainModels(bio)
     assert(models.size > 0)

   }


}
