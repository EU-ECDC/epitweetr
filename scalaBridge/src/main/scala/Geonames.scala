package org.ecdc.twitter 

import demy.storage.{Storage, WriteMode, FSNode}
import demy.util.{log => l, util}
import demy.mllib.index.implicits._
import org.apache.spark.sql.{SparkSession, Column, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class Geonames(source:String, destination:String) {
  val allCitiesPath = s"$destination/all-cities.parquet"
  val allGeosPath = s"$destination/all-geos.parquet"
  val allGeosIndex = s"$destination/all-geos.parquet.index"

  def geoLocate(
    ds:Dataset[_]
    , geoTexts:Seq[Column]
    , maxLevDistance:Int = 1
    , termWeightsCols:Seq[Option[String]]=Seq[Option[String]]()
    , reuseExistingIndex:Boolean = true
    , minScore:Int = 170
    , nGram:Int = 3
    , stopWords:Set[String] = Set[String]()
    , tokenizeRegex:Option[String]=None
    )(implicit storage:Storage) = {

    implicit val spark = ds.sparkSession
    import spark.implicits._
    val termWeightsPadded = geoTexts.zipWithIndex.map{case (col, i) => if(termWeightsCols.size > i) termWeightsCols(i) else None}
    ds.luceneLookups(
      right = this.getDataset()
      , queries = geoTexts.zip(termWeightsPadded).map{
          case(geoText, None) => geoText
          case(geoText, Some(termWeight)) => when(col(termWeight).isNotNull, geoText).as(geoText.toString.split("`").last)
        }
      , popularity = Some(col("pop"))
      , text =  expr("concat(coalesce(city, ''), ' ', coalesce(adm4, ''), ' ', coalesce(adm3, ''), ' ', coalesce(adm2, ''), ' ', coalesce(adm1, ''), ' ', coalesce(adm4, ''), ' ', coalesce(country, ''))")
      , rightSelect = Array(
          $"id".as("geo_id"), $"name".as("geo_name"), $"code".as("geo_code"), $"geo_type", $"country_code".as("geo_country_code")
          , $"country".as("geo_country"), $"city".as("geo_city"), $"adm1".as("geo_adm1"), $"adm2".as("geo_adm2"), $"adm3".as("geo_adm3"), $"adm4".as("geo_adm4")
          , $"population".as("geo_population"), $"pop".as("geo_pop"),  $"latitude".as("geo_latitude"), $"longitude".as("geo_longitude"))
      , leftSelect= Array(col("*"))
      , maxLevDistance=maxLevDistance
      , indexPath= this.allGeosIndex
      , reuseExistingIndex = reuseExistingIndex
      , indexPartitions = 1
      , maxRowsInMemory= 1
      , indexScanParallelism = 1
      , tokenizeRegex = tokenizeRegex
      , minScore = minScore
      , boostAcronyms = true
      , termWeightsColumnNames = termWeightsPadded
//      , strategy="demy.mllib.index.StandardStrategy"
      , stopWords = stopWords
      , strategy="demy.mllib.index.NgramStrategy"
      , strategyParams=Map(("nNgrams", nGram.toString))
   )
    
  }
  def getDataset(reuseExisting:Boolean = true)(implicit spark:SparkSession, storage:Storage) = {
    import spark.implicits._
    //Reading Source
    /*http://www.geonames.org/export/codes.html*/
    val newNames = 
      Seq("id", "name", "asciiname","alternatenames", "latitude", "longitude", "feature_class","feature_code"
           ,"country_code","cc2","admin1_code","admin2_code","admin3_code","admin4_code","population","elevation","dem", "timezone", "modification_date")

 
    (if(reuseExisting) util.restoreCheckPoint(allGeosPath) else None).getOrElse{
      val allCities = util.checkpoint(
        df = spark.read.option("sep", "\t").csv(source) //'http://download.geonames.org/export/dump/'
           .toDF(newNames: _*)
           .withColumn("population",expr("cast(population as int)"))
           .withColumn("id", col("id").cast(IntegerType))
           .withColumn("longitude", col("longitude").cast(DoubleType))
           .withColumn("latitude", col("latitude").cast(DoubleType))
           .repartition($"admin1_code")
        , path = allCitiesPath
        , reuseExisting = false
      )

      //Countries
      val countries = 
        allCities
          .where($"feature_code".isin(Array("PCL", "PCLD", "PCLF", "PCLI"):_*))
          .select($"id", $"name", concat_ws(",", regexp_replace($"name", lit(","), lit(" ")), coalesce($"alternatenames", lit(""))).as("alias")
                      , $"country_code".as("code"), typedLit[String](null).as("parent_code"), typedLit[String](null).as("city"), typedLit[String](null).as("adm4")
                      , typedLit[String](null).as("adm3"), typedLit[String](null).as("adm2"), typedLit[String](null).as("adm1"), typedLit[String](null).as("country")
                      , typedLit[String](null).as("adm4_code"), typedLit[String](null).as("adm3_code"), typedLit[String](null).as("adm2_code"), typedLit[String](null).as("adm1_code")
                      , $"country_code", $"population", $"feature_code".as("geo_type"), $"timezone".as("time_zone"), $"longitude", $"latitude").as[GeoElement]
          .map(current => 
            GeoElement(current.id, current.name, current.alias, current.code, current.parent_code, current.city, current.adm4, current.adm3, current.adm2, current.adm1, current.alias
                       , current.adm4_code, current.adm3_code, current.adm2_code, current.adm1_code, current.country_code, current.population, current.geo_type, current.time_zone
                       , current.longitude, current.latitude)
          )
          .persist
      
      //Level 1

      val adm1_0 = allCities
          .where($"feature_code"==="ADM1")
          .select($"id",$"name", concat_ws(",", regexp_replace($"name", lit(","), lit(" ")), coalesce($"alternatenames", lit(""))).as("alias")
                      , concat($"admin1_code", lit(";"),$"country_code").as("code"), $"country_code".as("parent_code"), typedLit[String](null).as("city"), typedLit[String](null).as("adm4")
                      , typedLit[String](null).as("adm3"), typedLit[String](null).as("adm2"), typedLit[String](null).as("adm1"), typedLit[String](null).as("country")
                      , typedLit[String](null).as("adm4_code"), typedLit[String](null).as("adm3_code"), typedLit[String](null).as("adm2_code"), $"admin1_code".as("adm1_code")
                      , $"country_code", $"population", $"feature_code".as("geo_type"), $"timezone".as("time_zone"), $"longitude", $"latitude").as[GeoElement]

      val adm1 = adm1_0
          .joinWith(countries, adm1_0("parent_code")===countries("code"), "inner")
          .map(p => p match { case (current, parent)
                  => GeoElement(current.id, current.name, current.alias, current.code, current.parent_code, current.city, current.adm4, current.adm3, current.adm2, current.alias, parent.country
                                      , current.adm4_code, current.adm3_code, current.adm2_code, current.adm1_code, current.country_code, current.population, current.geo_type, current.time_zone
                                      , current.longitude, current.latitude)
          })
          .persist
      
      //Level 2
      val adm2_0 = allCities
          .where($"feature_code"==="ADM2")
          .select($"id",$"name", concat_ws(",", regexp_replace($"name", lit(","), lit(" ")), coalesce($"alternatenames", lit(""))).as("alias")
                      , concat($"admin2_code", lit(";"),$"admin1_code", lit(";"),$"country_code").as("code"), concat($"admin1_code", lit(";"),$"country_code").as("parent_code")
                      , typedLit[String](null).as("city"), typedLit[String](null).as("adm4")
                      , typedLit[String](null).as("adm3"), typedLit[String](null).as("adm2"), typedLit[String](null).as("adm1"), typedLit[String](null).as("country")
                      , typedLit[String](null).as("adm4_code"), typedLit[String](null).as("adm3_code"), $"admin2_code".as("adm2_code"), $"admin1_code".as("adm1_code")
                      , $"country_code", $"population", $"feature_code".as("geo_type"), $"timezone".as("time_zone"), $"longitude", $"latitude").as[GeoElement]

      val adm2 = adm2_0
          .joinWith(adm1, adm2_0("parent_code")===adm1("code") , "inner")
          .map(p => p match { case (current, parent)
                  => GeoElement(current.id, current.name, current.alias, current.code, current.parent_code, current.city, current.adm4, current.adm3, current.alias, parent.adm1, parent.country
                                      , current.adm4_code, current.adm3_code, current.adm2_code, current.adm1_code, current.country_code, current.population, current.geo_type, current.time_zone
                                      , current.longitude, current.latitude)
          })
          .persist
      
      //Level 3
      val adm3_0 = allCities
          .where($"feature_code"==="ADM3")
          .select($"id",$"name", concat_ws(",", regexp_replace($"name", lit(","), lit(" ")), coalesce($"alternatenames", lit(""))).as("alias")
                      , concat($"admin3_code", lit(";"), $"admin2_code", lit(";"),$"admin1_code", lit(";"),$"country_code").as("code")
                      , concat($"admin2_code", lit(";"), $"admin1_code", lit(";"),$"country_code").as("parent_code")
                      , typedLit[String](null).as("city"), typedLit[String](null).as("adm4")
                      , typedLit[String](null).as("adm3"), typedLit[String](null).as("adm2"), typedLit[String](null).as("adm1"), typedLit[String](null).as("country")
                      , typedLit[String](null).as("adm4_code"), $"admin3_code".as("adm3_code"), $"admin2_code".as("adm2_code"), $"admin1_code".as("adm1_code")
                      , $"country_code", $"population", $"feature_code".as("geo_type"), $"timezone".as("time_zone"), $"longitude", $"latitude").as[GeoElement]

      val adm3 = adm3_0
          .joinWith(adm2, adm3_0("parent_code")===adm2("code") , "inner")
          .map(p => p match { case (current, parent)
                  => GeoElement(current.id, current.name, current.alias, current.code, current.parent_code, current.city, current.adm4, current.alias, parent.adm2, parent.adm1, parent.country
                                      , current.adm4_code, current.adm3_code, current.adm2_code, current.adm1_code, current.country_code, current.population, current.geo_type, current.time_zone
                                      , current.longitude, current.latitude)
          })
          .persist
      
      //Level 4
      val adm4_0 = allCities
          .where($"feature_code"==="ADM4")
          .select($"id",$"name", concat_ws(",", regexp_replace($"name", lit(","), lit(" ")), coalesce($"alternatenames", lit(""))).as("alias")
                      , concat($"admin4_code", lit(";"), $"admin3_code", lit(";"),$"admin2_code", lit(";"),$"admin1_code", lit(";"),$"country_code").as("code")
                      , concat($"admin3_code", lit(";"),$"admin2_code", lit(";"),$"admin1_code", lit(";"),$"country_code").as("parent_code"), typedLit[String](null).as("city"), typedLit[String](null).as("adm4")
                      , typedLit[String](null).as("adm3"), typedLit[String](null).as("adm2"), typedLit[String](null).as("adm1"), typedLit[String](null).as("country")
                      , $"admin4_code".as("adm4_code"), $"admin3_code".as("adm3_code"), $"admin2_code".as("adm2_code"), $"admin1_code".as("adm1_code")
                      , $"country_code", $"population", $"feature_code".as("geo_type"), $"timezone".as("time_zone"), $"longitude", $"latitude").as[GeoElement]

      val adm4 = adm4_0
          .joinWith(adm3, adm4_0("parent_code")===adm3("code") , "inner")
          .map(p => p match { case (current, parent)
                  => GeoElement(current.id, current.name, current.alias, current.code, current.parent_code, current.city, current.alias,  parent.adm3,  parent.adm2, parent.adm1, parent.country
                                      , current.adm4_code, current.adm3_code, current.adm2_code, current.adm1_code, current.country_code, current.population, current.geo_type, current.time_zone
                                      , current.longitude, current.latitude)
          })
          .persist
      
      val all_regions = adm1.union(adm2).union(adm3).union(adm4)
      
      val cities_0 = allCities
          .where($"feature_class"==="P")
          .select($"id",$"name", concat_ws(",", regexp_replace($"name", lit(","), lit(" ")), coalesce($"alternatenames", lit(""))).as("alias")
                      , $"id".as("code")
                      , when($"admin4_code".isNotNull, concat($"admin4_code", lit(";"), $"admin3_code", lit(";"),$"admin2_code", lit(";"),$"admin1_code", lit(";"),$"country_code"))
                          .when($"admin3_code".isNotNull, concat($"admin3_code", lit(";"),$"admin2_code", lit(";"),$"admin1_code", lit(";"),$"country_code"))
                          .when($"admin2_code".isNotNull, concat($"admin2_code", lit(";"),$"admin1_code", lit(";"),$"country_code"))
                          .when($"admin1_code".isNotNull, concat($"admin1_code", lit(";"),$"country_code"))
                          .otherwise($"country_code").as("parent_code"), typedLit[String](null).as("city"), typedLit[String](null).as("adm4")
                      , typedLit[String](null).as("adm3"), typedLit[String](null).as("adm2"), typedLit[String](null).as("adm1"), typedLit[String](null).as("country")
                      , $"admin4_code".as("adm4_code"), $"admin3_code".as("adm3_code"), $"admin2_code".as("adm2_code"), $"admin1_code".as("adm1_code"), $"country_code", $"population"
                      , $"feature_code".as("geo_type"), $"timezone".as("time_zone"), $"longitude", $"latitude").as[GeoElement]

      val cities = cities_0
          .joinWith(all_regions,cities_0("parent_code")===all_regions("code"), "inner")
          .map(p => p match { 
            case (current, parent) => 
              GeoElement(current.id, current.name, current.alias, current.code, current.parent_code, current.alias, current.adm4,  parent.adm3,  parent.adm2, parent.adm1, parent.country
                 , current.adm4_code, current.adm3_code, current.adm2_code, current.adm1_code, current.country_code, current.population, current.geo_type, current.time_zone
                 , current.longitude, current.latitude)
           })
          .persist

      //Writing all to disk
      val allGeos = util.checkpoint(
        df = cities
          .union(adm1)
          .union(adm2)
          .union(adm3)
          .union(adm4)
          .union(countries)
          .repartition(32, expr("cast(id/100000 as int)"))
          .withColumn("city_code", when($"city".isNotNull, $"code").as("city_code"))
          .withColumn("pop", expr("(log(10000+population)/10 + case when substring(geo_type, 0,3)='PPL' then 0.2 when substring(geo_type, 0,3)='PCL'then 0.3 else 0.0 end)"))
        , path = allGeosPath
        , reuseExisting = false
      )

      cities.unpersist
      adm1.unpersist
      adm2.unpersist
      adm3.unpersist
      adm4.unpersist
      countries.unpersist
      allGeos

    }
  } 
}

object Geonames {
  implicit class GeoLookup(val ds: Dataset[_]) {
    def geoLookup(geoTexts:Seq[Column], geoNames:Geonames, maxLevDistance:Int = 1, termWeightsCols:Seq[Option[String]]=Seq[Option[String]](), reuseExistingIndex:Boolean = true,  minScore:Int = 170, nGram:Int = 3, stopWords:Set[String]=Set[String](), tokenizeRegex:Option[String]=None)
      (implicit storage:Storage) = {
      geoNames.geoLocate(ds, geoTexts, maxLevDistance, termWeightsCols, reuseExistingIndex,  minScore,  nGram, stopWords, tokenizeRegex) 
    }
  }
}
