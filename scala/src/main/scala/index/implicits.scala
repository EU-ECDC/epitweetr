package demy.mllib.index;

import scala.collection.parallel.ForkJoinTaskSupport
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import demy.storage.Storage
import demy.util.{log => l}
import scala.collection.mutable.ArrayBuffer

object implicits {
  implicit class DatasetUtil(val ds: Dataset[_]) {
    def luceneLookup(
      right:Dataset[_]
      , query:Column
      , text:Column
      , maxLevDistance:Int=0
      , indexPath:String
      , reuseExistingIndex:Boolean=false
      , leftSelect:Array[Column]=Array(col("*"))
      , rightSelect:Array[Column]=Array(col("*"))
      , popularity:Option[Column]=None
      , indexPartitions:Int = 1
      , maxRowsInMemory:Int=100
      , indexScanParallelism:Int = 2
      , tokenizeRegex:Option[String]=Some("[^\\p{L}]+")
      , caseInsensitive:Boolean=true
      , minScore:Double=0.0
      , boostAcronyms:Boolean=false
      , termWeightsColumnName:Option[String]=None
      , strategy:String = "demy.mllib.index.StandardStrategy"
      , strategyParams: Map[String, String]=Map.empty[String,String]
      , stopWords:Set[String]=Set[String]()
    ) = luceneLookups(
      right = right
      , queries = Seq(query)
      , text = text
      , maxLevDistance = maxLevDistance
      , indexPath = indexPath
      , reuseExistingIndex = reuseExistingIndex
      , leftSelect = leftSelect
      , rightSelect = rightSelect
      , popularity = popularity
      , indexPartitions = indexPartitions
      , maxRowsInMemory = maxRowsInMemory
      , indexScanParallelism = indexScanParallelism
      , tokenizeRegex = tokenizeRegex
      , caseInsensitive = caseInsensitive
      , minScore = minScore
      , boostAcronyms = boostAcronyms
      , termWeightsColumnNames = Seq(termWeightsColumnName)
      , strategy = strategy
      , strategyParams = strategyParams
      , stopWords = stopWords
    )
    def luceneLookups(
      right:Dataset[_]
      , queries:Seq[Column]
      , text:Column
      , maxLevDistance:Int=0
      , indexPath:String
      , reuseExistingIndex:Boolean=false
      , leftSelect:Array[Column]=Array(col("*"))
      , rightSelect:Array[Column]=Array(col("*"))
      , popularity:Option[Column]=None
      , indexPartitions:Int = 1
      , maxRowsInMemory:Int=100
      , indexScanParallelism:Int = 2
      , tokenizeRegex: Option[String]=Some("[^\\p{L}]+")
      , caseInsensitive:Boolean=true
      , minScore:Double=0.0
      , boostAcronyms:Boolean=false
      , termWeightsColumnNames:Seq[Option[String]] = Seq[Option[String]]()
      , strategy:String = "demy.mllib.index.StandardStrategy"
      , strategyParams: Map[String, String]=Map.empty[String,String]
      , stopWords:Set[String]=Set[String]()
    ) = {
      val rightApplied = right.select(
        Array(text.as("_text_")) 
        ++ (popularity match {case Some(c) => Array(c.as("_pop_")) case _ => Array[Column]()}) 
        ++ (if(queries.size == 1) rightSelect
            else queries.map(q => struct(rightSelect:_*).as(s"${q.toString().split("`").last}_res"))
        ) :_*)

      val sparkStorage = Storage.getSparkStorage
      val indexNode = sparkStorage.getNode(path = indexPath)
      val exists = indexNode.exists
      
      //Building index if does not exists ou if it is not to be reused
      if(!exists || !reuseExistingIndex) {
        right.index(
          indexPath = indexPath
          , fields = rightSelect
          , text = text
          , tokenizeRegex = tokenizeRegex
          , popularity = popularity
          , boostAcronyms = boostAcronyms
          , indexPartitions = indexPartitions
          , caseInsensitive = caseInsensitive
        ) 
      }
      //Reading the index
      ds.indexLookUp(
        queries = queries
        , maxLevDistance = maxLevDistance
        , indexPath = indexPath
        , indexSchema = right.select(rightSelect :_*).schema
        , leftSelect = leftSelect
        , popularity = popularity
        , maxRowsInMemory = maxRowsInMemory
        , indexScanParallelism = indexScanParallelism
        , minScore = minScore
        , boostAcronyms = boostAcronyms
        , termWeightsColumnNames = termWeightsColumnNames
        , tokenizeRegex = tokenizeRegex
        , caseInsensitive = caseInsensitive
        , strategy = strategy
        , strategyParams = strategyParams
        , stopWords = stopWords
      ) 
    }

    def index(
      indexPath:String
      , fields:Array[Column]=Array(col("*"))
      , text:Column
      , tokenizeRegex:Option[String]=Some("[^\\p{L}]+")
      , popularity:Option[Column]=None
      , boostAcronyms:Boolean=false
      , caseInsensitive:Boolean= true
      , indexPartitions:Int = 1
    ) {

      demy.util.log.msg(s"indexing $text")
      val sparkStorage = Storage.getSparkStorage
      val indexNode = sparkStorage.getNode(path = indexPath)
      val exists = indexNode.exists
      if(exists)
        indexNode.delete(recurse = true)

      val dsApplied = ds.select((
        Array(text.as("_text_")) 
        ++ (popularity match {case Some(c) => Array(c.as("_pop_")) case _ => Array[Column]()}) 
        ++ fields
        ) :_*)

      val rdd = dsApplied.rdd
      val partedRdd = 
        if(rdd.getNumPartitions<indexPartitions) rdd.repartition(indexPartitions)
        else rdd.coalesce(indexPartitions)
        
      val popPosition = popularity match {case Some(c) => Some(1) case _ => None }
      val popPositionSet = popularity match {case Some(c) => Set(1) case _ =>Set[Int]()}

      partedRdd.mapPartitions{iter => 
        //Index creation
        var index = SparkLuceneWriter(indexDestination=indexPath,  boostAcronyms=boostAcronyms)
        var createIndex = true
        var indexInfo:SparkLuceneWriterInfo = null
        var indexHDFSDest:String = null
        iter.foreach{row => 
          if(createIndex) {
              indexInfo = index.create
              createIndex = false
          }
          indexInfo.indexRow(
            row = row
            , textFieldPosition = 0
            , popularityPosition = popPosition
            , notStoredPositions = Set(0) ++ popPositionSet, tokenisation = !tokenizeRegex.isEmpty, caseInsensitive = caseInsensitive 
          )
        }
        if(indexInfo != null) {
          indexInfo.push(deleteSource = true)
          Array(indexHDFSDest).iterator
        } else {
          Array[String]().iterator
        }
      }
      .collect
    }

    def indexLookUp(
      queries:Seq[Column]
      , maxLevDistance:Int=0
      , indexPath:String
      , indexSchema:StructType
      , leftSelect:Array[Column]=Array(col("*"))
      , popularity:Option[Column]=None
      , maxRowsInMemory:Int=100
      , indexScanParallelism:Int = 2
      , minScore:Double=0.0
      , boostAcronyms:Boolean=false
      , termWeightsColumnNames:Seq[Option[String]]=Seq[Option[String]]()
      , caseInsensitive:Boolean = true
      , tokenizeRegex:Option[String]=Some("[^\\p{L}]+")
      , strategy:String = "demy.mllib.index.StandardStrategy"
      , strategyParams: Map[String, String]=Map.empty[String,String]
      , stopWords:Set[String]=Set[String]()
      
    ) = {
      val sparkStorage = Storage.getSparkStorage
      val indexNode = sparkStorage.getNode(path = indexPath)
      val indexFiles =  
        indexNode.list().toArray
        .map{node => 
            SparkLuceneReader(indexPartition=node.path
              , reuseSnapShot = true
              , useSparkFiles= false
              , usePopularity=popularity match {
                  case Some(c) => true 
                  case None => false
                }
              , indexStrategy = strategy
              , strategyParams = strategyParams)
        }
      //Preparing the results
      val leftApplied = ds.select((Array(array(queries :_*).as("_text_")) ++ leftSelect) :_*)


      val queriesCount = queries.size
      val isArrayJoin = 
        leftApplied.schema.fields(0).dataType
          match {
            case ArrayType(ArrayType(x:StringType, _), _) => true
            case ArrayType(x:StringType, _) => false
            case _ => throw new Exception(s"Queries must be all of same type (Strings or array of strings)")
          }

      val isArrayJoinWeights:Seq[Option[Boolean]] =
        queries.zipWithIndex
          .map{case (q, i) =>
            if(i >= termWeightsColumnNames.size || termWeightsColumnNames(i).isEmpty) None
            else {
              leftApplied.schema.fields(leftApplied.schema.fieldIndex(termWeightsColumnNames(i).get)).dataType
                match {
                  case ArrayType(x:DoubleType, _) => Some(false)
                  case ArrayType(ArrayType(x:DoubleType, _), _) => Some(true)
                  case _ => throw new Exception(s"TermWeights must be an array of Doubles")
                }
            }
          }


      val leftOutFields = leftApplied.schema.fields.slice(1, leftApplied.schema.fields.size)
      val leftOutSchema = new StructType(leftOutFields)
      val rightRequestFields = 
        indexSchema
          .fields
          .map(f => new StructField(
            name = f.name
            , dataType = f.dataType
            , nullable = true
            , metadata = f.metadata)
          )

      val rightOutFields = 
        rightRequestFields ++ Array(
           StructField("_score_", FloatType)
           , StructField("_tags_", ArrayType(StringType))
           , StructField("_startIndex_", IntegerType)
           , StructField("_endIndex_", IntegerType)
         )

      val rightOutSchema = 
        if(queries.size == 1) {
          if(!isArrayJoin) 
            StructType(rightOutFields)
          else 
            StructType(
              fields = Array(StructField(
                name = "array", 
                dataType = ArrayType(elementType=new StructType(rightOutFields) , containsNull = true)
              )))
        } else {
            StructType(fields = queries.map{q => 
              if(!isArrayJoin) 
                StructField(name = s"${q.toString().split("`").last}_res", dataType = StructType(rightOutFields))
              else 
                StructField(name = s"${q.toString().split("`").last}_res", dataType = ArrayType(elementType= StructType(rightOutFields) , containsNull = true))
              }
            )
        }

      val stopBC = ds.sparkSession.sparkContext.broadcast(stopWords)
      val searchCols = queries.mkString(",")
      Some(leftApplied.rdd
        .mapPartitionsWithIndex{case (iPart, iter) =>
          var indexLocation = ""
          var rInfo:IndexStrategy = null

          iter.flatMap(r => {
            val chunkIter = (Iterator(r) ++ (iter.take(maxRowsInMemory -1))).map(r => (r, Array.fill(queriesCount)(ArrayBuffer[Option[Row]]())))
            val rowsChunk = chunkIter.toArray 
            val rowsIter = Seq.range(0, rowsChunk.size) match {
              case s if indexScanParallelism > 1  => 
                val parIter = s.par
                parIter.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(indexScanParallelism))
                parIter
              case s =>
                s
            }
           

            indexFiles.foreach(iReader => {
              if(indexLocation != iReader.indexPartition) {
                if(rInfo!=null) rInfo.close(false)
                rInfo = iReader.open
                indexLocation = iReader.indexPartition
              }
              rowsIter.foreach { iRow =>
                 val leftRow = rowsChunk(iRow)._1
                 val rightResults = rowsChunk(iRow)._2
                 val qValues = if(isArrayJoin) leftRow.getSeq[Seq[String]](0) else leftRow.getSeq[String](0).map(q => Seq(q))
                 val termWeightsArray:Seq[Seq[Option[Seq[Double]]]] = 
                   isArrayJoinWeights.zipWithIndex.map{
                     case (None, i) => qValues(i).map(_ => None) 
                     case (Some(false), i) => Seq(Some(leftRow.getAs[Seq[Double]](leftRow.fieldIndex(termWeightsColumnNames(i).get))))
                     case (Some(true), i) => throw new Exception("@epi not yet supported") 
                 }
                 for(i <- Iterator.range(0, queriesCount)) {
                   if(rightResults(i).size == 0) rightResults(i) ++= qValues(i).map(_=> None)
                 }

                 for{i <- Iterator.range(0, qValues.size)
                      j <-Iterator.range(0, qValues(i).size)} { 
                     // call search on SearchStrategy
                     val startTime = System.nanoTime
                     
                     val tokens = (tokenizeRegex,  qValues(i)(j)) match {
                           case (_, null) => null
                           case (Some(r), s) => s.split(r).filter(_.size > 0)
                           case (None, s) => Array(s)
                         }
                     val (searchTokens, searchWeights) = 
                       if(stopBC.value.size > 0 && tokens != null) 
                         (tokens.filter(t => !stopBC.value(t))
                          ,termWeightsArray(i)(j).map(seq => 
                             tokens.toSeq.zipWithIndex.filter{
                               case (t, k) => !stopBC.value(t)}.map{case (t, k) =>  {if(seq.size < tokens.size) {println(s"$seq, ${tokens.mkString(", ")}");throw new Exception("OUCH")} else seq(k)}
                             })
                         )
                       else (tokens,  termWeightsArray(i)(j))
                     //l.msg(s"searching for ${searchTokens.mkString(",")}") 
                     val res:Array[GenericRowWithSchema] =  rInfo.search(
                       tokens = searchTokens, maxHits=1, filter = Row.empty, outFields=rightRequestFields,
                         maxLevDistance=maxLevDistance, minScore=minScore, boostAcronyms=boostAcronyms,
                         usePopularity = iReader.usePopularity, termWeights=searchWeights,
                         caseInsensitive = caseInsensitive)
                     val endTime = System.nanoTime
                     //if(termWeightsArray(i)(j) != null && termWeightsArray(i)(j).map(s => s.size).getOrElse(0)>0)
                     //  l.msg(s"long in ${(endTime - startTime)/1e6d} for $iRow in $iPart ${(endTime - startTime)/1e6d} mili seconds for ${if(tokens != null) tokens.size else "null"}")

                     if(res.size > 0 && (rightResults(i)(j).isEmpty || res(0).getAs[Float]("_score_") > rightResults(i)(j).get.getAs[Float]("_score_"))) { 
                        rightResults(i)(j) = Some(res(0)) 
                     }
                 }
               }
             })
             if(!iter.hasNext) rInfo.close(false)
             rowsChunk.iterator
          })
        }
        .map{case (left, right) => 
          val leftRow =  Row.fromSeq(left.toSeq.slice(1, leftOutFields.size+1)) //new GenericRowWithSchema(left.toSeq.slice(1, leftOutFields.size+1).toArray, leftOutSchema)
          val rightRow = 
            if(!isArrayJoin && queriesCount == 1) 
              right(0)(0) match { 
                case Some(row) => row
                case None => Row.fromSeq(rightOutFields.map(f => null)) //new GenericRowWithSchema(rightOutFields.map(f => null), rightOutSchema) //
              }
            else
              Row.fromSeq(//new GenericRowWithSchema(
                right.map(qresults => 
                  Some(qresults.map{
                    case None => Row.fromSeq(rightOutFields.map(f => null))
                    case Some(row) => row
                  }).map{
                    case s if(!isArrayJoin) => s(0) 
                    case s => s
                  }
                  .get
                )
                //,rightOutSchema
              )
          Row.merge(leftRow, rightRow)
        })
      .map(resultRdd => ds.sparkSession.createDataFrame(resultRdd, new StructType(leftOutFields ++ rightOutSchema.fields)))
      .get
    }
  }
}
