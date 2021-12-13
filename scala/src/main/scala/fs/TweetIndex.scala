package org.ecdc.epitweetr.fs

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.search.{IndexSearcher, Query, TermQuery, TermRangeQuery}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{ScoreDoc, Sort, SortField}
import org.apache.lucene.util.BytesRef
import java.nio.file.{Files, Paths, Path}
import org.apache.lucene.store.{ FSDirectory, MMapDirectory}
import org.apache.lucene.index.{IndexWriter, IndexReader, DirectoryReader,IndexWriterConfig}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField, BinaryPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search.{TopDocs, BooleanQuery, PrefixQuery}
import org.apache.lucene.search.BooleanClause.Occur
import scala.util.{Try,Success,Failure}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.ecdc.epitweetr.geo.Geonames.Geolocate
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{StructField, StringType, IntegerType, FloatType, BooleanType, LongType, DoubleType}
import demy.util.{log => l}

object QuerySort {
  val index =  Sort.INDEXORDER
  val relevance = Sort.RELEVANCE
  val creationDateHour = new Sort(Seq(new SortField("created_at", SortField.Type.STRING, true), new SortField("created_hour", SortField.Type.INT, true)):_*)
  val indexReverse = new Sort(new SortField(null, SortField.Type.DOC, true))
}

object TweetIndex {
  def apply(indexPath:String, writeEnabled:Boolean=false):TweetIndex = {
    val analyzer = new StandardAnalyzer()
    val indexDir = Files.createDirectories(Paths.get(s"${indexPath}"))
    val index = new MMapDirectory(indexDir, org.apache.lucene.store.SimpleFSLockFactory.INSTANCE)
    val config = new IndexWriterConfig(analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    val writer = 
      if(writeEnabled)
        Some(new IndexWriter(index, config))
      else
        None
    //make the index near real time
    val reader =
      //Try{ 
        if(writeEnabled)
          DirectoryReader.open(writer.get,true, true)
        else {
          DirectoryReader.open(index)
        }
      /*} match {
        case Success(r) => r
        case Failure(e: org.apache.lucene.index.IndexNotFoundException) => 
          Files.list(Paths.get(indexPath)).iterator().asScala.toArray.foreach(p => Files.delete(p))
          if(writeEnabled)
            DirectoryReader.open(writer.get,true, true)
          else {
            DirectoryReader.open(index)
          }
        case Failure(f) => throw(f)
      }*/
    val searcher = new IndexSearcher(reader)
      TweetIndex(reader = reader, writer = writer, searcher = searcher, index = index, writeEnabled)
  }
}

case class TweetIndex(var reader:IndexReader, writer:Option[IndexWriter], var searcher:IndexSearcher, index:FSDirectory, writeEnabled:Boolean){
  def isOpen = {
    if(writeEnabled)
      this.writer.get.isOpen
    else
      true
  }
  def refreshReader()  = {
    this.synchronized {
      val oldReader = this.reader
      try{
        val newReader = 
          if(writeEnabled){
            DirectoryReader.open(this.writer.get,true, true)
          } else {
            DirectoryReader.open(this.index)
          }
        this.reader = newReader
        this.searcher = new IndexSearcher(this.reader)
      } 
      finally {
        try {
          oldReader.decRef() 
        }
        catch {
          case e:org.apache.lucene.store.AlreadyClosedException =>
            //println("reader already closed")
            //Thread.dumpStack()
        }
      }
    }
    this
  }

  def tweetDoc(tweet:TweetV1, topic:String) = { 
      val doc = new Document()
      doc.add(new StringField("topic", s"${topic}", Field.Store.YES))  
      doc.add(new StringField("topic_tweet_id", s"${topic.toLowerCase}_${tweet.tweet_id}", Field.Store.YES))  
      doc.add(new StringField("hash", s"${Math.abs(tweet.tweet_id.toString.hashCode).toInt.toString}", Field.Store.YES))  
      doc.add(new LongPoint("tweet_id", tweet.tweet_id))
      doc.add(new StoredField("tweet_id", tweet.tweet_id))
      doc.add(new TextField("text", tweet.text, Field.Store.YES))  
      tweet.linked_text.map(text => doc.add(new TextField("linked_text", text, Field.Store.YES)))  
      doc.add(new StringField("user_description", tweet.user_description, Field.Store.YES))  
      tweet.linked_user_description.map(desc => doc.add(new StringField("linked_user_description", desc, Field.Store.YES)))  
      doc.add(new StringField("is_retweet", if(tweet.is_retweet) "true" else "false", Field.Store.NO)) 
      doc.add(new StoredField("is_retweet", Array(if(tweet.is_retweet) 1.toByte else 0.toByte )))  
      doc.add(new StringField("screen_name", tweet.screen_name, Field.Store.YES))
      doc.add(new StringField("user_name", tweet.user_name, Field.Store.YES))
      doc.add(new LongPoint("user_id", tweet.user_id))
      doc.add(new StoredField("user_id", tweet.user_id))
      doc.add(new StoredField("user_location", tweet.user_location))
      tweet.linked_user_name.map(value => doc.add(new StringField("linked_user_name", value, Field.Store.YES))) 
      tweet.linked_screen_name.map(value => doc.add(new StringField("linked_screen_name", value, Field.Store.YES))) 
      tweet.linked_user_location.map(value => doc.add(new StoredField("linked_user_location", value))) 
      doc.add(new LongPoint("created_timestamp", tweet.created_at.toEpochMilli()))
      doc.add(new StringField("created_at", tweet.created_at.toString().toLowerCase, Field.Store.NO)) 
      doc.add(new StoredField("created_at", tweet.created_at.toString()))
      doc.add(new StringField("created_date", tweet.created_at.toString().take(10), Field.Store.YES)) 
      doc.add(new StringField("lang", tweet.lang, Field.Store.YES)) 
      tweet.linked_lang.map(value => doc.add(new StoredField("linked_lang", value))) 
      tweet.tweet_longitude.map(value => doc.add(new StoredField("tweet_longitude", value))) 
      tweet.tweet_latitude.map(value => doc.add(new StoredField("tweet_latitude", value))) 
      tweet.linked_longitude.map(value => doc.add(new StoredField("linked_longitude", value))) 
      tweet.linked_latitude.map(value => doc.add(new StoredField("linded_latitude", value))) 
      tweet.place_type.map(value => doc.add(new StoredField("place_type", value))) 
      tweet.place_name.map(value => doc.add(new StringField("place_name", value, Field.Store.YES))) 
      tweet.place_full_name.map(value => doc.add(new StringField("place_full_name", value, Field.Store.YES))) 
      tweet.linked_place_full_name.map(value => doc.add(new StoredField("linked_place_full_name", value))) 
      tweet.place_country_code.map(value => doc.add(new StoredField("place_country_code", value))) 
      tweet.place_country.map(value => doc.add(new StoredField("place_country", value))) 
      tweet.place_longitude.map(value => doc.add(new StoredField("place_longitude", value))) 
      tweet.place_latitude.map(value => doc.add(new StoredField("place_latitude", value)))
      tweet.linked_place_longitude.map(value => doc.add(new StoredField("linked_place_longitude", value))) 
      tweet.linked_place_latitude.map(value => doc.add(new StoredField("linked_place_latitude", value)))
      tweet.hashtags.map(arr => {if(arr.size > 0) doc.add(new StoredField("hashtags", arr.mkString("\n")))})
      tweet.urls.map(arr => {if(arr.size > 0) doc.add(new StoredField("urls", arr.mkString("\n")))})
      tweet.contexts.map(arr => {if(arr.size > 0) doc.add(new StoredField("contexts", arr.mkString("\n")))})
      tweet.entities.map(arr => {if(arr.size > 0) doc.add(new StoredField("entities", arr.mkString("\n")))})
      doc
  }
  def sparkRowDoc(row:Row, pk:Option[Seq[String]]=None, textFields:Set[String]=Set[String](), oldDoc:Option[Document], aggr:Map[String, String]) = {
    val doc = new Document()
    val pkVal = HashMap[String, String]()
    row.schema.fields.zipWithIndex.foreach{case (StructField(name, dataType, nullable, metadata), i) =>
      pk.map{pkFields =>
        if(pkFields.contains(name)) {
          if(!row.isNullAt(i))
            pkVal += ((name, row.get(i).toString))
        }
      }
      if(!row.isNullAt(i)) {
        dataType match {
          case StringType if !textFields.contains(name) =>  doc.add(new StringField(name, row.getAs[String](i), Field.Store.YES))
          case StringType if textFields.contains(name) =>  doc.add(new TextField(name, row.getAs[String](i), Field.Store.YES))
          case IntegerType => 
            doc.add(new IntPoint(name, applyIntAggregation(row.getAs[Int](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyIntAggregation(row.getAs[Int](i), name, aggr, oldDoc)))
          case BooleanType => 
            doc.add(new StringField(name, if(row.getAs[Boolean](i)) "true" else "false", Field.Store.NO)) 
            doc.add(new StoredField(name, Array(if(row.getAs[Boolean](i)) 1.toByte else 0.toByte )))  
          case LongType =>   
            doc.add(new LongPoint(name, applyLongAggregation(row.getAs[Long](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyLongAggregation(row.getAs[Long](i), name, aggr, oldDoc)))
          case DoubleType => 
            doc.add(new DoublePoint(name, applyDoubleAggregation(row.getAs[Double](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyDoubleAggregation(row.getAs[Double](i), name, aggr, oldDoc)))
          case FloatType =>
            doc.add(new FloatPoint(name, applyFloatAggregation(row.getAs[Float](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyFloatAggregation(row.getAs[Float](i), name, aggr, oldDoc)))
          //case ArrayType(StringType) =>
          //  doc.add(new StringF
          case _ =>
            throw new NotImplementedError(f"Indexing automatic index of datatype $dataType is not supported")
           
        }
      }
    }
    pk.map{pkFields =>
      doc.add(new StringField(
        pkFields.mkString("_"), 
        pkFields.map(k => pkVal.get(k).getOrElse(null)).mkString("_"), 
        Field.Store.YES
      ))
    }
    pk.map{pkFields =>
      doc.add(new StringField(
        "hash", 
        Math.abs(pkFields.map(k => pkVal.get(k).getOrElse(null)).mkString("_").hashCode).toInt.toString, 
        Field.Store.YES
      ))
    }
    doc
  }
  def applyIntAggregation(value:Int, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(oldDoc.isEmpty  || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.intValue()
    else if(aggr.get(fieldName) == Some("avg"))  
      (value + oldDoc.get.getField(fieldName).numericValue.intValue())/2
    else value
  }
  def applyLongAggregation(value:Long, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(oldDoc.isEmpty  || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.longValue()
    else if(aggr.get(fieldName) == Some("avg")) 
      (value + oldDoc.get.getField(fieldName).numericValue.longValue())/2
    else value
  }
  def applyDoubleAggregation(value:Double, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(!oldDoc.isEmpty && oldDoc.get.getField(fieldName) == null)
      println(s"INFO: $fieldName is not in >>>>>>>>>>>>>>>>>>>>>>${oldDoc}")

    if(oldDoc.isEmpty || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.doubleValue()
    else if(aggr.get(fieldName) == Some("avg")) 
      (value + oldDoc.get.getField(fieldName).numericValue.doubleValue())/2
    else value
  }
  def applyFloatAggregation(value:Float, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(oldDoc.isEmpty  || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.floatValue()
    else if(aggr.get(fieldName) == Some("avg")) 
      (value + oldDoc.get.getField(fieldName).numericValue.floatValue())/2
    else value
  }
  def indexTweet(tweet:TweetV1, topic:String) {
    val doc = tweetDoc(tweet, topic)
    // we call uptadedocuent do tweet is only updated if existing already for the particular topic
    if(!writeEnabled)
      throw new Exception("Cannot index  on a read only index")
    this.writer.get.updateDocument(new Term("topic_tweet_id", s"${topic.toLowerCase}_${tweet.tweet_id}"), doc)
  }
  def indexSparkRow(row:Row, pk:Seq[String], textFields:Set[String]=Set[String](), aggr:Map[String, String] =Map[String, String]() ) = {
    val oldDoc = searchRow(row, pk)
     
    val doc = this.sparkRowDoc(row = row, pk = Some(pk), textFields = textFields, oldDoc = oldDoc, aggr = aggr)
    //if(pk.contains("token")) {
    //  print(s"old $oldDoc \nnew $doc \n\n")
    //}

    if(!writeEnabled)
       throw new Exception("Cannot index on a read only index")
    if(pk.isEmpty)
      throw new Exception(s"Indexing spark rows without primary key definition is not supported")
    
    val pkTerm = new Term(pk.mkString("_"), pk.map(k => if(doc.getField(k) == null) null else doc.getField(k).stringValue).mkString("_"))
    this.writer.get.updateDocument(pkTerm, doc)
  }

  def searchTweetV1(id:Long, topic:String) = {
    implicit val searcher = this.useSearcher()
    Try{
      val res = search(new TermQuery(new Term("topic_tweet_id", s"${topic.toLowerCase}_${id}")), maxHits = 1, sort = QuerySort.index)
      val doc = if(res.scoreDocs.size == 0) {
        None
      } else {
         Some(searcher.getIndexReader.document(res.scoreDocs(0).doc))
      }
      doc.map(d => (d, d.getField("topic").stringValue))
        .map{case (d, t) => (EpiSerialisation.luceneDocFormat.customWrite(d, asArray = Set("hashtags","urls", "contexts","entities")), t)}
        .map{case (json, t) => (EpiSerialisation.tweetV1Format.read(json), t)}
    } match {
      case Success(r) =>
        this.releaseSearcher(searcher)
        r
      case Failure(e) =>
        this.releaseSearcher(searcher)
        throw e
    }
  }

  def searchTweet(id:Long, topic:String) = {
    implicit val searcher = this.useSearcher()
    Try{
      val res = search(new TermQuery(new Term("topic_tweet_id", s"${topic.toLowerCase}_${id}")), maxHits = 1, sort = QuerySort.index)
      val doc = if(res.scoreDocs.size == 0) {
        //searchTweetV1(id, topic)
        None
      } else {
         Some(searcher.getIndexReader.document(res.scoreDocs(0).doc))
      }
      doc.map(d => EpiSerialisation.luceneDocFormat.customWrite(d, asArray = Set("hashtags","urls", "contexts","entities")).toString)
    } match {
      case Success(r) =>
        this.releaseSearcher(searcher)
        r
      case Failure(e) =>
        this.releaseSearcher(searcher)
        throw e
    }
  }
  def searchRow(row:Row, pk:Seq[String]) = {
    assert(pk.size > 0)
    implicit val searcher = this.useSearcher()
    Try{
      val qb = new BooleanQuery.Builder()
      row.schema.fields.zipWithIndex
        .filter{case (StructField(name, dataType, nullable, metadata), i) => pk.contains(name)}
        .foreach{case (StructField(name, dataType, nullable, metadata), i) =>
        if(!row.isNullAt(i)) {//throw new Exception("cannot search a row on a null PK value")
        dataType match {
          case StringType => qb.add(new TermQuery(new Term(name, row.getAs[String](i))), Occur.MUST)
          case IntegerType => qb.add(IntPoint.newExactQuery(name, row.getAs[Int](i)), Occur.MUST)
          case BooleanType => qb.add(BinaryPoint.newExactQuery(name, Array(row.getAs[Boolean](i) match {case true => 1.toByte case _ => 0.toByte})), Occur.MUST)
          case LongType => qb.add(LongPoint.newExactQuery(name, row.getAs[Long](i)), Occur.MUST)
          case FloatType => qb.add(FloatPoint.newExactQuery(name, row.getAs[Float](i)), Occur.MUST)
          case DoubleType => qb.add(DoublePoint.newExactQuery(name, row.getAs[Double](i)), Occur.MUST)
          case dt => throw new Exception(s"Spark type {$dt.typeName} cannot be used as a filter since it has not been indexed")
         }
        }
      }

      val res = search(qb.build, maxHits = 1, sort = QuerySort.index)
      if(res.scoreDocs.size == 0) {
          None
      } else {
         Some(searcher.getIndexReader.document(res.scoreDocs(0).doc))
      }
    } match {
      case Success(r) =>
        this.releaseSearcher(searcher)
        r
      case Failure(e) =>
        this.releaseSearcher(searcher)
        throw e
    }
  }

  def indexGeolocated(geo:GeolocatedTweet) = {
    searchTweetV1(geo.id, geo.topic).orElse({geo.topic = geo.topic.toLowerCase;searchTweetV1(geo.id, geo.topic)}) match {
      case Some((tweet, t))  => 
        val doc = tweetDoc(tweet, t)
        val fields = Seq(  
            new StringField("is_geo_located", if(geo.is_geo_located) "true" else "false", Field.Store.NO), 
            new StoredField("is_geo_located", Array(if(geo.is_geo_located) 1.toByte else 0.toByte )) 
          ) ++  
            geo.linked_place_full_name_loc.map(l => getLocationFields(field = "linked_place_full_name_loc", loc = l)).getOrElse(Seq[Field]()) ++ 
            geo.linked_text_loc.map(l => getLocationFields(field = "linked_text_loc", loc = l)).getOrElse(Seq[Field]()) ++
            geo.place_full_name_loc.map(l => getLocationFields(field = "place_full_name_loc", loc = l)).getOrElse(Seq[Field]()) ++
            geo.text_loc.map(l => getLocationFields(field = "text_loc", loc = l)).getOrElse(Seq[Field]()) ++
            geo.user_description_loc.map(l => getLocationFields(field = "user_description_loc", loc = l)).getOrElse(Seq[Field]()) ++
            geo.user_location_loc.map(l => getLocationFields(field = "user_location_loc", loc = l)).getOrElse(Seq[Field]())
       
          fields.foreach(f => doc.add(f))
          if(!writeEnabled)
            throw new Exception("Cannot index on a read only index")
          this.writer.get.updateDocument(new Term("topic_tweet_id", s"${geo.topic.toLowerCase}_${geo.id}"), doc)
          true
      case _ =>
        false
    }     
  }

  def parseQuery(query:String) = {
    val analyzer = new StandardAnalyzer()
    val parser = new QueryParser("text", analyzer)
    val ret = smarterQuery(parser.parse(query))
    ret
  }
  def smarterQuery(q:Query):Query = {
    q match  {
      case bq:BooleanQuery => 
        val bqb = new BooleanQuery.Builder()
        bq.clauses.asScala.foreach{c => bqb.add(smarterQuery(c.getQuery), c.getOccur)}
        bqb.build
      case tq:TermQuery =>
        val field = tq.getTerm.field
        val text = 
          if(field.endsWith("country_code")) tq.getTerm.text.toUpperCase
          else tq.getTerm.text.toLowerCase
        new TermQuery(new Term(field, text))
      case tr:TermRangeQuery =>
        val field = tr.getField
        val (from, to) = 
          if(field.endsWith("country_code")) 
            (Try(tr.getLowerTerm.utf8ToString).getOrElse(tr.getLowerTerm.toString).toUpperCase, 
            Try(tr.getUpperTerm.utf8ToString).getOrElse(tr.getUpperTerm.toString).toUpperCase)
          else 
            (Try(tr.getLowerTerm.utf8ToString).getOrElse(tr.getLowerTerm.toString), 
            Try(tr.getUpperTerm.utf8ToString).getOrElse(tr.getUpperTerm.toString))
        new TermRangeQuery(tr.getField, new BytesRef(from), new BytesRef(to), tr.includesLower, tr.includesUpper) 
      case _ => 
        throw new Exception(s"Query is from an unmanaged query type: ${q.getClass}")
    }
  }

  def parseAndSearch(query:String, maxHits:Int = 100, after:Option[ScoreDoc] = None, sort:Sort)(implicit searcher:IndexSearcher):TopDocs  = {
    search(query = parseQuery(query), maxHits = maxHits, after = after, sort = sort) 
  }
  def search(query:Query, maxHits:Int= 50000, after:Option[ScoreDoc]= None, sort:Sort)(implicit searcher:IndexSearcher):TopDocs  = {
    val docs = after match {
      case Some(last) => searcher.searchAfter(last, query, maxHits, sort, false)
      case None => searcher.search(query, maxHits, sort, false)
    }
    docs
  }
  def parseAndSearchTweets(query:String, max:Option[Int] = None, doCount:Boolean=false, sort:Sort=QuerySort.index):Iterator[(Document, Long)]  = {
    searchTweets(parseQuery(query), max = max, doCount = doCount, sort = sort)
  }
  def searchTweets(query:Query, max:Option[Int] = None, doCount:Boolean=false, sort:Sort=QuerySort.index, topFieldLimit:Option[(String, String)] = None):Iterator[(Document, Long)]  = {
    val tops = HashMap[String, Double]()
    var refinedQ = None.asInstanceOf[Option[Query]]

    Seq(query)
      .flatMap{qq => 
        var q = qq
        var after:Option[ScoreDoc] = None
        var left = max
        var first = true
        var last = false
        var searcher:Option[IndexSearcher]=None
        var topFilter = None.asInstanceOf[Option[Set[(String, String)]]]
        Iterator.continually{
          if(left.map(l => l == 0).getOrElse(false))
            Array[(Document, Long)]()
          else {
            if(first) {
              searcher = Some(this.useSearcher()) 
              first = false
            }
            val (res, ret) = Iterator.range(0, 3).map{iTry =>
              Try{
                if(iTry > 0) {
                  l.msg(s"Retrying failed search $query on ${this.index.getDirectory}")
                  searcher = Some(this.useSearcher()) 
                }

                val r = search(query = q, after = after, sort = sort)(searcher.get)
                val t = r.scoreDocs.map(doc => searcher.get.getIndexReader.document(doc.doc))

                // Refining the query after 100k elements are obtained if the scope is only top Elements
                topFieldLimit match { 
                  case Some((topField, freqField)) =>
                    if(refinedQ.isEmpty) { 
                      t.map(t => (t.getField(topField).stringValue, t.getField(freqField).numericValue)).map{
                        case (top, value) =>
                          if(tops.size < 100000) {
                            tops.get(top) match {
                              case None =>
                                tops(top) = value.doubleValue()
                              case Some(v) =>
                                tops(top) = value.doubleValue() + v
                            }
                          } else if(refinedQ.isEmpty) {
                            val qparent = new BooleanQuery.Builder()
                            val qtops = new BooleanQuery.Builder()
                            tops.toSeq.sortWith(_._2 > _._2).take(500).foreach{case (top, v) => qtops.add(new TermQuery(new Term(topField, top)), Occur.SHOULD)}
                            qparent.add(qq, Occur.MUST)
                            qparent.add(qtops.build, Occur.MUST)
                            refinedQ = Some(qparent.build)
                            q = refinedQ.get
                          }
                      }
                    }
                  case _ => {}
                }
                
                (r, t)
              } match {
                case Success(p) => Some(p)
                case Failure(f) => 
                  if(iTry >=2) {
                    l.msg(s"Too many retries of tweet search\n ${f}")
                    throw f
                  }
                  None
              }
            }.flatMap(e => e)
             .next
            val count = if(doCount) res.totalHits.value else 0L
            if(res.scoreDocs.size > 0) {
              after = Some(res.scoreDocs.last)
              left  = left.map(l => l -  res.scoreDocs.size)
            }
            if(res.scoreDocs.size == 0 || left.map(l => l <= 0).getOrElse(false) && !last) {
              this.releaseSearcher(searcher.get)
              last = true
            }
            left.map{l => 
             if(l < 0) 
               ret.take(ret.size+l).map(d => (d, count))
             else 
               ret.map(d => (d, count))
            }.getOrElse{
              ret.map(d => (d, count))
            }
          }
        }.takeWhile(_.size > 0)
        .flatMap{docs => docs}
      }.toIterator
  }

  def getLocationFields(field:String, loc:Location) = {
    Seq(
      if(loc.geo_code !=null) Some(new StringField(s"$field.geo_code", loc.geo_code, Field.Store.YES)) else None,
      if(loc.geo_country_code != null) Some(new StringField(s"$field.geo_country_code", loc.geo_country_code, Field.Store.YES)) else None,
      Some(new LongPoint(s"$field.geo_id", loc.geo_id)),
      Some(new StoredField(s"$field.geo_id", loc.geo_id)),
      Some(new StoredField(s"$field.geo_latitude", loc.geo_latitude)),
      Some(new StoredField(s"$field.geo_longitude", loc.geo_longitude)),
      if(loc.geo_name != null) Some(new StringField(s"$field.geo_name", loc.geo_name, Field.Store.YES)) else None,
      if(loc.geo_type != null) Some(new StringField(s"$field.geo_type", loc.geo_type, Field.Store.YES)) else None
    ).flatMap(e => e)
  }
  def useSearcher() = {
    this.synchronized {
      val searcher = this.searcher
      val reader = searcher.getIndexReader
      if(reader.tryIncRef())
        searcher
      else {
        this.refreshReader()
        val searcher = this.searcher
        val reader = searcher.getIndexReader
        reader.incRef()
        searcher
      }
    }
  }
  def partitionKey(partitions:Int) = {
    if(partitions == 1)
      Seq(Seq.range(0, 9).map(_.toString))
    else {
      val prefixLength =  Math.ceil(Math.log10(partitions)).toInt
      val max = Math.pow(10, prefixLength).toInt
      Range(0, max).map(i => (i % partitions, i)).groupBy(_._1).mapValues(s => s.map(v => s"%0${prefixLength}d".format(v._2))).values
    }
  }

  def partition(baseQuery:Query, partitions:Int) = {
    partitionKey(partitions)
      .map{hashSeq =>  
        val qparent = new BooleanQuery.Builder()
        val qhash = new BooleanQuery.Builder()
        hashSeq.foreach{key => qhash.add(new PrefixQuery(new Term("hash", key)), Occur.SHOULD)}
        qparent.add(baseQuery, Occur.MUST)
        qparent.add(qhash.build, Occur.MUST)
        qparent.build
      }
  }
  def releaseSearcher(searcher:IndexSearcher) {
    this.synchronized {
      //println(s"decreasing ${this.index.getDirectory} from release Sercher")
      //Thread.dumpStack()
        try {
          searcher.getIndexReader.decRef()
        }
        catch {
          case e:org.apache.lucene.store.AlreadyClosedException =>
            //println("reader already closed")
            //Thread.dumpStack()
        }
    }
  }
  def deleteDoc(doc:Document, pkName:String){
    val pk = doc.getField(pkName).stringValue 
    this.writer.get.deleteDocuments(new Term(pkName, pk))
  }
  def searchReplaceInDoc(doc:Document, pkName:String, updateMap:Map[String, (String, String)], textFields:Set[String] = Set[String]()){
    val pk = doc.getField(pkName).stringValue 
    updateMap.foreach{case (field, (s, r)) =>
      if(doc.get(field)!=null) {
        val old = doc.getField(field).stringValue 
        doc.removeField(field)
        doc.add(new StringField(field, old.replaceAll(s, r), Field.Store.YES ))
      }
    }
    this.writer.get.updateDocument(new Term(pkName, pk), rebuildDoc(doc, textFields))
  }
  def updateHash(doc:Document, pkName:String){
    val pk = doc.getField(pkName).stringValue 
    val hash = Math.abs(pk.hashCode).toInt.toString
    doc.removeField("hash")
    doc.add(new StringField("hash", hash, Field.Store.YES ))
    this.writer.get.updateDocument(new Term(pkName, pk), rebuildDoc(doc))
  }

  def rebuildDoc(doc:Document, textFields:Set[String] = Set[String]()) = {
    val ndoc = new Document()
    doc.getFields.asScala.toSeq.foreach{f =>
      if(textFields.contains(f.name)) {
        ndoc.add(new TextField(f.name, f.stringValue, Field.Store.YES))  
      } else if(f.binaryValue != null) {
        ndoc.add(new StringField(f.name, if(f.binaryValue == 1.toByte) "true" else "false", Field.Store.NO ))
        ndoc.add(new StoredField(f.name, f.binaryValue))
      } else if(f.numericValue != null) {
        f.numericValue match {
          case v:java.lang.Integer =>
            ndoc.add(new IntPoint(f.name, v))
            ndoc.add(new StoredField(f.name, v))
          case v:java.lang.Float => 
            ndoc.add(new FloatPoint(f.name, v))
            ndoc.add(new StoredField(f.name, v))
          case v:java.lang.Double =>
            ndoc.add(new DoublePoint(f.name, v))
            ndoc.add(new StoredField(f.name, v))
          case v:java.lang.Long => 
            ndoc.add(new LongPoint(f.name, v))
            ndoc.add(new StoredField(f.name, v))
          case _ => throw new NotImplementedError("I do not know how convert ${v.getClass.getName} into lucene document")
        }
      } else if(f.stringValue != null)
        ndoc.add(new StringField(f.name, f.stringValue, Field.Store.YES))
      else
        throw new NotImplementedError("I do not know how convert ${f} into lucene document")
    }
    ndoc
  }
}
