package org.ecdc.epitweetr.fs

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.search.{IndexSearcher, Query, TermQuery}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.ScoreDoc
import java.nio.file.{Files, Paths, Path}
import org.apache.lucene.store.{ FSDirectory, MMapDirectory}
import org.apache.lucene.index.{IndexWriter, IndexReader, DirectoryReader,IndexWriterConfig}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField}
import org.apache.lucene.index.Term
import org.apache.lucene.search.TopDocs
object TweetIndex {
  def apply(indexPath:String):TweetIndex = {
    val analyzer = new StandardAnalyzer()
    val indexDir = Files.createDirectories(Paths.get(s"${indexPath}"))
    val index = new MMapDirectory(indexDir, org.apache.lucene.store.SimpleFSLockFactory.INSTANCE)
    val config = new IndexWriterConfig(analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    println(s"Opening the index")
    val writer = new IndexWriter(index, config)
    //make the index near real time
    val reader = DirectoryReader.open(writer,true, true)
    println(s"Index opened = ${writer.isOpen}")

    TweetIndex(reader = reader, writer = writer, index = index)
  }
}


case class TweetIndex(var reader:IndexReader, writer:IndexWriter, index:FSDirectory){
  def tweetDoc(tweet:TweetV1, topic:String) = { 
      val doc = new Document()
      doc.add(new StringField("topic", s"${topic}", Field.Store.YES))  
      doc.add(new StringField("topic_tweet_id", s"${topic.toLowerCase}_${tweet.tweet_id}", Field.Store.YES))  
      doc.add(new LongPoint("tweet_id", tweet.user_id))
      doc.add(new StoredField("tweet_id", tweet.user_id))
      doc.add(new TextField("text", tweet.text, Field.Store.YES))  
      tweet.linked_text.map(text => doc.add(new TextField("linked_text", text, Field.Store.YES)))  
      doc.add(new StoredField("user_description", tweet.user_description))  
      tweet.linked_user_description.map(desc => doc.add(new StoredField("linked_user_description", desc)))  
      doc.add(new StringField("is_retweet", if(tweet.is_retweet) "true" else "false", Field.Store.NO)) 
      doc.add(new StoredField("is_retweet", Array(if(tweet.is_retweet) 1.toByte else 0.toByte )))  
      doc.add(new StringField("screen_name", tweet.screen_name, Field.Store.YES))
      doc.add(new StringField("user_name", tweet.user_name, Field.Store.YES))
      doc.add(new LongPoint("user_id", tweet.user_id))
      doc.add(new StoredField("user_id", tweet.user_id))
      doc.add(new StoredField("user_location", tweet.user_location))
      tweet.linked_user_name.map(value => doc.add(new StoredField("linked_user_name", value))) 
      tweet.linked_screen_name.map(value => doc.add(new StoredField("linked_screen_name", value))) 
      tweet.linked_user_location.map(value => doc.add(new StoredField("linked_user_location", value))) 
      doc.add(new LongPoint("created_timestamp", tweet.created_at.toEpochMilli()))
      doc.add(new StringField("created_at", tweet.created_at.toString(), Field.Store.YES)) 
      doc.add(new StringField("created_date", tweet.created_at.toString().take(10), Field.Store.YES)) 
      doc.add(new StringField("lang", tweet.lang, Field.Store.YES)) 
      tweet.linked_lang.map(value => doc.add(new StoredField("linked_lang", value))) 
      tweet.tweet_longitude.map(value => doc.add(new StoredField("tweet_longitude", value))) 
      tweet.tweet_latitude.map(value => doc.add(new StoredField("tweet_latitude", value))) 
      tweet.linked_longitude.map(value => doc.add(new StoredField("linked_longitude", value))) 
      tweet.linked_latitude.map(value => doc.add(new StoredField("linded_latitude", value))) 
      tweet.place_type.map(value => doc.add(new StoredField("place_type", value))) 
      tweet.place_name.map(value => doc.add(new StoredField("place_name", value))) 
      tweet.place_full_name.map(value => doc.add(new StoredField("place_full_name", value))) 
      tweet.linked_place_full_name.map(value => doc.add(new StoredField("linked_place_full_name", value))) 
      tweet.place_country_code.map(value => doc.add(new StoredField("place_country_code", value))) 
      tweet.place_country.map(value => doc.add(new StoredField("place_country", value))) 
      tweet.place_longitude.map(value => doc.add(new StoredField("place_longitude", value))) 
      tweet.place_latitude.map(value => doc.add(new StoredField("place_latitude", value)))
      tweet.linked_place_longitude.map(value => doc.add(new StoredField("linked_place_longitude", value))) 
      tweet.linked_place_latitude.map(value => doc.add(new StoredField("linked_place_latitude", value)))
      doc
  }
  def indexTweet(tweet:TweetV1, topic:String) {
      val doc = tweetDoc(tweet, topic)
      // we call uptadedocuent do tweet is only updated if existing already for the particular topic
      this.writer.updateDocument(new Term("topic_tweet_id", s"${topic.toLowerCase}_${tweet.tweet_id}"), doc)
  }


  def searchTweetV1(id:Long, topic:String) = {
    val searcher = new IndexSearcher(this.reader)
    val res = querySearch(new TermQuery(new Term("topic_tweet_id", s"${topic.toLowerCase}_${id}")), maxHits = 1)
    val doc = if(res.scoreDocs.size == 0) {
        None
    } else {
       Some(this.reader.document(res.scoreDocs(0).doc))
    }
    doc.map(d => (d, d.getField("topic").stringValue))
      .map{case (d, t) => (EpiSerialisation.luceneDocFormat.write(d), t)}
      .map{case (json, t) => (EpiSerialisation.tweetV1Format.read(json), t)
    }
  }
  def indexGeolocated(geo:Geolocated) {
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
          this.writer.updateDocument(new Term("topic_tweet_id", s"${geo.topic.toLowerCase}_${geo.id}"), doc)
      case _ =>
        println(s"Cannot find tweet ${geo.topic}_${geo.id} for update it")
    }     
  }

  def refreshReader() {
    //TODO: confirm that this refreshing is working
    println("refreshing")
    val oldReader = this.reader
    this.reader = DirectoryReader.open(this.writer,true, true)
    oldReader.close()
  }

  def search(query:String, maxHits:Int = 100, after:Option[ScoreDoc] = None) = {
    val analyzer = new StandardAnalyzer()
    val parser = new QueryParser("text", analyzer)
    val q = parser.parse(query)
    querySearch(q, maxHits, after) 
  }
  def querySearch(query:Query, maxHits:Int = 100, after:Option[ScoreDoc] = None) = {
    val searcher = new IndexSearcher(this.reader)
    val docs = after match {
      case Some(last) => searcher.searchAfter(last, query, maxHits)
      case None => searcher.search(query, maxHits)
    }
    docs
  } 
  def searchAll(query:String)  = {
    var after:Option[ScoreDoc] = None
    Iterator.continually{
      val res = search(query = query, after = after)
      if(res.scoreDocs.size > 0)
        after = Some(res.scoreDocs.last)
      res.scoreDocs
    }.takeWhile(_.size > 0)
      .flatMap(docs => docs)
      .map(doc => this.reader.document(doc.doc))
  }

  def getLocationFields(field:String, loc:Location) = {
    Seq(
      new StringField(s"$field.geo_code", loc.geo_code, Field.Store.YES), 
      new StringField(s"$field.geo_country_code", loc.geo_code, Field.Store.YES),
      new LongPoint(s"$field.geo_id", loc.geo_id),
      new StoredField(s"$field.geo_id", loc.geo_id),
      new StoredField(s"$field.geo_latitude", loc.geo_latitude), 
      new StoredField(s"$field.geo_longitude", loc.geo_longitude), 
      new StringField(s"$field.geo_name", loc.geo_name, Field.Store.YES),
      new StringField(s"$field.geo_type", loc.geo_type, Field.Store.YES)
    )
  }

}
