package org.ecdc.epitweetr.fs

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.ScoreDoc
import java.nio.file.{Files, Paths, Path}
import org.apache.lucene.store.{ FSDirectory, MMapDirectory}
import org.apache.lucene.index.{IndexWriter, IndexReader, DirectoryReader,IndexWriterConfig}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField}
import org.apache.lucene.index.Term

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


case class TweetIndex(reader:IndexReader, writer:IndexWriter, index:FSDirectory){
  def indexTweet(tweet:TweetV1, topic:String) {
      val doc = new Document()
      doc.add(new TextField("topic_tweet_id", s"${topic}_${tweet.tweet_id}", Field.Store.YES))  
      doc.add(new TextField("topic", topic, Field.Store.YES))  
      doc.add(new TextField("tweet_id", tweet.tweet_id.toString, Field.Store.YES))  
      doc.add(new TextField("text", tweet.text, Field.Store.YES))  
      tweet.linked_text.map(text => doc.add(new TextField("linked_text", text, Field.Store.YES)))  
      doc.add(new StoredField("user_description", tweet.user_description))  
      tweet.linked_user_description.map(desc => doc.add(new StoredField("linked_user_description", desc)))  
      doc.add(new IntPoint("is_retweet", if(tweet.is_retweet) 1 else 0 )) 
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
      this.writer.updateDocument(new Term("topic_tweet_id", s"${topic}_${tweet.tweet_id}"), doc)
  }
  def commit() {
    this.writer.commit()
  }

  def close() {
    this.writer.close()
  }

  def searchTopDocs(query:String, maxHits:Int = 100, after:Option[ScoreDoc] = None) = {
    val searcher = new IndexSearcher(this.reader)
    val analyzer = new StandardAnalyzer()
    val parser = new QueryParser("text", analyzer)
    val q = parser.parse(query) 
    val docs = after match {
      case Some(last) => searcher.searchAfter(last, q, maxHits)
      case None => searcher.search(q, maxHits)
    }
    docs
  } 
  def searchAll(query:String)  = {
    var after:Option[ScoreDoc] = None
    Iterator.continually{
      val res = searchTopDocs(query = query, after = after)
      if(res.scoreDocs.size > 0)
        after = Some(res.scoreDocs.last)
      res.scoreDocs
    }.takeWhile(_.size > 0)
      .flatMap(docs => docs)
      .map(doc => this.reader.document(doc.doc))
  }

}
