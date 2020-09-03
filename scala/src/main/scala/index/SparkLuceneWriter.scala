package demy.mllib.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.nio.file.{Files, Paths, Path}
import org.apache.lucene.store.{NIOFSDirectory, FSDirectory, MMapDirectory}
import org.apache.lucene.index.{IndexWriter,IndexWriterConfig}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer
//import org.apache.lucene.analysis.standard.StandardFilterFactory
import org.apache.lucene.analysis.standard.StandardTokenizerFactory 
import org.apache.lucene.analysis.core.StopFilterFactory 
import org.apache.lucene.analysis.TokenStream
import org.apache.commons.lang.RandomStringUtils
import demy.storage.Storage

case class SparkLuceneWriter(indexDestination:String, reuseSnapShot:Boolean=false, boostAcronyms:Boolean = false) {
  lazy val sparkStorage = Storage.getSparkStorage
  lazy val indexNode = {
    if(this.sparkStorage.isLocal)
      sparkStorage.getNode(indexDestination+"/lucIndex_" + RandomStringUtils.randomAlphanumeric(10))

    else 
      sparkStorage.localStorage.getTmpNode(prefix = Some("lucIndex"), markForDeletion = !reuseSnapShot ,sandBoxed = !reuseSnapShot)
  }
  lazy val destination = {
    if(this.sparkStorage.isLocal)
      this.indexNode
    else 
      sparkStorage.getNode(indexDestination).setAttr("replication", "1")
  }
  def create = {
    val analyzer = 
      if(boostAcronyms == false) { 
        new StandardAnalyzer()
      } else {
        CustomAnalyzer.builder()
                      .withTokenizer(classOf[StandardTokenizerFactory])
                      //.addTokenFilter(classOf[StandardFilterFactory])
                      .addTokenFilter(classOf[AcronymFilterFactory])
                      .addTokenFilter(classOf[StopFilterFactory])
                      .build();
    }
    val indexDir = Files.createDirectories(Paths.get(s"${indexNode.path}"))
    val index =
         if(true || System.getProperty("os.name").toLowerCase.contains("windows"))
           new MMapDirectory(indexDir, org.apache.lucene.store.NoLockFactory.INSTANCE)
         else  
           new NIOFSDirectory(indexDir, org.apache.lucene.store.NoLockFactory.INSTANCE)
    val config = new IndexWriterConfig(analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    val writer = new IndexWriter(index, config);
    SparkLuceneWriterInfo(writer = writer, index = index, destination = this.destination)
  }
}
