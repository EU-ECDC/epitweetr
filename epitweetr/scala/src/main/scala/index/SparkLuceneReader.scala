package demy.mllib.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.store.{NIOFSDirectory, FSDirectory, MMapDirectory}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.{IndexSearcher, LRUQueryCache, UsageTrackingQueryCachingPolicy}
import java.nio.file.{Paths}
import org.apache.spark.sql.SparkSession
import demy.storage.{Storage, LocalNode}
import demy.storage.WriteMode
import scala.reflect._
import scala.reflect.runtime.universe
import scala.collection.mutable.HashMap
import demy.util.{log => l}

case class SparkLuceneReader(indexPartition:String, reuseSnapShot:Boolean = false, useSparkFiles:Boolean = false,
                             usePopularity:Boolean=false, indexStrategy:String, strategyParams:Map[String,String]) {
  lazy val sparkStorage = Storage.getSparkStorage
  lazy val indexNode = {
    if(this.sparkStorage.isLocal)
      sparkStorage.getNode(indexPartition)
    else if(this.useSparkFiles)
      sparkStorage.localStorage.getNode(org.apache.spark.SparkFiles.get(this.indexPartition))
    else
      sparkStorage.localStorage.getNode(path = "/space/tmp/"+indexPartition.split("/").last)
  }

  def open() = {
    //Intra JVM lock
    SparkLuceneReader.readLock.synchronized {
      if(SparkLuceneReader.readerCount.get(this.indexNode.path).getOrElse(0)>0) {
         SparkLuceneReader.readerCount(this.indexNode.path) = SparkLuceneReader.readerCount(this.indexNode.path) + 1
      } else {
        if(!sparkStorage.isLocal && !this.useSparkFiles) {
          val indexName = Paths.get(this.indexPartition).getFileName().toString
          val source = sparkStorage.getNode(indexPartition)
          val dest = this.indexNode
          //checking if it is open

          //SparkLuceneReader.readLock.synchronized {
            //Inter JVM lock
            sparkStorage.localStorage.ensurePathExists("/space/tmp")
            val lockFile = new java.io.File("/space/tmp/"+indexName+".lock")
            lockFile.createNewFile()
            val wr = new java.io.RandomAccessFile(lockFile, "rw")
            try {
                val lock = wr.getChannel().lock();
                try {
                    //Critical section for downloading index file
                    sparkStorage.copy(from=source
                                       , to = dest
                                       ,  writeMode = if(reuseSnapShot) WriteMode.ignoreIfExists else WriteMode.overwrite
                                                  )
                } catch { case(e:Exception) => throw e
                } finally {
                    lock.release();
                }
            } catch { case(e:Exception) => throw e
            } finally {
                wr.close();
            }
          //}
        }
        val index = 
         if(true || System.getProperty("os.name").toLowerCase.contains("windows"))
           new MMapDirectory(Paths.get(this.indexNode.path), org.apache.lucene.store.NoLockFactory.INSTANCE)
         else  
           new NIOFSDirectory(Paths.get(this.indexNode.path), org.apache.lucene.store.NoLockFactory.INSTANCE)
        val reader = DirectoryReader.open(index)
        val searcher = new IndexSearcher(reader);
        //StandardStrategy(searcher = searcher, indexDirectory=indexNode.asInstanceOf[LocalNode], reader = reader, usePopularity = usePopularity);
        val useCache = true
        if(useCache) {
          val maxNumberOfCachedQueries = 10000;
          val maxRamBytesUsed = 500 * 1024L * 1024L; // 50MB
          // these cache and policy instances can be shared across several queries and readers
          // it is fine to eg. store them into static variables
          val queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
          val defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
          searcher.setQueryCache(queryCache);
          searcher.setQueryCachingPolicy(defaultCachingPolicy);
        }
        val mirror = universe.runtimeMirror(getClass.getClassLoader);
        val classInstance = Class.forName(indexStrategy);
        val classSymbol = mirror.classSymbol(classInstance);
        val classType = classSymbol.toType;
        val baseStrategy = classInstance.newInstance(/*Array(searcher, indexNode.asInstanceOf[LocalNode], reader)*/).asInstanceOf[IndexStrategy]
          .set(searcher = searcher, indexDirectory=indexNode.asInstanceOf[LocalNode], reader = reader)
        //l.msg(s"using strategy $baseStrategy")
        val ret = strategyParams.toSeq.foldLeft(baseStrategy)((current, prop)=> current.setProperty(prop._1, prop._2))
        SparkLuceneReader.readerCount(this.indexNode.path) = 1
        SparkLuceneReader.readers(this.indexNode.path) = ret
      }
    }
    SparkLuceneReader.readers(this.indexNode.path)
  }

  def mergeWith(that:SparkLuceneReader) = {
      val analyzer = new StandardAnalyzer();
      val config = new IndexWriterConfig(analyzer);
      config.setOpenMode(IndexWriterConfig.OpenMode.APPEND)

      val thisInfo = this.open
      val thatInfo = that.open
      val writer = new IndexWriter(thisInfo.reader.directory(),config)
      writer.addIndexes(thatInfo.reader.directory())
      val newDestPath = (this.indexPartition.split("/") match {case s => s.slice(0, s.size-1)}).mkString("/")+"/"+that.indexNode.path.split("/").head
      val winfo = SparkLuceneWriterInfo(writer = writer, index = thisInfo.reader.directory().asInstanceOf[FSDirectory], destination = this.indexNode.storage.getNode(path = newDestPath))
      winfo.push(deleteSource = false)
      thatInfo.close(true)
      that.sparkStorage.getNode(path = that.indexPartition).delete(recurse = true)
      this
  }
}
object SparkLuceneReader {
  val readLock = new Object()
  val readers = HashMap[String, IndexStrategy]()
  val readerCount = HashMap[String, Int]()
}
