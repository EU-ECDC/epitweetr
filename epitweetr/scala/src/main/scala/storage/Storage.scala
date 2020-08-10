package demy.storage

import demy.util.log
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.hadoop.conf.Configuration
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.io.IOUtils

import java.io.{InputStream, ByteArrayInputStream, OutputStream, OutputStreamWriter}
import java.util.ArrayDeque
import java.nio.file.{Files, Paths, Path => LPath}
import java.nio.file.{StandardCopyOption,StandardOpenOption }
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
case class WriteMode(name:String) {
} 
object WriteMode {
  object overwrite extends WriteMode("overwrite")
  object ignoreIfExists extends WriteMode("ignoreIfExists")
  object failIfExists extends WriteMode("failIfExists")
}

trait FSNode {
  type Self <: FSNode
  val path:String
  val storage:Storage
  val isLocal:Boolean 
  val sparkCanRead:Boolean 
  val attrs:Map[String, String]

  def setAttr(name:String, value:String):Self
  def setPath(path:String):Self

  def exists = storage.exists(this)
  def isDirectory = storage.isDirectory(this)
  def delete(recurse:Boolean = false) = storage.delete(this, recurse) 
  def deleteIfTemporary(recurse:Boolean = false) = storage.deleteIfTemporary(this, recurse) 
  def getContent = storage.getContent(this)
  def getContentAsString = storage.getContentAsString(this)
  def getContentAsJson = storage.getContentAsJson(this)
  def list(recursive:Boolean = false) = storage.list(this, recursive)
  def move(to:FSNode, writeMode:WriteMode) = {this.storage.move(this, to, writeMode)}
  def setContent(content:InputStream, writeMode:WriteMode) = {this.storage.setContent(this, content, writeMode);this}
  def setContent(content:InputStream) = {this.storage.setContent(this, content, WriteMode.failIfExists);this}
  def setContent(content:String, writeMode:WriteMode) = {this.storage.setContent(this, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), writeMode);this}
  def setContent(content:String) = {this.storage.setContent(this, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), WriteMode.failIfExists);this}
  def getFileModificationTime(recurse:Boolean = true) = 
    storage.getFileModificationTime(
      path = if(this.path!=null) Some(this.path) else throw new Exception("getModificationTime requires a non empty hash")
      , attrPattern=Map[String, String]())
  def getWriter(writeMode:WriteMode) = storage.getWriter(this, writeMode)
  def getTextWriter(writerMode:WriteMode, charsetName:String) = this.storage.getTextWriter(this, writerMode, charsetName)
}

case class LocalNode(path:String, storage:LocalStorage, sparkCanRead:Boolean, attrs:Map[String, String]= Map[String, String]()) extends FSNode{
  type Self = LocalNode 
  val isLocal:Boolean = true
  val jPath = Paths.get(path) 
  def setAttr(name:String, value:String) = LocalNode(path=this.path, storage=this.storage, sparkCanRead = this.sparkCanRead, attrs = this.attrs + (name -> value))
  def setPath(path:String) = LocalNode(path=path, storage=this.storage, sparkCanRead = this.sparkCanRead, attrs = attrs)

} 
case class EpiFileNode(path:String, storage:EpiFileStorage, attrs:Map[String, String]= Map[String, String]()) extends FSNode{
  type Self = EpiFileNode 
  val isLocal = false
  val sparkCanRead = false

  def setAttr(name:String, value:String) = EpiFileNode(path=this.path, storage=this.storage, attrs = attrs + (name -> value))
  def setPath(path:String) =  EpiFileNode(path=path, storage=this.storage, attrs=this.attrs)
}

case class HDFSNode(path:String, storage:HDFSStorage, attrs:Map[String, String]= Map[String, String]()) extends FSNode {
  type Self = HDFSNode 
  val isLocal = false
  val sparkCanRead = true
  val hPath = new HPath(path)

  def setAttr(name:String, value:String) = HDFSNode(path=this.path, storage=this.storage, attrs = attrs + (name -> value))
  def setPath(path:String) =  HDFSNode(path=path, storage=this.storage, attrs=this.attrs)
}

trait Storage {
  val protocol:String  
  val systemTmpDir:String 
  val sparkCanRead:Boolean 
  val isLocal:Boolean 
  val tmpPrefix:String="demy_" 
  lazy val sandBoxDir =  (systemTmpDir + "/" + tmpPrefix+ RandomStringUtils.randomAlphanumeric(10))
  
  val localStorage:LocalStorage
  def exists(node:FSNode):Boolean
  def isDirectory(node:FSNode):Boolean 
  def delete(node:FSNode, recurse:Boolean = false) 
  def deleteIfTemporary(node:FSNode, recurse:Boolean)  { deleteIfTemporary(path = node.path, recurse = recurse)}
  def deleteIfTemporary(path:String, recurse:Boolean = false)  {
    this.tmpFiles.iterator.asScala.find(n => n.path == path) match {
      case Some(n) =>
        this.delete(node=n, recurse=recurse)
        this.tmpFiles.removeFirstOccurrence(n)
      case None =>
    }
  }
  def getContent(node:FSNode):InputStream
  def getContentAsString(node:FSNode) = {
    val s = new java.util.Scanner(this.getContent(node), "UTF-8").useDelimiter("\\A") 
    if(s.hasNext())  s.next()
    else "";
  }
  def getContentAsJson(node:FSNode) = scala.util.parsing.json.JSON.parseFull(getContentAsString(node))
  
  def setContent(node:FSNode, data:InputStream, writeMode:WriteMode = WriteMode.failIfExists)
  def list(node:FSNode, recursive:Boolean):Seq[FSNode]
  def last(path:Option[String], attrPattern:Map[String, String]):Option[FSNode]
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode
  val tmpFiles:ArrayDeque[FSNode]= new ArrayDeque[FSNode]()
  def removeMarkedFiles(cleanSandBox:Boolean=true) {
    while(tmpFiles.size > 0) {
      tmpFiles.pop().delete(recurse = true)
    }
    if(cleanSandBox && this.systemTmpDir!=null)
      log.msg(s"Removing sandboxed tmp dir ${this.sandBoxDir}")
      this.getNode(path = this.sandBoxDir).delete(recurse = true)
    if(!this.isLocal) {this.localStorage.removeMarkedFiles(cleanSandBox)}
  } 
  def getTmpPath(fixName:Option[String]=None, prefix:Option[String]=None, sandBoxed:Boolean=true):String
       = (if(sandBoxed) sandBoxDir else systemTmpDir) +"/"+ 
         ((fixName, prefix) match {
         case (Some(name), _) => name 
         case (_, Some(prefix)) => prefix + "_" +RandomStringUtils.randomAlphanumeric(10) 
         case _ =>  RandomStringUtils.randomAlphanumeric(10)
         })

  def getTmpNode(fixName:Option[String]=None, prefix:Option[String]=None, markForDeletion:Boolean=true, sandBoxed:Boolean=true) = {
    ensurePathExists(if(sandBoxed) this.sandBoxDir else this.systemTmpDir)
    val ret = getNode(path = getTmpPath(fixName=fixName, sandBoxed = sandBoxed))
    if(markForDeletion) this.markForDeletion(ret)
    ret
  }
  def markForDeletion(n:FSNode) {
    tmpFiles.push(n)
  }
  def ensurePathExists(path:String)
  def copy(from:FSNode, to:FSNode, writeMode:WriteMode) {
    if(from.storage != to.storage || from.path !=to.path) {
      to.setContent(content = from.getContent, writeMode = writeMode)
    }
  }
  def moveWithin(from:FSNode, to:FSNode, writeMode:WriteMode)
  def move(from:FSNode, to:FSNode, writeMode:WriteMode) {
    if(from.storage != to.storage) {
      copy(from, to, writeMode)
      delete(from, false)
    } else if(from.storage == to.storage && from.path !=to.path) {
      moveWithin(from, to, writeMode)
    }
  }
  def getFileModificationTime(path:Option[String], attrPattern:Map[String, String] = Map[String, String]()):Option[Long]
  def isUnchanged(path:Option[String], attrPattern:Map[String, String] = Map[String, String](), checkPath:Option[String], checkAttr:Map[String, String] = Map[String, String](), updateStamp:Boolean = true) = {
    //Getting stored Timestamp
    val timestampNode =  this.getNode(checkPath.getOrElse(null), checkAttr)
    val storedTimestamp = if(timestampNode.exists) Some(timestampNode.getContentAsString.toLong) else None

    //Getting current timestamp
    val currentTimestamp = this.getFileModificationTime(path, attrPattern) 

    //Performing update actions and returning values
    (storedTimestamp, currentTimestamp) match {
      case (_, None) => throw new Exception("Cannot find file to get modification time")
      case (None, Some(current)) => {
        if(updateStamp) this.getNode(checkPath.get, checkAttr).setContent(current.toString, WriteMode.overwrite)
        false
      }
      case (Some(stored), Some(current)) => {
        if(stored != current && updateStamp) this.getNode(checkPath.getOrElse(null), checkAttr).setContent(current.toString, WriteMode.overwrite)
        stored == current
      } 
    }
  }
  def getWriter(node:FSNode, writeMode:WriteMode):OutputStream
  def getTextWriter(node:FSNode, writerMode:WriteMode, charsetName:String) = new OutputStreamWriter(this.getWriter(node, writerMode), charsetName)

}

object Storage {
  lazy val getLocalStorage = LocalStorage()
  lazy val getSparkStorage = {
    System.getenv().asScala.get("HADOOP_CONF_DIR") match {
      case Some(confDir) =>
        val hadoopConf = new Configuration();
        hadoopConf.addResource(new HPath(s"file://${confDir}/core-site.xml"));
        hadoopConf.addResource(new HPath(s"file://${confDir}/hdfs-site.xml"));
        HDFSStorage(hadoopConf = hadoopConf)
      case _ =>
        LocalStorage(sparkCanRead = true)
    }
  }
}

case class LocalStorage(override val sparkCanRead:Boolean=false, override val tmpPrefix:String="demy_") extends Storage {
  override val isLocal:Boolean = true 
  override val protocol:String="file"  
  override val localStorage:LocalStorage = this
  val systemTmpDir = System.getenv("TMPDIR") match {case null => System.getProperty("java.io.tmpdir") case s => s}

  def exists(node:FSNode) = 
    node match { 
      case lNode:LocalNode => Files.exists(lNode.jPath)          
      case _ => throw new Exception(s"Local Storage cannot manage ${node.getClass.getName} nodes")
    }
  def isDirectory(node:FSNode) = 
    node match { 
      case lNode:LocalNode => Files.isDirectory(lNode.jPath)     
      case _ => throw new Exception(s"Local Storage cannot manage ${node.getClass.getName} nodes")
    }
  def delete(node:FSNode, recurse:Boolean = false) 
       = node match { case lNode:LocalNode => {
         if(Files.isDirectory(lNode.jPath) && recurse ) {
           val list = Files.list(lNode.jPath)
           list.iterator().asScala.foreach{ cPath =>
            lNode.storage.delete(node = LocalNode(path = cPath.toAbsolutePath().toString(), storage=lNode.storage, sparkCanRead = this.sparkCanRead)
                                  , recurse = recurse)
           }
           list.close()
         }
         if(lNode.exists) Files.delete(lNode.jPath)
       } case _ => throw new Exception(s"Local Storage cannot manage ${node.getClass.getName} nodes")}
  def moveWithin(from:FSNode, to:FSNode, writeMode:WriteMode) = 
    (from, to) match { 
      case (lfrom:LocalNode, lto:LocalNode) => {
        if(lfrom.exists && lto.exists && writeMode == WriteMode.overwrite) 
          Files.move(lfrom.jPath, lto.jPath, StandardCopyOption.REPLACE_EXISTING)
        else 
          Files.move(lfrom.jPath, lto.jPath)
      } 
      case _ => throw new Exception(s"Local Storage cannot move within using (${from.getClass.getName} and  ${from.getClass.getName}) nodes")
    }
  
  def getContent(node:FSNode)
       = node match { case lNode:LocalNode => Files.newInputStream(lNode.jPath)               case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def setContent(node:FSNode, data:InputStream, writeMode:WriteMode)
       = node match { case lNode:LocalNode => 
                          writeMode match {
                            case WriteMode.overwrite => Files.copy(data, lNode.jPath, StandardCopyOption.REPLACE_EXISTING)
                            case WriteMode.ignoreIfExists => if(!this.exists(node)) Files.copy(data, lNode.jPath)
                            case WriteMode.failIfExists => Files.copy(data, lNode.jPath)  
                          } 
                      case _ => throw new Exception(s"Local Storage cannot manage ${node.getClass.getName} nodes")
       }
  def last(path:Option[String], attrPattern:Map[String, String] = Map[String, String]())  = {
    val it = this.getNode(path = path.getOrElse("/")) match { 
       case lNode => 
         if(Files.isDirectory(Paths.get(lNode.path))) {
              val list = Files.list(Paths.get(lNode.path))
              val ret = list.iterator().asScala.toArray
              list.close()
              ret.iterator
          }
          else 
            Seq(Paths.get(lNode.path)).iterator
    }
    var lastModified = Long.MinValue
    var lastFile:Option[FSNode] = None 
    while (it.hasNext) {
      val filePath = it.next()
      val fileName = filePath.getFileName().toString
      val isMatch = attrPattern.get("name") match { case Some(pattern) => !pattern.r.findFirstIn(fileName).isEmpty case _ => true}
      val modified = Files.getLastModifiedTime(filePath).toMillis()
      lastFile = if(isMatch && modified > lastModified) {
        lastModified = modified
        Some(this.getNode(path = filePath.toString))
      } else {lastFile}
    }
    lastFile
  }
  def list(node:FSNode, recursive:Boolean = false)  = 
     node match { 
       case lNode:LocalNode => 
         Some(if(Files.isDirectory(lNode.jPath)) { 
              val list = Files.list(lNode.jPath)
              val ret = list
                .toArray.map(_.asInstanceOf[LPath])
                .map(cPath => LocalNode(path = cPath.toAbsolutePath().toString(), storage=lNode.storage, sparkCanRead = this.sparkCanRead))
                .toSeq
              list.close()
              ret
          }
          else 
            Seq(LocalNode(path = lNode.jPath.toAbsolutePath().toString(), storage=lNode.storage, sparkCanRead = this.sparkCanRead))
         )
         .map(children => 
           if(!recursive) children
           else children ++ children.filter(_.isDirectory).flatMap(c => this.list(c, recursive))
         )
         .get
       case _ => throw new Exception(s"Local Storage cannot manage ${node.getClass.getName} nodes")
    }
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode
       = LocalNode(path = path, storage = this, attrs=attrs, sparkCanRead=this.sparkCanRead)
  def ensurePathExists(path:String) = Files.createDirectories(Paths.get(path))
  def getFileModificationTime(path:Option[String], attrPattern:Map[String, String]) = 
    this.getNode(path = path.getOrElse("/")) match { 
       case lNode => 
         if(!Files.exists(Paths.get(lNode.path)))
           None
         else if(Files.isDirectory(Paths.get(lNode.path))) { 
           val list = Files.list(Paths.get(lNode.path))
           val ret = list.iterator().asScala.toArray
             .map(cPath => getFileModificationTime(path = Some(cPath.toString), attrPattern = attrPattern))
             .foldLeft(None.asInstanceOf[Option[Long]]){(current, iter) => (current, iter) match {
               case (Some(i), Some(j)) => if(i > j) Some(i) else Some(j)
               case (Some(i), None) => Some(i)
               case (None, Some(j)) => Some(j)
               case _ => None
             }}
           list.close()
           ret
         }
         else { 
           val filePath = Paths.get(lNode.path)
           val fileName = filePath.getFileName.toString
           val isMatch = attrPattern.get("name") match { case Some(pattern) => !pattern.r.findFirstIn(fileName).isEmpty case _ => true}
           if(isMatch)
             Some(Files.getLastModifiedTime(filePath).toMillis)
           else
             None
            
        }
    }
  def getWriter(node:FSNode, writeMode:WriteMode)= node match { 
    case lNode:LocalNode => 
      writeMode match {
        case WriteMode.overwrite => Files.newOutputStream(lNode.jPath, if(this.exists(node)) StandardOpenOption.TRUNCATE_EXISTING else StandardOpenOption.CREATE)
        case WriteMode.ignoreIfExists => if(!this.exists(node)) Files.newOutputStream(lNode.jPath) else null
        case WriteMode.failIfExists => Files.newOutputStream(lNode.jPath)  
      } 
     case _ => throw new Exception(s"Local Storage cannot manage ${node.getClass.getName} nodes")
  }
}

case class EpiFileStorage(vooUrl:String, user:String, pwd:String) extends Storage {
  override val protocol:String="epi" 
  override val isLocal:Boolean = false 
  val localStorage:LocalStorage=LocalStorage()
  override val sparkCanRead = false
  val systemTmpDir = null 
  def exists(node:FSNode) 
       = node match { case eNode:EpiFileNode => eNode.path != null && EpiFiles.exists(id = eNode.path, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd)          
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def isDirectory(node:FSNode) = false
  def delete(node:FSNode, recurse:Boolean = false) { throw new Exception("Not implemented") }
  def moveWithin(from:FSNode, to:FSNode, writeMode:WriteMode) { throw new Exception("Not implemented") }
  def getContent(node:FSNode)
       = node match { case eNode:EpiFileNode => { EpiFiles.download(id = eNode.path, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd) }
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def setContent(node:FSNode, data:InputStream, writeMode:WriteMode)
       = node match { case eNode:EpiFileNode => {
                          writeMode match {
                            case WriteMode.overwrite => throw new Exception("Epifiles does not support overriding files yet")
                            case WriteMode.ignoreIfExists => 
                              if(!this.exists(node)) {
                                val tmp = localStorage.getTmpNode().setContent(data)
                                val id = EpiFiles.epifileUpload(vooUrl=this.vooUrl, user=this.user, pwd=this.pwd, path=tmp.path, name=node.attrs("name"), comment=node.attrs.get("comment").getOrElse(""))
                                tmp.delete()
                                eNode.setPath(id.get) 
                              }
                            case WriteMode.failIfExists =>   
                              if(!this.exists(node)) {
                                val tmp = localStorage.getTmpNode().setContent(data)
                                val id = EpiFiles.epifileUpload(vooUrl=this.vooUrl, user=this.user, pwd=this.pwd, path=tmp.path, name=node.attrs("name"), comment=node.attrs.get("comment").getOrElse(""))
                                tmp.delete()
                                eNode.setPath(id.get) 
                              } else {
                                throw new Exception("File exists and override WriteMode.ignoreIfExists has not been provided")
                              }

                          }
                      }
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def list(node:FSNode, recursive:Boolean)  = throw new Exception("Not implemented")
  def last(path:Option[String], attrPattern:Map[String, String] = Map[String, String]()) = {
     val commentPattern = attrPattern.get("comment")
     val namePattern = attrPattern.get("name")

     EpiFiles.findFile(commentPattern = commentPattern, namePattern=namePattern, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd) match {
       case Some((name, comment, id, date)) => Some(EpiFileNode(path = id, storage=this, attrs = Map[String, String]("comment"->comment, "name"->name, "date"->date, "id"->id)))
       case _ => None
     }
  }
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode
       = EpiFileNode(path = path, storage = this, attrs=attrs)
  def ensurePathExists(path:String) = throw new Exception("Not implemented")
  def getFileModificationTime(path:Option[String], attrPattern:Map[String, String]) = last(path = path, attrPattern = attrPattern) match {case Some(n) => Some(n.attrs("date").toLong) case _ => None }
  def getWriter(node:FSNode, writeMode:WriteMode) = throw new Exception("Not implemented")
}
case class HDFSStorage(hadoopConf:Configuration, override val tmpPrefix:String="demy_") extends Storage {
  override val protocol:String="hdfs"
  override val isLocal:Boolean = false 
  override val sparkCanRead = true

  val localStorage:LocalStorage = LocalStorage()
  val fs = FileSystem.get(hadoopConf)
  val systemTmpDir = hadoopConf.get("hadoop.tmp.dir")
  def exists(node:FSNode) = 
    node match { 
      case hNode:HDFSNode => fs.exists(hNode.hPath)          
      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
    }
  def isDirectory(node:FSNode) = 
    node match { 
      case hNode:HDFSNode => fs.isDirectory(hNode.hPath)     
      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
    }
  def delete(node:FSNode, recurse:Boolean = false) = 
    node match { 
      case hNode:HDFSNode => fs.delete(hNode.hPath, recurse) 
      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
    }
  def moveWithin(from:FSNode, to:FSNode, writeMode:WriteMode) = 
    (from, to) match { 
      case (hfrom:HDFSNode, hto:HDFSNode) => {
        if(hfrom.exists && hto.exists && writeMode == WriteMode.overwrite) 
          fs.rename(hfrom.hPath, hto.hPath)
        else if(hfrom.exists && !hto.exists )
          fs.rename(hfrom.hPath, hto.hPath)
        else
          throw new Exception("Cannot overwrite destination no overwrite flag provided")
      } 
      case _ => throw new Exception(s"HDFS Storage cannot move within using (${from.getClass.getName} and  ${from.getClass.getName}) nodes")
    }
  def getContent(node:FSNode)
       = node match { case hNode:HDFSNode => fs.open(hNode.hPath)            case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def setContent(node:FSNode, data:InputStream, writeMode:WriteMode)
       = node match { 
         case hNode:HDFSNode => 
           val writer = writeMode match {
             case WriteMode.overwrite => Some(fs.create(hNode.hPath, true))
             case WriteMode.ignoreIfExists => if(!this.exists(node)) Some(fs.create(hNode.hPath)) else None
             case WriteMode.failIfExists => Some(fs.create(hNode.hPath))
           }
           if(node.attrs.contains("replication")) fs.setReplication(hNode.hPath,node.attrs("replication").toShort)
           writer match {
             case Some(w) =>
               IOUtils.copy(data,w)
               w.close() 
             case _ => {} 
           }
        case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def last(path:Option[String], attrPattern:Map[String, String] =  Map[String, String]())  = {
    val it = fs.listFiles(new HPath(path.getOrElse("/")), true)
    var lastModified = Long.MinValue
    var lastFile:Option[FSNode] = None 
    while (it.hasNext()) {
      val file = it.next()
      val filePath = file.getPath()
      val fileName = filePath.getName()
      val isMatch = attrPattern.get("name") match { case Some(pattern) => !pattern.r.findFirstIn(fileName).isEmpty case _ => true}
      val modified = file.getModificationTime()
      lastFile = if(isMatch && modified > lastModified) {
        lastModified = modified
        Some(this.getNode(path = filePath.toString))
      } else {lastFile}
    }
    lastFile
  }
  def list(node:FSNode, recursive:Boolean = false)  =
        node match { 
          case hNode:HDFSNode => 
            fs.listStatus(hNode.hPath)
              .map(st => this.getNode(path = st.getPath.toString))
              .toSeq
              .flatMap{ node => 
                if(node.isDirectory && recursive)
                  this.list(node, recursive)
                else
                  Seq(node)
              }
          case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode
       = HDFSNode(path = path, storage = this, attrs=attrs)
  def ensurePathExists(path:String) = fs.mkdirs(new HPath(path)
    )
  override def copy(from:FSNode, to:FSNode, writeMode:WriteMode) {
    if(!from.isDirectory)
      super.copy(from = from, to = to, writeMode = writeMode)
    else if(from.isLocal && to.storage == this) {
      if(writeMode != WriteMode.ignoreIfExists || !to.exists )
        fs.copyFromLocalFile(false, writeMode == WriteMode.overwrite, new HPath(from.path), new HPath(to.path))
    } else if(from.storage == this && to.isLocal) {
       if(writeMode != WriteMode.ignoreIfExists || !to.exists )
         fs.copyToLocalFile(false, new HPath(from.path), new HPath(to.path))
    } else {
      throw new Exception("Copy opreation not supportes @epi")
    }
  }
  def getFileModificationTime(path:Option[String], attrPattern:Map[String, String]) = {
    val it = fs.listFiles(new HPath(path.getOrElse("/")), true)
    var lastModified:Option[Long] = None
    while (it.hasNext()) {
      val file = it.next()
      val filePath = file.getPath()
      val fileName = filePath.getName()
      val isMatch = attrPattern.get("name") match { case Some(pattern) => !pattern.r.findFirstIn(fileName).isEmpty case _ => true}
      val modified = file.getModificationTime()
      lastModified = 
        if(isMatch && modified > lastModified.getOrElse(Long.MinValue)) {
          Some(modified)
      } else 
          lastModified
    }
    lastModified
  }
  def getWriter(node:FSNode, writeMode:WriteMode)
       = node match { 
         case hNode:HDFSNode => {
           val writer = writeMode match {
             case WriteMode.overwrite => Some(fs.create(hNode.hPath, true))
             case WriteMode.ignoreIfExists => if(!this.exists(node)) Some(fs.create(hNode.hPath)) else None
             case WriteMode.failIfExists => Some(fs.create(hNode.hPath))
           }
           if(node.attrs.contains("replication")) fs.setReplication(hNode.hPath,node.attrs("replication").toShort)
           writer.getOrElse(null)
          }
          case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
 
}
