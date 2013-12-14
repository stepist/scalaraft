package jkm.cineclub.raft


import org.iq80.leveldb.{WriteBatch, WriteOptions, Options, DB}
import org.iq80.leveldb.impl.Iq80DBFactory._
import scala.Some
import java.io.File
import java.nio.ByteBuffer
import scala.pickling._
import binary._

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/14/13
 * Time: 4:20 PM
 * To change this template use File | Settings | File Templates.
 */
class LogEntryLevelDBJava(val dbName:String,val dbRootPath:String=null) extends LogEntryDB  {
  val db :DB = factory.open(new File(dbRootPath,dbName), new Options())
  val writeOption=new WriteOptions().sync(true)

  import LogEntryDB._

  type ReverseIdx = Long
  type ReverseIdxBytes = Array[Byte]


  def getReverseIdxBytesFromIndex(index:Long):ReverseIdxBytes = ByteBuffer.allocate(8).putLong(getReverseIdxFromIndex(index)).array()
  def getIndexFromReverseIdx(rIdx:ReverseIdx)= Long.MaxValue-rIdx
  def getReverseIdxFromIndex(index:Long)= Long.MaxValue-index


  def deleteEntry(index:Long){
    val key = getReverseIdxBytesFromIndex(index)
    db.delete(key,writeOption)
  }

  def deleteFrom(index:Long){
    val lastIndexSomething = getLastIndex()
    for (lastIndex <-lastIndexSomething) {
      if (index > lastIndex) return

      val batch :WriteBatch= db.createWriteBatch()
      try {
        for(index2<- index to lastIndex){
          val idx=getReverseIdxFromIndex(index2)
          val key = getReverseIdxBytesFromIndex(idx)
          batch.delete(key)
        }
        db.write(batch,writeOption)
      } finally {
        batch.close()
      }
    }
  }

  def appendEntry(logEntry:LogEntry){
    val idx=getReverseIdxFromIndex(logEntry.index)
    val key = getReverseIdxBytesFromIndex(idx)
    db.put(key, logEntry.pickle.value , writeOption)
  }

  def appendEntries(logEntries : Array[LogEntry]){
    //for(logEntry <- logEntries)  appendEntry(logEntry)
    val batch :WriteBatch= db.createWriteBatch()
    try {
      for(logEntry <- logEntries) {
        val idx=getReverseIdxFromIndex(logEntry.index)
        val key = getReverseIdxBytesFromIndex(idx)
        batch.put(key, logEntry.pickle.value)
      }
      db.write(batch,writeOption)
    } finally {
      batch.close()
    }

  }

  def getEntry(index:Long) : Option[LogEntry] = {
    val idx=getReverseIdxFromIndex(index)
    val key = getReverseIdxBytesFromIndex(idx)
    val re=db.get(key)
    if (re==null) return None
    Some(unPickle(re))
  }

  def unPickle(data:Array[Byte]):LogEntry = binary.toBinaryPickle(data).unpickle[LogEntry]

  def getLast():Option[LogEntry]={
    var re : Option[LogEntry] = None
    val iterator= db.iterator
    try{
      iterator.seekToFirst()
      if (iterator.hasNext )
        re=Some(unPickle(iterator.peekNext().getValue))
    } finally {
      iterator.close()
    }
    re
  }

  def getLastN(n:Int):Array[LogEntry]={
    var logEntries: Array[LogEntry] =Array()
    val iterator= db.iterator

    try{
      iterator.seekToFirst()
      var count=0
      while(iterator.hasNext() && count<n){
        logEntries :+= unPickle(iterator.peekNext().getValue)
        iterator.next()
        count+=1
      }
    } finally {
      iterator.close()
    }
    logEntries
  }

  def getLastNFrom(n:Int,index:Int):Array[LogEntry]={
    val idx=getReverseIdxFromIndex(index)
    var logEntries: Array[LogEntry] =Array()
    val iterator= db.iterator

    try {
      iterator.seek(getReverseIdxBytesFromIndex(idx))
      var count=0
      while(iterator.hasNext() && count<n){
        logEntries :+= unPickle(iterator.peekNext().getValue)
        iterator.next()
        count+=1
      }
    } finally {
      iterator.close()
    }
    logEntries
  }

  def getLastIndex():Option[Long]={
    var re:Option[Long] = None

    val iterator= db.iterator
    try{
      iterator.seekToFirst()
      if ( iterator.hasNext) {
        val reverseIdx=ByteBuffer.wrap(iterator.peekNext().getKey).getLong
        re=Some(getIndexFromReverseIdx(reverseIdx))
      }
    } finally {
      iterator.close()
    }
    re
  }

  def getSomething[

  def getIndex(idx:Long)= Long.MaxValue-idx
  def getIdx(index:Long)= Long.MaxValue-index



  def close = db.close

}

