package jkm.cineclub.raft


import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import scala._
import java.io.File
import java.nio.ByteBuffer
import scala.pickling._
import binary._
import scala.Some

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


  val logEntrySomething:LogEntry=null
  val longSomething=0


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
    val key = getReverseIdxBytesFromIndex(index)
    val re=db.get(key)
    if (re==null) return None
    Some(unPickle[LogEntry](re,logEntrySomething))
  }

  def unPickle[T:Unpickler:FastTypeTag](data:Array[Byte],t:T):T = binary.toBinaryPickle(data).unpickle[T]

  //def unPickle2(data:Array[Byte]):LogEntry = binary.toBinaryPickle(data).unpickle[LogEntry]


  def getLast():Option[LogEntry]={
    var re : Option[LogEntry] = None
    val iterator= db.iterator
    try{
      iterator.seekToFirst()
      if (iterator.hasNext )
        re=Some(unPickle[LogEntry](iterator.peekNext().getValue,logEntrySomething))
    } finally {
      iterator.close()
    }
    re
  }


  implicit class levelDBIteration(val iterator:DBIterator) {
    def iterate[A](n:Int, initExec: DBIterator=>Unit,  exec: (List[A],DBIterator)=> List[A]):Option[List[A]] ={
      if (n<0) return None

      var list: List[A]=List[A]()
      try{
        initExec(iterator)
        var count=0
        while(iterator.hasNext() && count<n){
          list=exec(list,iterator)
          iterator.next()
          count+=1
        }
      } finally {
        iterator.close()
      }
      Some(list)
    }
  }



  def getLastN2(n:Int):Option[List[LogEntry]]={
    db.iterator iterate(  n, _.seekToFirst(), (x:List[LogEntry],it) =>{ x :+ unPickle[LogEntry](it.peekNext().getValue,logEntrySomething) } )
  }

  def getLastN(n:Int):Option[List[LogEntry]]={
    if (n<0) return None

    var logEntries: List[LogEntry] = Nil
    val iterator= db.iterator

    try{

      iterator.seekToFirst()
      var count=0
      while(iterator.hasNext() && count<n){
        logEntries :+= unPickle[LogEntry](iterator.peekNext().getValue,logEntrySomething)
        iterator.next()
        count+=1
      }
    } finally {
      iterator.close()
    }
    Some(logEntries)
  }

  def getLastNFrom2(n:Int,index:Int):Option[List[LogEntry]]={
    val reverseIdxBytes=getReverseIdxBytesFromIndex(index)
    val exec:IterExec[LogEntry] = (x,it) =>{ x :+ unPickle[LogEntry](it.peekNext().getValue,logEntrySomething) }
    db.iterator iterate( n, _.seek(reverseIdxBytes), exec )
  }

  def getLastNFrom(n:Int,index:Int):Option[List[LogEntry]]={
    if (n<0) return None


    val reverseIdxBytes=getReverseIdxBytesFromIndex(index)
    var logEntries: List[LogEntry] = Nil
    val iterator= db.iterator

    try {
      iterator.seek(reverseIdxBytes)
      var count=0
      while(iterator.hasNext() && count<n){
        logEntries :+= unPickle[LogEntry](iterator.peekNext().getValue,logEntrySomething)
        iterator.next()
        count+=1
      }
    } finally {
      iterator.close()
    }
    Some(logEntries)
  }

  type IterInit = (DBIterator) => Unit
  type IterExec[T] = (List[T],DBIterator)=> List[T]

  def getLastIndex2():Option[Long]={
    val exec:IterExec[Long] = (x,it) =>{ x :+ unPickle[Long](it.peekNext().getKey,longSomething) }
    db.iterator iterate (1, _.seekToFirst(),exec) map(_.head)
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



  def close = db.close

}


object test extends App{
  var a:List[Int]=Nil
  val b = 3 :: a
  val b2 = b :+ 9


  println("hello")

}