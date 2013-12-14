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
class LogEntryLevelDB(val dbName:String,val dbRootPath:String=null) extends LogEntryDB  {
  import LogEntryDB._

  val db :DB = factory.open(new File(dbRootPath,dbName), new Options())
  implicit val writeOption=new WriteOptions().sync(true)

  def unPickle[T: Unpickler: FastTypeTag](data:Array[Byte]):T = binary.toBinaryPickle(data).unpickle[T]
  def doPickle[T: SPickler: FastTypeTag](a:T):Array[Byte]=a.pickle.value

  case class CKey(key:Array[Byte] )
  type IterInit = (DBIterator) => Unit
  type IterExec[T] = (List[T],DBIterator)=> List[T]

  type Index = Long
  def translateIndex(index:Index):Index=Long.MaxValue-index
  implicit def translateIndexToCKey(index:Index):CKey= {
    val translatedIndex=translateIndex(index)
    CKey(doPickle(translatedIndex))
  }


  def dbDelete(ckey:CKey)(implicit options:WriteOptions) =  db.delete(ckey.key,options)
  def dbPut(ckey:CKey , value:Array[Byte])(implicit options:WriteOptions) =db.put(ckey.key, value , writeOption)
  def dbGet(ckey:CKey)=db.get(ckey.key)


  def iterSeekToLast(iter:DBIterator) = iter.seekToFirst
  def iterSeek(iter:DBIterator,ckey:CKey) = iter.seek(ckey.key)
  def iterPrev(iter:DBIterator) = iter.next
  def iterNext(iter:DBIterator) = iter.prev
  def iterPeekPrev(iter:DBIterator) = iter.peekNext
  def iterPeekNext(iter:DBIterator) = iter.peekPrev
  def iterHasPrev(iter:DBIterator) = iter.hasNext
  def iterHasNext(iter:DBIterator) = iter.hasPrev

  def batchDelete(batch:WriteBatch,ckey:CKey)= batch.delete(ckey.key)
  def batchPut(batch:WriteBatch,ckey:CKey,value:Array[Byte])= batch.put(ckey.key,value)




  def deleteEntry(index:Index)=dbDelete(index)

  def delete(list:List[Index]){
    val batch :WriteBatch= db.createWriteBatch()
    for(index<-list) batchDelete(batch,index)
  }

  implicit class BatchIteration(val batch:WriteBatch) {
    def iterate[A](list:List[A],exec:(WriteBatch,A) => Unit ){
      try {
        for(item<-list) exec(batch,item)
        db.write(batch,writeOption)
      } finally {
        batch.close()
      }
    }
  }

  def deleteFrom(index:Long){
    val lastIndexSomething = getLastIndex()
    for (lastIndex <-lastIndexSomething) {
      if (index > lastIndex) return

      val batch :WriteBatch= db.createWriteBatch()
      try {
        for(index2<- index to lastIndex) batchDelete(batch,index2)
        db.write(batch,writeOption)
      } finally {
        batch.close()
      }
    }
  }



  def appendEntry(logEntry:LogEntry)=dbPut(logEntry.index,doPickle(logEntry))

  def appendEntries(logEntries : Array[LogEntry]){
    val batch :WriteBatch= db.createWriteBatch()
    try {
      for(logEntry <- logEntries) batchPut(batch,logEntry.index, doPickle(logEntry))
      db.write(batch,writeOption)
    } finally {
      batch.close()
    }
  }

  def getEntry(index:Index) : Option[LogEntry] = {
    val entryBytes=dbGet(index)
    if (entryBytes==null) None else Some(unPickle[LogEntry](entryBytes))
  }

  def getLast():Option[LogEntry]={
    val exec:IterExec[LogEntry] = (x,it) =>{ x :+ unPickle[LogEntry](iterPeekPrev(it).getValue) }
    db.iterator iterateBack(  1, iterSeekToLast(_),exec ) map(_.head)
  }

  def getLastN(n:Int):Option[List[LogEntry]]={
    val exec:IterExec[LogEntry] = (x,it) =>{ x :+ unPickle[LogEntry](iterPeekPrev(it).getValue) }
    db.iterator iterateBack(  n, iterSeekToLast(_),exec )
  }

  def getLastNFrom(n:Int,index:Index):Option[List[LogEntry]]={
    val exec:IterExec[LogEntry] = (x,it) =>{ x :+ unPickle[LogEntry](iterPeekPrev(it).getValue) }
    db.iterator iterateBack( n,iterSeek(_,index), exec )
  }

  def getLastIndex():Option[Long]={
    val exec:IterExec[Long] = (x,it) =>{ x :+ unPickle[Long](iterPeekPrev(it).getKey) }
    db.iterator iterateBack (1, iterSeekToLast(_),exec) map(_.head)
  }

  def close = db.close




  implicit class levelDBIteration(val iterator:DBIterator) {
    def iterateBack[A](n:Int, initExec: DBIterator=>Unit,  exec: (List[A],DBIterator)=> List[A]):Option[List[A]] ={
      if (n<0) return None

      var list: List[A]=List[A]()
      try{
        initExec(iterator)
        var count=0
        while(iterHasPrev(iterator) && count<n){
          list=exec(list,iterator)
          iterator.next()
          iterPrev(iterator)
          count+=1
        }
      } finally {
        iterator.close()
      }
      Some(list)
    }

    def iterateForward[A](n:Int, initExec: DBIterator=>Unit,  exec: (List[A],DBIterator)=> List[A]):Option[List[A]] ={
      if (n<0) return None

      var list: List[A]=List[A]()
      try{
        initExec(iterator)
        var count=0
        while(iterHasNext(iterator) && count<n){
          list=exec(list,iterator)
          iterNext(iterator)
          count+=1
        }
      } finally {
        iterator.close()
      }
      Some(list)
    }
  }
}


object test extends App{
  var a:List[Int]=Nil
  val b = 3 :: a
  val b2 = b :+ 9


  println("hello")

}