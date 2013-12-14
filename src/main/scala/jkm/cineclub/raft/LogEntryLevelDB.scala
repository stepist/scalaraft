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

  implicit class BatchIteration(val batch:WriteBatch) {
    def batchOp[A](list:List[A],exec:(WriteBatch,A) => Unit ){
      try {
        for(item<-list) exec(batch,item)
        db.write(batch,writeOption)
      } finally {
        batch.close()
      }
    }
  }

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
  }

  def deleteEntry(index:Index)=dbDelete(index)

  def deleteFrom(index:Index) =
    for(lastIndex<-getLastIndex())
      if (index <= lastIndex) db.createWriteBatch batchOp( (index to lastIndex).toList , (btc,idx:Index)=>batchDelete(btc,idx) )

  def appendEntry(logEntry:LogEntry)=dbPut(logEntry.index,doPickle(logEntry))

  def appendEntries(logEntries : List[LogEntry]) = db.createWriteBatch batchOp( logEntries, (btc,e:LogEntry)=>batchPut(btc,e.index, doPickle(e)) )

  def getEntry(index:Index) : Option[LogEntry] = Some(unPickle[LogEntry](dbGet(index)))

  val execIterPeekPrevGetValue:IterExec[LogEntry] = (x,it) =>{ x :+ unPickle[LogEntry](iterPeekPrev(it).getValue) }
  val execIterPeekPrevGetKey:IterExec[Long] = (x,it) =>{ x :+ unPickle[Long](iterPeekPrev(it).getKey) }

  def getLast():Option[LogEntry] = db.iterator iterateBack( 1, iterSeekToLast(_), execIterPeekPrevGetValue) map(_.head)

  def getLastN(n:Int):Option[List[LogEntry]] = db.iterator iterateBack( n, iterSeekToLast(_), execIterPeekPrevGetValue)

  def getLastNFrom(n:Int,index:Index):Option[List[LogEntry]] = db.iterator iterateBack( n, iterSeek(_,index), execIterPeekPrevGetValue)

  def getLastIndex():Option[Long] = db.iterator iterateBack ( 1, iterSeekToLast(_), execIterPeekPrevGetKey) map(_.head)


  def close = db.close

}


object test extends App{
  var a:List[Int]=Nil
  val b = 3 :: a
  val b2 = b :+ 9


  println("hello")

  import LogEntryDB._

  val db :DB = factory.open(new File("testLevelDB","testLevelDB"), new Options())
  implicit val writeOption=new WriteOptions().sync(true)

  for(i<- 1 to 10)  db.put((10-i).pickle.value,("c"+i).pickle.value)



  val iter=db.iterator

  iter.seekToFirst
  println(binary.toBinaryPickle(iter.peekNext().getValue).unpickle[String])
  iter.close


  val iter2=db.iterator
  iter2.seekToLast
  println(binary.toBinaryPickle(iter2.peekPrev().getValue).unpickle[String])
  iter2.close


  db.close()



}