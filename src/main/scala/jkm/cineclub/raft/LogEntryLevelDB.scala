package jkm.cineclub.raft

import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import scala._
import java.io.File
import java.nio.ByteBuffer
import scala.pickling._
import binary._
import scala.Some

import java.util.Map

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

  type BA = Array[Byte]
  type DBEntry = Map.Entry[BA,BA]

  def unPickle[T: Unpickler: FastTypeTag](data:BA):T = binary.toBinaryPickle(data).unpickle[T]
  def doPickle[T: SPickler: FastTypeTag](a:T):BA=a.pickle.value
  //def unPickle2[T: Unpickler: FastTypeTag](data:BA,tt:T):T = binary.toBinaryPickle(data).unpickle[T]


  type IterInit = (DBIterator) => Unit
  type IterExec[T] = (DBEntry)=> T

  def translateIndex(index:Index):Index = Long.MaxValue-index
  implicit def translateIndexToKey(index:Index):BA = doPickle(translateIndex(index))

  def dbDelete(index:Index)(implicit options:WriteOptions) = db.delete(index,options)
  def dbPut(index:Index, value:Array[Byte])(implicit options:WriteOptions) = db.put(index, value , writeOption)
  def dbGet(index:Index) = db.get(index)

  def iterSeekToLast(iter:DBIterator) = iter.seekToFirst
  def iterSeek(iter:DBIterator,index:Index) = iter.seek(index)
  def iterPrev(iter:DBIterator) = iter.next
  def iterNext(iter:DBIterator) = iter.prev
  def iterPeekPrev(iter:DBIterator) = iter.peekNext
  def iterPeekNext(iter:DBIterator) = iter.peekPrev
  def iterHasPrev(iter:DBIterator) = iter.hasNext
  def iterHasNext(iter:DBIterator) = iter.hasPrev

  def batchDelete(batch:WriteBatch,index:Index)= batch.delete(index)
  def batchPut(batch:WriteBatch,index:Index,value:Array[Byte])= batch.put(index,value)

  implicit class BatchIteration(val batch:WriteBatch) {
    def batchOp[A](list:List[A],exec:(WriteBatch,A) => Unit ){
      try {
        for(item<-list) exec(batch,item)
        db.write(batch,writeOption)
      }
      finally batch.close()
    }
  }

  implicit class levelDBIteration(val iterator:DBIterator) {
    def iterateBack[A](n:Int, initExec: DBIterator=>Unit,  exec: IterExec[A]):Option[List[A]] ={
      if (n<0) return None

      val tt:A = None.get

      var list: List[A]= Nil
      try{
        initExec(iterator)
        var count=0
        while(iterHasPrev(iterator) && count<n){
          list = list :+ exec(iterPeekPrev(iterator))
          iterPrev(iterator)
          count+=1
        }
      }
      finally iterator.close()

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

  def getLast():Option[LogEntry] = db.iterator iterateBack( 1, iterSeekToLast(_), x => unPickle[LogEntry](x.getValue) ) map(_.head)

  def getLastN(n:Int):Option[List[LogEntry]] = db.iterator iterateBack( n, iterSeekToLast(_), x => unPickle[LogEntry](x.getValue))

  def getLastNFrom(n:Int,index:Index):Option[List[LogEntry]] = db.iterator iterateBack( n, iterSeek(_,index), x => unPickle[LogEntry](x.getValue))

  def getLastIndex():Option[Index] = db.iterator iterateBack ( 1, iterSeekToLast(_),x => unPickle[Index](x.getKey)) map(_.head)


  def close = db.close

}

object TestLogEntryLevelDB extends App{
  import LogEntryDB._
  val dbName="TestLevelDB2"

  try{
  factory.destroy(new File(dbName), new Options())
  }catch{
    case e:Exception => println(e)
  }


  val levelDB= new LogEntryLevelDB(dbName)

 //for(i <- 1 to 10) levelDB.appendEntry(LogEntry(i,i+1,"test"+i))
  //for(i <- 1 to 10) println(levelDB.getEntry(i))

  //println(levelDB.doPickle(null))
  val a:Array[Byte] = null
  levelDB.unPickle[Int](a)


  levelDB.close

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