package jkm.cineclub.raft.DB

import jkm.cineclub.raft.DB.LogEntryDB.LogEntry
import scala.collection.immutable.TreeMap

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/17/13
 * Time: 7:40 PM
 * To change this template use File | Settings | File Templates.
 */

class LogEntryMemroyDB extends LogEntryDB{
  var a=new TreeMap[Index,LogEntry]()

  def dbName: String = "MemoryDB"

  def dbRootPath: String = ""

  def deleteEntry(index: Index) =  a = a - index

  def deleteFrom(index: Index) = a = a.dropWhile(_._1>index)

  def appendEntry(logEntry: LogEntry) =  a = a + ( logEntry.index -> logEntry)

  def appendEntries(logEntries: List[LogEntry]) = logEntries.foreach(appendEntry(_))

  def getEntry(index: Index): Option[LogEntry] = a.get(index)

  def getLast(): Option[LogEntry] = a.lastOption.map(_._2)

  def getLastN(n: Int): List[Option[LogEntry]] = a.takeRight(n).map(_._2).toList.map(Some(_)).reverse

  def getLastNFrom(n: Int, index: Index): List[Option[LogEntry]] = a.to(index).takeRight(n).map(_._2).toList.map(Some(_)).reverse

  def getLastIndex(): Option[Long] = a.lastOption.map(_._1)

  def close() {}
}


object TestTreeMap extends App {
  var a =new  TreeMap[Int,String]()
  a=a + ( 1 -> "asdf" )
  println(a)
  a= a - 1
  println(a)

  for(i <- 1 to 10 ) a=a + ( i -> ("abc"+i) )
  println(a)



  val b=new TreeMap[Int,String]()
  println(b.last)

}