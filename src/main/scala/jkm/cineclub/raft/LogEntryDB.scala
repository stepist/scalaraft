package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/14/13
 * Time: 4:17 PM
 * To change this template use File | Settings | File Templates.
 */
trait LogEntryDB {
  def dbName:String
  def dbRootPath:String

  import LogEntryDB._

  def deleteEntry(index:Long)
  def deleteFrom(index:Long)

  def appendEntry(logEntry:LogEntry)
  def appendEntries(logEntries : Array[LogEntry])

  def getEntry(index:Long) : Option[LogEntry]
  def getLast():Option[LogEntry]
  def getLastN(n:Int):Array[LogEntry]
  def getLastNFrom(n:Int,index:Int):Array[LogEntry]

  def getLastIndex():Option[Long]


  def close()

}

object LogEntryDB{
  case class LogEntry(index:Long,term:Long,command:String)



}