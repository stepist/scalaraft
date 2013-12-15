package jkm.cineclub.raft.DB

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

  type Index = Long

  import LogEntryDB._

  def deleteEntry(index:Index)
  def deleteFrom(index:Index)

  def appendEntry(logEntry:LogEntry)
  def appendEntries(logEntries : List[LogEntry])

  def getEntry(index:Index) : Option[LogEntry]
  def getLast():Option[LogEntry]
  def getLastN(n:Int):List[Option[LogEntry]]
  def getLastNFrom(n:Int,index:Index):List[Option[LogEntry]]

  def getLastIndex():Option[Long]


  def close()

}

object LogEntryDB{
  case class LogEntry(index:Long,term:Long,command:String)
}