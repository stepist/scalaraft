package jkm.cineclub.raft

import jkm.cineclub.raft.DB.LogEntryDB.LogEntry
import jkm.cineclub.raft.DB.LogEntryLevelDB
import jkm.cineclub.raft.RaftConfig.DBInfo

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/25/13
 * Time: 10:31 PM
 * To change this template use File | Settings | File Templates.
 */
class DummyStateMachine(dbIno:DBInfo) extends LogEntryLevelDB(dbIno) with StateMachine{
  def applyEntry(logEntry: LogEntry): String = {
    appendEntry(logEntry)
    "ok"
  }

  def applyEntries(logEntries: List[LogEntry]): String = {
    appendEntries(logEntries)
    "ok"
  }

  def getLastAppliedIndex: Long = {
    println("getLastIndex()="+getLastIndex())
    getLast()
    getLastIndex().getOrElse(0)
  }
}
