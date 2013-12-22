package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/23/13
 * Time: 12:33 AM
 * To change this template use File | Settings | File Templates.
 */


import jkm.cineclub.raft.DB.LogEntryDB.LogEntry

trait StateMachine {
  def applyEntry(logEntry:LogEntry):Boolean
  def applyEntries(logEntries:List[LogEntry]):Boolean
  def getLastAppliedLogEntry:LogEntry
}
