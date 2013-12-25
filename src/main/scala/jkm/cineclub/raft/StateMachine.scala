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
  def applyEntry(logEntry:LogEntry):String
  def applyEntries(logEntries:List[LogEntry]):String
  //def getLastAppliedLogEntry:LogEntry
  def getLastAppliedIndex:Long
}

object StateMachine {
  abstract class Command
  abstract class CommandRet

  //  uid 0~20 are predefined
  //  uid 1 :  Membership change ..
  // etc
}
