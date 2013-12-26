package jkm.cineclub.raft.DB

import jkm.cineclub.raft.RaftConfig.DBInfo

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/26/13
 * Time: 9:16 PM
 * To change this template use File | Settings | File Templates.
 */
trait RaftDB {
  def checkExistDB(dbInfo:DBInfo):Boolean

  def getLogEntryDB(dbInfo:DBInfo):LogEntryDB
  def getPersistentStateDB(dbInfo:DBInfo):PersistentStateDB
}
