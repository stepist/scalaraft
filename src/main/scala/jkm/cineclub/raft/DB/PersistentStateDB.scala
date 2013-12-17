package jkm.cineclub.raft.DB

import jkm.cineclub.raft.PersistentState._

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/17/13
 * Time: 9:39 PM
 * To change this template use File | Settings | File Templates.
 */
trait PersistentStateDB {
  def dbName:String
  def dbRootPath:String

  def getState(dbKey:PersistentStateDBKey):Option[Any]
  def getStates(dbKeys:List[PersistentStateDBKey]):List[Option[Any]]

  def putState(dbKey:PersistentStateDBKey,data:Any):Boolean
  def putStates( pairs:Map[PersistentStateDBKey,Any] )

  def close
}
