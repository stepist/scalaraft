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
  def getStates(dbKeys:List[PersistentStateDBKey]):List[(PersistentStateDBKey,Option[Any])]

  def putState(dbKey:PersistentStateDBKey,data:Any):Boolean
  def putStates( pairs:Map[PersistentStateDBKey,Any] )


  def setTermInfo(a:TermInfo)

  def setMyId(myId:MyId)
  def setRaftMembership(a:RaftMembership)
  def setElectionTimeout(a:Int)


  def getTermInfo:TermInfo

  def getMyId:MyId
  def getRaftMembership:RaftMembership
  def getElectionTimeout:Int

  def close
}
