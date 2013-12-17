package jkm.cineclub.raft.DB

import jkm.cineclub.raft.PersistentState

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

  def set(ps:PersistentState)
  def get(ps:PersistentState):PersistentState
}
