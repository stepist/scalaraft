package jkm.cineclub.raft.DB

import jkm.cineclub.raft.PersistentState
import org.iq80.leveldb.{WriteBatch, WriteOptions, Options, DB}
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File
import jkm.cineclub.raft.PersistentState._

import scala.pickling._
import binary._

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/17/13
 * Time: 9:47 PM
 * To change this template use File | Settings | File Templates.
 */
class PersistentStateLevelDB(val dbName:String,val dbRootPath:String=null) extends PersistentStateDB{

  val db :DB = factory.open(new File(dbRootPath,dbName), new Options())
  implicit val writeOption=new WriteOptions().sync(true)

  def batchPut[T: SPickler: FastTypeTag](batch:WriteBatch,item: PersistentStateItem[T]) =
    batch.put( item.dbKey.getBytes,item.value.pickle.value)



  def set(ps: PersistentState) = {
    val batch :WriteBatch= db.createWriteBatch()

    try {
      if (ps.lastAppendedIndex != null) batchPut(batch,ps.lastAppendedIndex)
      if (ps.lastAppliedIndex != null) batchPut(batch,ps.lastAppliedIndex)
      if (ps.leaderCommitIndex != null) batchPut(batch,ps.leaderCommitIndex)
      if (ps.myId != null) batchPut(batch,ps.myId)
      if (ps.raftMemberId != null) batchPut(batch,ps.raftMemberId)
      if (ps.raftMembership != null) batchPut(batch,ps.raftMembership)
      if (ps.termInfo != null) batchPut(batch,ps.termInfo)

      db.write(batch,writeOption)
    } finally {batch.close()}

  }


  //binary.toBinaryPickle(data).unpickle[T]

  def get(ps: PersistentState): PersistentState = {
    val a=RaftMemberIdWrap

    if (ps.lastAppendedIndex != null) binary.toBinaryPickle(db.get(LastAppendedIndexWrap().dbKey.getBytes())).unpickle[LastAppendedIndexWrap]
    if (ps.lastAppliedIndex != null) batchPut(batch,ps.lastAppliedIndex)
    if (ps.leaderCommitIndex != null) batchPut(batch,ps.leaderCommitIndex)
    if (ps.myId != null) batchPut(batch,ps.myId)
    if (ps.raftMemberId != null) batchPut(batch,ps.raftMemberId)
    if (ps.raftMembership != null) batchPut(batch,ps.raftMembership)
    if (ps.termInfo != null) batchPut(batch,ps.termInfo)

  }
}
