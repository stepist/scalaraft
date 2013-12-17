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


  def set(ps: PersistentState) = {
    val batch :WriteBatch= db.createWriteBatch()

    try {
      if (ps.lastAppendedIndex != null) batch.put(LastAppendedIndexDBKey.getBytes, ps.lastAppendedIndex.pickle.value)
      if (ps.lastAppliedIndex != null) batch.put(LastAppliedIndexDBKey.getBytes, ps.lastAppliedIndex.pickle.value)
      if (ps.leaderCommitIndex != null) batch.put(LeaderCommitIndexDBKey.getBytes, ps.leaderCommitIndex.pickle.value)
      if (ps.myId != null) batch.put(MyIdDBKey.getBytes, ps.myId.pickle.value)
      if (ps.raftMemberId != null) batch.put(RaftMemberIdDBKey.getBytes, ps.raftMemberId.pickle.value)
      if (ps.raftMembership != null) batch.put(RaftMembershipDBKey.getBytes, ps.raftMembership.pickle.value)
      if (ps.termInfo != null) batch.put(TermInfoDBKey.getBytes, ps.termInfo.pickle.value)

      db.write(batch,writeOption)
    } finally {batch.close()}

  }


  //binary.toBinaryPickle(data).unpickle[T]

  def get(dbKeys:List[PersistentStateDBKey]): PersistentState = {
    var lastAppendedIndex:LastAppendedIndex = -1

    if (ps.lastAppendedIndex != null)  binary.toBinaryPickle(db.get(LastAppendedIndexDBKey.getBytes)).unpickle[LastAppendedIndex]
    if (ps.lastAppliedIndex != null) binary.toBinaryPickle(db.get(LastAppliedIndexDBKey.getBytes)).unpickle[LastAppliedIndex]
    if (ps.leaderCommitIndex != null) binary.toBinaryPickle(db.get(LeaderCommitIndexDBKey.getBytes)).unpickle[LeaderCommitIndex]
    if (ps.myId != null) binary.toBinaryPickle(db.get(MyIdDBKey.getBytes)).unpickle[MyId]
    if (ps.raftMemberId != null) binary.toBinaryPickle(db.get(RaftMemberIdDBKey.getBytes)).unpickle[RaftMemberId]
    if (ps.raftMembership != null) binary.toBinaryPickle(db.get(RaftMembershipDBKey.getBytes)).unpickle[RaftMembership]
    if (ps.termInfo != null) binary.toBinaryPickle(db.get(TermInfoDBKey.getBytes)).unpickle[TermInfo]


  }

  def get2(dbKey:PersistentStateDBKey): PersistentState =
    dbKey match{
      case LastAppendedIndexDBKey => new PersistentState(lastAppendedIndex=binary.toBinaryPickle(db.get(LastAppendedIndexDBKey.getBytes)).unpickle[LastAppendedIndex] )
      case LastAppliedIndexDBKey => new PersistentState( lastAppliedIndex= binary.toBinaryPickle(db.get(LastAppliedIndexDBKey.getBytes)).unpickle[LastAppliedIndex] )
      case LeaderCommitIndexDBKey => new PersistentState( leaderCommitIndex = binary.toBinaryPickle(db.get(LeaderCommitIndexDBKey.getBytes)).unpickle[LeaderCommitIndex] )
      case MyIdDBKey =>  new PersistentState(myId=  binary.toBinaryPickle(db.get(MyIdDBKey.getBytes)).unpickle[MyId] )
      case RaftMemberIdDBKey =>  new PersistentState( raftMemberId= binary.toBinaryPickle(db.get(RaftMemberIdDBKey.getBytes)).unpickle[RaftMemberId] )
      case RaftMembershipDBKey => new PersistentState( raftMembership =  binary.toBinaryPickle(db.get(RaftMembershipDBKey.getBytes)).unpickle[RaftMembership] )
      case TermInfoDBKey => new PersistentState( termInfo =  binary.toBinaryPickle(db.get(TermInfoDBKey.getBytes)).unpickle[TermInfo] )
    }
}

}


object TestPersistentStateLevelDB extends App{
  case class AA(a:Int)
  case class BB(b:String) extends AA(3)

  val c=BB("hello").pickle.value
  val d= binary.toBinaryPickle(c).unpickle[AA]
  println(d)

}