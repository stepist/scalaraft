package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/17/13
 * Time: 9:05 PM
 * To change this template use File | Settings | File Templates.
 */
object PersistentState {
  type RaftMemberId=String
  type MyId = RaftMemberId

  object RaftMembership{
    val RaftMembershipConfigNormal=1
    val RaftMembershipConfigJoint=2
  }
  case class RaftMembership( configType:Int , newMembership:List[RaftMemberId], oldMembership:List[RaftMemberId])

  case class TermInfo(currentTerm :Long,votedFor :RaftMemberId )

  type LeaderCommitIndex=Long
  type LastAppliedIndex=Long
  type LastAppendedIndex=Long




  type PersistentStateDBKey=String
  val RaftMemberIdDBKey:PersistentStateDBKey="RaftMemberId"
  val MyIdDBKey:PersistentStateDBKey= "MyId"
  val RaftMembershipDBKey:PersistentStateDBKey= "RaftMembership"
  val TermInfoDBKey:PersistentStateDBKey= "TermInfo"
  val LeaderCommitIndexDBKey:PersistentStateDBKey= "LeaderCommitIndex"
  val LastAppliedIndexDBKey:PersistentStateDBKey="LastAppliedIndex"
  val LastAppendedIndexDBKey:PersistentStateDBKey= "LastAppendedIndex"


}
import PersistentState._
class PersistentState(
  val raftMemberId:RaftMemberId=null ,
  val myId:MyId =null,
  val raftMembership:RaftMembership =null ,
  val termInfo: TermInfo =null ,
  val leaderCommitIndex: LeaderCommitIndex = -1,
  val lastAppliedIndex: LastAppliedIndex = -1,
  val lastAppendedIndex: LastAppendedIndex = -1
)







