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

  case class PersistentStateItem[T](value:T,dbKey:String)

  case class RaftMemberIdWrap(override val value:RaftMemberId=null)  extends PersistentStateItem[RaftMemberId](value,"RaftMemberId")
  case class MyIdWrap(override val value:MyId=null)  extends PersistentStateItem[MyId](value,"MyId")
  case class RaftMembershipWrap(override val value:RaftMembership=null)  extends PersistentStateItem[RaftMembership](value,"RaftMembership")
  case class TermInfoWrap(override val value:TermInfo=null)  extends PersistentStateItem[TermInfo](value,"TermInfo")
  case class LeaderCommitIndexWrap(override val value:LeaderCommitIndex= -1)  extends PersistentStateItem[LeaderCommitIndex](value,"LeaderCommitIndex")
  case class LastAppliedIndexWrap(override val value:LastAppliedIndex= -1)  extends PersistentStateItem[LastAppliedIndex](value,"LastAppliedIndex")
  case class LastAppendedIndexWrap(override val value:LastAppendedIndex= -1)  extends PersistentStateItem[LastAppendedIndex](value,"LastAppendedIndex")

  def createMyId(myId:MyId)=new PersistentState(myId=MyIdWrap(myId))


}
import PersistentState._
class PersistentState(
  val raftMemberId:RaftMemberIdWrap=null ,
  val myId:MyIdWrap =null,
  val raftMembership:RaftMembershipWrap =null ,
  val termInfo: TermInfoWrap =null ,
  val leaderCommitIndex: LeaderCommitIndexWrap =null,
  val lastAppliedIndex: LastAppliedIndexWrap =null,
  val lastAppendedIndex: LastAppendedIndexWrap =null
)







