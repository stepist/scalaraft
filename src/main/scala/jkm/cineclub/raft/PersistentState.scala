package jkm.cineclub.raft

import jkm.cineclub.raft.PersistentState._
import jkm.cineclub.raft.PersistentState.TermInfo

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

    def getConfigType(configTypeStr:String):Int ={
      configTypeStr match {
        case "normal" => RaftMembershipConfigNormal
        case "joint"  => RaftMembershipConfigJoint
        case _ => -1
      }
    }
  }

  case class RaftMembership( configType:Int , newMembers:List[RaftMemberId], oldMembers:List[RaftMemberId]){

    def contains(memberId:RaftMemberId):Boolean= {
      newMembers.contains(memberId)
    }
    def members:List[RaftMemberId]={
      //if(configType==RaftMembership.RaftMembershipConfigNormal) newMembers
      //else oldMembers
      newMembers
    }

    def getMajoritySize:Int=members.size/2+1

  }

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
  val ElectionTimeoutDBKey:PersistentStateDBKey="ElectionTimeout"


}

class PersistentState(
  val raftMemberId:RaftMemberId = null ,
  val myId:MyId = null,
  val raftMembership:RaftMembership =null ,
  val termInfo: TermInfo =null ,
  val leaderCommitIndex: LeaderCommitIndex = -1,
  val lastAppliedIndex: LastAppliedIndex = -1,
  val lastAppendedIndex: LastAppendedIndex = -1
)







