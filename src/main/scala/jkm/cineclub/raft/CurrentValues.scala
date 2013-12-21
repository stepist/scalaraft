package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/20/13
 * Time: 10:15 PM
 * To change this template use File | Settings | File Templates.
 */
object CurrentValues {
  import PersistentState._
  object MemberState{
    val Leader:Int=1
    val Follower:Int=2
    val Candidate:Int=3

    val LeaderStr="leader"
    val FollowerStr="follower"
    val CandidateStr= "candidate"

    def getMemberStateStr(state:Int):String={
      state match {
        case Leader => LeaderStr
        case Follower => FollowerStr
        case Candidate => CandidateStr
        case _ => null
      }
    }

    def getMemberState(stateStr:String):Int={
      stateStr match {
        case LeaderStr => Leader
        case FollowerStr => Follower
        case CandidateStr => Candidate
        case _ => -1
      }
    }
  }



  var myId:MyId=null

  var memberState:Int=MemberState.Follower
  var leaderId:RaftMemberId=null

  var currentTerm:Long = -1
  var votedFor:RaftMemberId=null

  type AkkaAddress=String
  var addressTable:Map[RaftMemberId,AkkaAddress]=null
  var raftMembership:RaftMembership=null

  var lastIndex:Long= -1

  def printCurrentValues={

    println("myId="+myId)
    println("memberState="+MemberState.getMemberStateStr(memberState))
    println("leaderId="+leaderId)
    println("currentTerm="+currentTerm)
    println("votedFor="+votedFor)
    println("addressTable="+addressTable)
    println("raftMembership="+raftMembership)
  }

}

class CurrentValues {

}
