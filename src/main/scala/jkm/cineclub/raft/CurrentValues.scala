package jkm.cineclub.raft


import jkm.cineclub.raft.RaftConfig.{TcpAddress, DBInfo}

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/20/13
 * Time: 10:15 PM
 * To change this template use File | Settings | File Templates.
 */
object CurrentValues {

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


  type AkkaAddress=String


}

class CurrentValues {
  import PersistentState._
  import CurrentValues._

  var myId:MyId=null

  var memberState:Int=MemberState.Follower
  var leaderId:RaftMemberId=null

  var currentTerm:Long = -1
  var votedFor:RaftMemberId=null

  var addressTable:Map[RaftMemberId,AkkaAddress]=null
  var addressTableRaw:Map[RaftMemberId,TcpAddress] =null
  var raftMembership:RaftMembership=null

  var lastIndex:Long= -1

  var persistentStateDBInfo:DBInfo=null
  var logEntryDBInfo:DBInfo=null

  var electionTimeout:Int= -1

  var commitIndex:Long = -1


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
