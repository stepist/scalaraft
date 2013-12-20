package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/20/13
 * Time: 10:15 PM
 * To change this template use File | Settings | File Templates.
 */
object CurrentState {
  import PersistentState._
  var currentTerm:Long =0


  object MemberState{
    val Leader:Int=1
    val Follower:Int=2
    val Candidate:Int=3
  }
  var currentState:Int=MemberState.Follower
  var leaderId:RaftMemberId="111.222.333."

  var votedFor:RaftMemberId=null

  val myId:MyId="me"

  val addressTable:Map[RaftMemberId,String]=Map("raft.aaa.b01" -> "akka:/dfasdf/asdfj.d")
  val memberShip:RaftMembership=null

  var lastIndex:Long=0




}
