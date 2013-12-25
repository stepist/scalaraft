package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/21/13
 * Time: 1:47 PM
 * To change this template use File | Settings | File Templates.
 */

import PersistentState._
import jkm.cineclub.raft.DB.{PersistentStateDB, LogEntryDB}
import LogEntryDB._
import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import scala.concurrent.duration._
import RPCHandler._
import jkm.cineclub.raft.CurrentValues.MemberState
import jkm.cineclub.raft.ClientCmdHandlerActor.ClientCommand

class Timer(implicit val context:ActorContext,implicit val cv:CurrentValues) {
  import java.util.Date

  val guard=1.1
  var requestedTime:Long = 0
  var timeout:Duration = Duration.Undefined

  def resetTimeout={
    timeout=cv.electionTimeout.millis
    requestedTime=new Date().getTime
    context.setReceiveTimeout(timeout*guard)
  }

  def resetTimeout(a:Duration = cv.electionTimeout.millis)={
    timeout=a
    requestedTime=new Date().getTime
    context.setReceiveTimeout(timeout*guard)
  }

  def isTimeout:Boolean ={
    val elapsedTime= new Date().getTime - requestedTime
    elapsedTime.millis  >= timeout
  }

  def ifTimeout(exec:() => Unit) {
    val elapsedTime= new Date().getTime - requestedTime

    if (elapsedTime.millis  >= timeout) exec()
    else resetTimeout(timeout - elapsedTime.millis)
  }

  def close= {
    requestedTime = 0
    timeout = Duration.Undefined
    context.setReceiveTimeout(Duration.Undefined)
  }
}


class RaftMember(val logEntryDB:LogEntryDB ,val persistentStateDB:PersistentStateDB,val cv:CurrentValues,val stateMachine:StateMachine)  extends Actor {

  import RaftRPC._


  val timer=new Timer()

  def setCurrentTerm(currentTerm:Long) {
    val TermInfo(term,votedFor)=persistentStateDB.getTermInfo

    persistentStateDB.setTermInfo( TermInfo(currentTerm,votedFor) )
    cv.currentTerm=currentTerm
    cv.votedFor=votedFor
  }

  def setVotedFor(votedFor:RaftMemberId) {
    val TermInfo(currentTerm,prevVotedFor)=persistentStateDB.getTermInfo

    persistentStateDB.setTermInfo( TermInfo(currentTerm,votedFor) )
    cv.currentTerm=currentTerm
    cv.votedFor=votedFor
  }


  def receive = followerBehavior

  def isLocalLogMoreCompleteThanCandidate(cLastLogIndex:Long,cLastLogTerm:Long):Boolean={
      val lastLogEntrySome= logEntryDB.getLast()
    if (lastLogEntrySome.isEmpty) return true  // when there is no logEntry in LogEntryDB ,   when server is launced for the first time.

    val LogEntry(vLastLogIndex,vLastLogTerm,_)=lastLogEntrySome.get

    (vLastLogTerm > cLastLogTerm) | (vLastLogTerm== cLastLogTerm)  & (vLastLogIndex > cLastLogIndex)
  }



  def rpcHandlerBehavior :Receive = {
    case RequestVoteRPC(RPCTo(uid,to), term,candidateId ,lastLogIndex  ,lastLogTerm ) => {

      val isValidRPCReq = to==cv.myId & term > 0 & cv.raftMembership.contains(candidateId) & lastLogIndex >0 &  lastLogTerm>0

      if (isValidRPCReq)
      term match {
        case a if a < cv.currentTerm => sender ! RequestVoteRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,false)
        case _ => {

          if ( term > cv.currentTerm ) setCurrentTerm(term)

          stepDown //?

          val isCandidateProper:Boolean= (cv.votedFor==null | cv.votedFor==candidateId) &
            !isLocalLogMoreCompleteThanCandidate(lastLogIndex,lastLogTerm)

          if ( isCandidateProper ) {
            setVotedFor(candidateId)
            sender ! RequestVoteRPCResult(RPCFrom(uid,cv.myId), cv.currentTerm,true)
            timer.resetTimeout
          }else{
            sender ! RequestVoteRPCResult(RPCFrom(uid,cv.myId), cv.currentTerm,false)
          }
        }
      }
    }
    case AppendEntriesRPC(RPCTo(uid,to), term , leaderId,prevLogIndex, prevLogTerm , entries ,commitIndex ) => {

      val isValidRPCReq = to==cv.myId & term > 0 & cv.raftMembership.contains(leaderId) & prevLogIndex >0 &  prevLogTerm>0 & commitIndex>0 & entries!=null

      if (isValidRPCReq)
      term match {
        case a if a < cv.currentTerm => sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),    cv.currentTerm,false)
        case _ => {

          if ( term > cv.currentTerm ) setCurrentTerm(term)

          stepDown //?

          timer.resetTimeout

          val logEntrySome=logEntryDB.getEntry(prevLogIndex)

          if (logEntrySome.isEmpty | logEntrySome.get.term != prevLogTerm) {

            sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,false)

          } else {

            if ( entries.size ==0 ) {
              sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,true)
            } else {

              if (entries.head != logEntrySome.get) logEntryDB.deleteFrom(prevLogIndex+1)
              logEntryDB.appendEntries(entries)
              sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,true)

              stateMachine.applyEntries(entries)     // Async?

            }
          }

        }
      }
    }
  }




  def followerBehavior :Receive = rpcHandlerBehavior orElse {
    case ReceiveTimeout => {
      timer.ifTimeout(becomeCandidate)
    }
  }














  var requestVoteRPCUidTable:Map[RaftMemberId,Long]=null
  var requestVoteRPCUID:Long = util.Random.nextLong()
  var voteList:List[RaftMemberId]=null

  def becomeCandidate() {
    context.become(candidateBehavior)
    cv.memberState=MemberState.Candidate

    setCurrentTerm(cv.currentTerm+1)
    setVotedFor(cv.myId)

    import util.Random
    timer.resetTimeout(cv.electionTimeout*(1.0+Random.nextFloat*0.7)  millis)

    val lastLogEntrySome= logEntryDB.getLast()
    val lastLogIndex = lastLogEntrySome.get.index
    val lastLogTerm = lastLogEntrySome.get.term

    requestVoteRPCUidTable= Map[RaftMemberId,Long]()
    voteList=List[RaftMemberId]()


    for ( memberId <- cv.raftMembership.members if memberId != cv.myId ) {
        requestVoteRPCUID += 3
        requestVoteRPCUidTable = requestVoteRPCUidTable + (memberId -> requestVoteRPCUID)
        context.actorSelection(cv.addressTable(memberId)) ! RequestVoteRPC(RPCTo(requestVoteRPCUID,memberId),  cv.currentTerm,cv.myId ,lastLogIndex   ,lastLogTerm  )
    }
  }

  def candidateBehavior :Receive = rpcHandlerBehavior orElse {
    case ReceiveTimeout => {
      timer.ifTimeout(becomeCandidate)
    }
    case RequestVoteRPCResult(RPCFrom(uid,from),     term, voteGranted) => {
      if ( cv.raftMembership.contains(from) &  requestVoteRPCUidTable(from) == uid & term >= cv.currentTerm) {

         if (term>cv.currentTerm) {
           setCurrentTerm(term)
           stepDown
         }
         else  {
           if (voteGranted) {
             if ( ! voteList.contains(from) ) voteList=voteList :+ from
           }
         }
      }

      if ( voteList.size >= cv.raftMembership.getMajoritySize) becomeLeader

    }
  }




















  import LeaderSubActor._


  def stepDown={
    cv.memberState=MemberState.Follower

    if (cv.memberState==MemberState.Leader ) {
      if ( nowInOperation ){
        cmdHandleActor ! "StepDown"
        nowInOperation=false
        cmdHandleActor=null
      }

      //kill LeaderSubActor
      for (leaderSubActor <- leaderSubActorTable.values)  {
        val future=leaderSubActor ?  StopLeaderSubActor
        val result = Await.result(future,10 millis).asInstanceOf[String]
        if (result!="ok") throw new RuntimeException("LeaderSubActor Stop failed")
      }

      //clear state
    }

    if (cv.memberState==MemberState.Candidate) {

    }

    timer.resetTimeout
    context.become(followerBehavior)
  }





  import RaftMemberLeader._
  var nowInOperation=false
  var lastIndex:Long=0

  var cmdHandleActor:ActorRef =null
  var isAtLeastOneEntryOfThisTermCommited=false
  var newLogEntry:LogEntry=null



  var leaderSubActorTable:Map[RaftMemberId,ActorRef]= null
  var lastAppendedIndexTable:Map[RaftMemberId,Long]=null
  var clientCmdHandlerActor:ActorRef=null

  def becomeLeader={
    nowInOperation=false
    lastIndex=0
    cmdHandleActor=null
    isAtLeastOneEntryOfThisTermCommited=false
    newLogEntry=null


    timer.close
    context.become(leaderBehavior)
    cv.memberState=MemberState.Leader



    lastIndex=logEntryDB.getLastIndex.get

    // when membeship changed ,  we need to change these tables.
    leaderSubActorTable=Map[RaftMemberId,ActorRef]()
    lastAppendedIndexTable = Map[RaftMemberId,Long]()

    for (leaderSubActor <- leaderSubActorTable.values)  leaderSubActor !   StartLeaderSubActor(lastIndex)

    //clientCmdHandlerActor=createClientCmdHandlerActor

  }

  def createcreateLeaderSubActors {
    for ( memberId <- cv.raftMembership.members if memberId != cv.myId ) {
      leaderSubActorTable = leaderSubActorTable + (memberId -> createLeaderSubActor(memberId))
    }
  }
  def createClientCmdHandlerActor:ActorRef={null}
  def createLeaderSubActor(memberId:RaftMemberId,nextIndex:Long):ActorRef = {

    context.actorOf(Props[LeaderSubActor], "leader_sub_"+memberId)


  }

  def getLastCommitedIndex(lastAppendedIndexTable:Map[RaftMemberId,Long]):Long={
      1
  }

  def leaderBehavior :Receive = rpcHandlerBehavior orElse {
    case AppendEntriesRPCResult(RPCFrom(uid,from),      term, success)  => {
      if ( cv.raftMembership.contains(from)  & term >= cv.currentTerm) {
        if (term>cv.currentTerm) {
          setCurrentTerm(term)
          stepDown
        } else {
          leaderSubActorTable(from) ! AppendEntriesRPCResult(RPCFrom(uid,from),   term,success)
        }
      }
    }



    case ClientCommand(uid,command) =>  {
      if (nowInOperation) {
        sender ! "Busy"
      } else {

        nowInOperation=true
        cmdHandleActor=sender

        newLogEntry =LogEntry(lastIndex,cv.currentTerm,command)
        logEntryDB.appendEntry(newLogEntry)

        lastIndex=logEntryDB.getLastIndex().get

        leaderSubActorTable ! NewLastLogIndex(lastIndex)
      }
    }
    case Commited(lastCommitedIndex)  => {
      val lastAppliedEntry=stateMachine.getLastAppliedLogEntry
      for(index <-lastAppliedEntry.index to (lastCommitedIndex - 1) ) stateMachine.applyEntry(logEntryDB.getEntry(index).get)
      if ( lastIndex!=lastCommitedIndex)  stateMachine.applyEntry(logEntryDB.getEntry(lastCommitedIndex).get)
      if (isAtLeastOneEntryOfThisTermCommited)  cv.commitedIndex= lastCommitedIndex

      if (nowInOperation &  lastIndex==lastCommitedIndex) {
        isAtLeastOneEntryOfThisTermCommited=true

        ret=stateMachine.applyEntry(newLogEntry)


        cmdHandleActor ! StateMachineResult(ret)
        cmdHandleActor=null
        nowInOperation=false
      }
    }
    case AppendOkNoti(memberId,nextIndex ) =>{

    }

  }




}



class LeaderSubActor(val memberId:RaftMemberId,val logEntryDB:LogEntryDB,val cv:CurrentValues) extends Actor {
  import RaftMemberLeader._
  import RaftRPC._
  import LeaderSubActor._
  import java.util.Date

  type RPCUid = Long

  case class SentedRPC(uid:RPCUid,sentedTime:Long)
  val timer=new Timer()
  var nextIndex:Long= 0
  var sentedRPC:Option[SentedRPC]=None
  var uid = util.Random.nextLong
  var lastLogIndex:Long=0

  //var commitedIndex:Long= -1

  def init{
    timer.close
    nextIndex= 0
    sentedRPC=None
    uid = util.Random.nextLong
    lastLogIndex=0
  }



   def snedRpc{
     if (sentedRPC.isEmpty | timer.isTimeout ) {
       uid=uid+7

       val prevLogEntry = logEntryDB.getEntry(nextIndex-1).get
       val prevLogIndex = prevLogEntry.index
       val prevLogTerm = prevLogEntry.term

       val entry = logEntryDB.getEntry(nextIndex).getOrElse(null)
       val entries = if (entry==null) Nil else List(entry)

       val target=  context.actorSelection(cv.addressTable(memberId))
       val rpc =  AppendEntriesRPC( RPCTo(uid,memberId),
         cv.currentTerm ,cv.myId,prevLogIndex, prevLogTerm , entries ,cv.commitedIndex )

       target ! rpc
       sentedRPC=Some(SentedRPC(uid,new Date().getTime))

       timer.resetTimeout(cv.electionTimeout*0.5 millisecond)
     }
   }

  def receive = initalState

  def initalState:Receive = {
    case StartLeaderSubActor(qlastLogIndex) =>{
      init
      lastLogIndex=qlastLogIndex
      nextIndex=lastLogIndex+1
      snedRpc
      context.become(handler)
    }
  }

   def handler:Receive={
     case StopLeaderSubActor =>{
       init
       context.become(initalState)
       sender ! "ok"
     }

     case NewLastLogIndex(qlastLogIndex)  if lastLogIndex > nextIndex  =>  {
       lastLogIndex=qlastLogIndex
       snedRpc
     }

     case AppendEntriesRPCResult(RPCFrom(uid,from),    term, success) => {
       if ( memberId==from & sentedRPC.isDefined & sentedRPC.get.uid == uid) {

         sentedRPC=None

         if ( success ) {
           context.parent ! AppendOkNoti(memberId,nextIndex )

           if ( nextIndex < (lastLogIndex+1) ) {
             nextIndex = nextIndex+1
             snedRpc
           }


         } else {
           if ( nextIndex >= 2) {
             nextIndex=nextIndex-1
             snedRpc
           }
         }

         timer.resetTimeout(cv.electionTimeout*0.5 millisecond)

       }
     }

     case ReceiveTimeout => {
       snedRpc
     }
   }
}

object LeaderSubActor {
  case class StartLeaderSubActor(lastLogIndex:Long)
  case class StopLeaderSubActor()
}



object RaftMemberLeader {
  case class NewLastLogIndex(lastLogIndex:Long)
  case class AppendOkNoti(memberId:RaftMemberId, lastAppendedIndex:Long)
  case class LastCommitedIndex(lastCommitedIndex:Long)
}

object RaftRPC {
  case class RPCTo(uid:Long,to:RaftMemberId)
  case class RPCFrom(uid:Long,from:RaftMemberId)

  case class RequestVoteRPC(to:RPCTo ,    term:Long,candidateId :RaftMemberId ,lastLogIndex :Long  ,lastLogTerm:Long  )
  case class RequestVoteRPCResult(from:RPCFrom,   term:Long, voteGranted:Boolean)

  case class AppendEntriesRPC(to:RPCTo,   term:Long , leaderId:RaftMemberId,prevLogIndex:Long, prevLogTerm:Long , entries: List[LogEntry] ,commitIndex:Long )
  case class AppendEntriesRPCResult(from:RPCFrom,   term:Long, success:Boolean)

}
