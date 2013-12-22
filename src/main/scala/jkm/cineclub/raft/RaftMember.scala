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


class RaftMember(val logEntryDB:LogEntryDB ,val persistentStateDB:PersistentStateDB,val cv:CurrentValues,val stateMachine:StateMachine)  extends Actor {

  import RaftRPC._

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

  var requestedTime:Long = 1

  def resetTimeout=context.setReceiveTimeout(cv.electionTimeout millisecond)

  def resetTimeout(timeout:FiniteDuration)=context.setReceiveTimeout(timeout)

  def receive = followerBehavior

  def isLocalLogMoreCompleteThanCandidate(cLastLogIndex:Long,cLastLogTerm:Long):Boolean={
      val lastLogEntrySome= logEntryDB.getLast()
    if (lastLogEntrySome.isEmpty) return true  // when there is no logEntry in LogEntryDB ,   when server is launced for the first time.

    val LogEntry(vLastLogIndex,vLastLogTerm,_)=lastLogEntrySome.get

    (vLastLogTerm > cLastLogTerm) | (vLastLogTerm== cLastLogTerm)  & (vLastLogIndex > cLastLogIndex)
  }



  def rpcHandlerBehavior :Receive = {
    case RequestVoteRPC(uid,to, term,candidateId ,lastLogIndex  ,lastLogTerm ) => {

      val isValidRPCReq = term > 0 & cv.raftMembership.contains(candidateId) & lastLogIndex >0 &  lastLogTerm>0

      if (isValidRPCReq)
      term match {
        case a if a < cv.currentTerm => sender ! RequestVoteRPCResult(uid,cv.myId,   cv.currentTerm,false)
        case _ => {

          if ( term > cv.currentTerm ) setCurrentTerm(term)

          val isCandidateProper:Boolean= (cv.votedFor==null | cv.votedFor==candidateId) &
            !isLocalLogMoreCompleteThanCandidate(lastLogIndex,lastLogTerm)

          if ( isCandidateProper ) {
            setVotedFor(candidateId)
            sender ! RequestVoteRPCResult(uid,cv.myId, cv.currentTerm,true)
            resetTimeout
          }else{
            sender ! RequestVoteRPCResult(uid,cv.myId, cv.currentTerm,false)
          }

        }
      }


    }
    case AppendEntriesRPC(uid,to, term , leaderId,prevLogIndex, prevLogTerm , entries ,commitIndex ) => {

      val isValidRPCReq = term > 0 & cv.raftMembership.contains(leaderId) & prevLogIndex >0 &  prevLogTerm>0 & commitIndex>0 & entries!=null

      if (isValidRPCReq)
      term match {
        case a if a < cv.currentTerm => sender ! RequestVoteRPCResult(uid,cv.myId,    cv.currentTerm,false)
        case _ => {

          if ( term > cv.currentTerm ) setCurrentTerm(term)

          resetTimeout

          val logEntrySome=logEntryDB.getEntry(prevLogIndex)

          if (logEntrySome.isEmpty | logEntrySome.get.term != prevLogTerm) {

            sender ! AppendEntriesRPCResult(uid,cv.myId,   cv.currentTerm,false)

          } else {

            if ( entries.size ==0 ) {
              sender ! AppendEntriesRPCResult(uid,cv.myId,   cv.currentTerm,true)
            }
            else {

              if (entries.head != logEntrySome.get) logEntryDB.deleteFrom(prevLogIndex+1)
              logEntryDB.appendEntries(entries)
              sender ! AppendEntriesRPCResult(uid,cv.myId,   cv.currentTerm,true)

              stateMachine.applyEntries(entries)

            }
          }

        }
      }
    }
  }


  var requestVoteRPCUidTable:Map[RaftMemberId,Long]=null
  var requestVoteRPCUID:Long = util.Random.nextLong()
  var voteList:List[RaftMemberId]=null

  def becomeCandidate() {
    cv.memberState=MemberState.Candidate
    context.become(candidateBehavior)

    setCurrentTerm(cv.currentTerm+1)
    setVotedFor(cv.myId)

    import util.Random
    resetTimeout(cv.electionTimeout*(1.0+Random.nextFloat)  millis)

    val lastLogEntrySome= logEntryDB.getLast()
    //  lastLogEntrySome is None  // when there is no logEntry in LogEntryDB ,   when server is launced for the first time.
    val lastLogIndex = if (lastLogEntrySome.isEmpty) 0 else lastLogEntrySome.get.index
    val lastLogTerm = if (lastLogEntrySome.isEmpty) 0 else lastLogEntrySome.get.term

    requestVoteRPCUidTable= Map[RaftMemberId,Long]()
    voteList=List[RaftMemberId]()


    for ( memberId <- cv.raftMembership.members ) {
      if (memberId != cv.myId)  {

        requestVoteRPCUID += 3
        requestVoteRPCUidTable = requestVoteRPCUidTable + (memberId -> requestVoteRPCUID)

        context.actorSelection(cv.addressTable(memberId)) ! RequestVoteRPC(requestVoteRPCUID,memberId,  cv.currentTerm,cv.myId ,lastLogIndex   ,lastLogTerm  )
      }
    }
  }

  def followerBehavior :Receive = rpcHandlerBehavior orElse {
    case ReceiveTimeout => {
      becomeCandidate
    }
  }

  def stepDown={
    cv.memberState=MemberState.Follower

    if (cv.memberState==MemberState.Leader ) {
      if ( nowInOperation ){
        cmdHandleActor ! StepDown
        nowInOperation=false
        cmdHandleActor=null
      }
      //kill LeaderSubActor
      //clear state
    }

    if (cv.memberState==MemberState.Candidate) {

    }


    //context.setReceiveTimeout(Duration.Undefined)
    context.become(followerBehavior)
    resetTimeout
  }


  def candidateBehavior :Receive = rpcHandlerBehavior orElse {
    case ReceiveTimeout => {
      becomeCandidate
    }
    case RequestVoteRPCResult(uid,from,     term, voteGranted) => {
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

  import RaftMemberLeader._

  var leaderSubActorTable:Map[RaftMemberId,ActorRef]= null
  var lastAppendedIndexTable:Map[RaftMemberId,Long]=null
  var clientCmdHandlerActor:ActorRef=null

  def becomeLeader={

    nowInOperation=false
    lastIndex=0
    cmdHandleActor=null
    isAtLeastOneEntryOfThisTermCommited=false
    newLogEntry=null


    cv.memberState=MemberState.Leader
    context.setReceiveTimeout(Duration.Undefined)
    context.become(leaderBehavior)

    val lastEntrySome=logEntryDB.getLast


    val nextIndex=if (lastEntrySome.isEmpty) 0 else lastEntrySome.get.index+1

    leaderSubActorTable=Map[RaftMemberId,ActorRef]()
    lastAppendedIndexTable = Map[RaftMemberId,Long]()

    for ( memberId <- cv.raftMembership.members ) {
      if (memberId != cv.myId)  {
        leaderSubActorTable = leaderSubActorTable + (memberId -> createLeaderSubActor(memberId,nextIndex))
      }
    }

    for (leaderSubActor <- leaderSubActorTable.values)  leaderSubActor !   NewLastLogIndex(nextIndex)

    clientCmdHandlerActor=createClientCmdHandlerActor

  }

  def createClientCmdHandlerActor:ActorRef={null}
  def createLeaderSubActor(memberId:RaftMemberId,nextIndex:Long):ActorRef = {

    context.actorOf(Props[LeaderSubActor], "leader_sub_"+memberId)


  }

  def getLastCommitedIndex(lastAppendedIndexTable:Map[RaftMemberId,Long]):Long={
      1
  }

  def leaderBehavior :Receive = rpcHandlerBehavior orElse {


    case AppendEntriesRPCResult(uid,from,      term, success)  => {
      if ( cv.raftMembership.contains(from)  & term >= cv.currentTerm) {
        if (term>cv.currentTerm) {
          setCurrentTerm(term)
          stepDown
        } else {
          leaderSubActorTable(from) ! AppendEntriesRPCResult(uid,from,term,success)
        }
      }
    }

    case NewLastLogIndex(lastLogIndex) => {
      for ( leaderSubActor <- leaderSubActorTable.values) {
        leaderSubActor ! NewLastLogIndex(lastLogIndex)
      }
    }

    case AppendOkNoti(memberId, lastAppendedIndex) => {
      lastAppendedIndexTable = lastAppendedIndexTable + (memberId -> lastAppendedIndex)


      val lastCommitedIndex=getLastCommitedIndex(lastAppendedIndexTable)
      clientCmdHandlerActor ! LastCommitedIndex(lastCommitedIndex)
    }








    case ClientCommand(uid,command) =>  {
      if (nowInOperation) {
        sender ! Busy
      } else {

        nowInOperation=true
        cmdHandleActor=sender
        lastIndex=logEntryDB.getLast().get.index + 1
        newLogEntry =LogEntry(lastIndex,cv.currentTerm,command)
        logEntryDB.appendEntry(newLogEntry)

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

  }
  var nowInOperation=false
  var lastIndex:Long=0

  var cmdHandleActor:ActorRef =null
  var isAtLeastOneEntryOfThisTermCommited=false
  var newLogEntry:LogEntry=null



}

import RaftMemberLeader._

class LeaderSubActor(val memberId:RaftMemberId,val logEntryDB:LogEntryDB,val cv:CurrentValues) extends Actor {

  var nextIndex:Long= -1
  var preUid:Long= -1
  var commitedIndex:Long= -1

   def receive={
     case NewLastLogIndex(lastLogIndex)  if lastLogIndex > nextIndex  =>  {



       nextIndex=lastLogIndex
        val prevLogEntry = logEntryDB.getLastNFrom(2,nextIndex)(1).get
       val prevLogIndex = prevLogEntry.index
       val prevLogTerm = prevLogEntry.term


        context.actorSelection(cv.addressTable(memberId)) !  AppendEntriesRPC(uid,memberId,   cv.currentTerm ,cv.myId,prevLogIndex:Long, prevLogTerm:Long , entries: List[LogEntry] ,cv.commitedIndex )
        preUid=uid
       context.setReceiveTimeout(2000 millisecond)

     }

     case AppendEntriesRPCResult(uid,from,    term, success) => {
       if ( memberId==from & uid == preUid ) {
         preUid= -1

         if ( success ) {
           context.parent ! AppendOkNoti(memberId,nextIndex )
         } else {
           self ! NewLastLogIndex(nextIndex-1)
         }

       }
     }

     case ReceiveTimeout => {

     }
   }
}




object RaftMemberLeader {
  case class NewLastLogIndex(lastLogIndex:Long)
  case class AppendOkNoti(memberId:RaftMemberId, lastAppendedIndex:Long)
  case class LastCommitedIndex(lastCommitedIndex:Long)





}

object RaftRPC {
  case class RequestVoteRPC(uid:Long,to:RaftMemberId ,   term:Long,candidateId :RaftMemberId ,lastLogIndex :Long  ,lastLogTerm:Long  )
  case class RequestVoteRPCResult(uid:Long,from:RaftMemberId,  term:Long, voteGranted:Boolean)

  case class AppendEntriesRPC(uid:Long,to:RaftMemberId,   term:Long , leaderId:RaftMemberId,prevLogIndex:Long, prevLogTerm:Long , entries: List[LogEntry] ,commitIndex:Long )
  case class AppendEntriesRPCResult(uid:Long,from:RaftMemberId, term:Long, success:Boolean)

}
