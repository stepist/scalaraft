package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/20/13
 * Time: 10:04 PM
 * To change this template use File | Settings | File Templates.
 */

import PersistentState._
import jkm.cineclub.raft.DB.{PersistentStateDB, LogEntryDB}
import LogEntryDB._
import akka.actor._
import scala.concurrent.duration._
import RPCHandler._
import CurrentValues._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

class RPCHandler extends Actor{


  val logEntryDB:LogEntryDB=null
  val persistentStateDB:PersistentStateDB=null

  val electionTimeout= 500 milliseconds

  context.setReceiveTimeout(electionTimeout)

  def resetTimeout=context.setReceiveTimeout(electionTimeout)
  def stepDown= {
    currentState=MemberState.Follower
  }

  def convertToCandidate  ={

  }

  def compareToLocalLog(lastLogIndex:Long  ,lastLogTerm:Long):Boolean

  val raftMemeberStateManager:ActorRef=null
  val currentValues:CurrentValues=null
  import currentValues._
  import RaftMemeberStateManager._

  def receive = {


    case ResetTimeout => resetTimeout


    case RequestVoteRPC(term,candidateId ,lastLogIndex  ,lastLogTerm ) => {
      if (term< currentTerm) {sender ! RequestVoteRPCResult(currentTerm,false)}
      else {


        if (term> currentTerm ) {
          persistentStateDB.putState(TermInfoDBKey,TermInfo(term,votedFor))
          currentTerm=term
        }

        if ( currentState == MemberState.Candidate | currentState == MemberState.Leader) stepDown


        //From now, It's in follower State

        if ( (votedFor==null | votedFor==candidateId) & compareToLocalLog(lastLogIndex  ,lastLogTerm ) ) {

          persistentStateDB.putState(TermInfoDBKey,TermInfo(currentTerm,candidateId))
          votedFor=candidateId

          sender ! RequestVoteRPCResult(currentTerm,true)
          resetTimeout

        }else{
          sender ! RequestVoteRPCResult(currentTerm,false)
        }

      }

      term match {
        case a if a<currentTerm => sender ! RequestVoteRPCResult(currentTerm,false)
        case _ => {
          implicit val timeout = Timeout(5 seconds)

          val future = raftMemeberStateManager ? CheckTerm(term)
          val result=Await.result(future, timeout.duration).asInstanceOf[CheckTermResult]

          if ( memberState == MemberState.Candidate | memberState == MemberState.Leader) {
            val future = raftMemeberStateManager ? StepDown()
            val result=Await.result(future, timeout.duration).asInstanceOf[StepDownResult]
          }


          //From now, It's in follower State

          if ( (votedFor==null | votedFor==candidateId) & compareToLocalLog(lastLogIndex  ,lastLogTerm ) ) {

            persistentStateDB.putState(TermInfoDBKey,TermInfo(currentTerm,candidateId))
            votedFor=candidateId

            sender ! RequestVoteRPCResult(currentTerm,true)
            resetTimeout

          }else{
            sender ! RequestVoteRPCResult(currentTerm,false)
          }

        }
      }


    }
    case AppendEntriesRPC(term , leaderId,prevLogIndex, prevLogTerm , entries ,commitIndex ) => {

      if (term< currentTerm) {sender ! AppendEntriesRPCResult(currentTerm,false)}
      else{
        if (term> currentTerm ) {
          persistentStateDB.putState(TermInfoDBKey,TermInfo(term,votedFor))
          currentTerm=term
        }
        if ( currentState == MemberState.Candidate | currentState == MemberState.Leader) stepDown

        resetTimeout

        val logEntrySome=logEntryDB.getEntry(prevLogIndex)
        if (logEntrySome.isEmpty | logEntrySome.get.term != prevLogTerm) {sender ! AppendEntriesRPCResult(currentTerm,false)}
        else {
          if ( entries.size ==0 ) {sender ! AppendEntriesRPCResult(currentTerm,true)}
          else {

            if (entries.head != logEntrySome.get) logEntryDB.deleteFrom(prevLogIndex+1)
            logEntryDB.appendEntries(entries)
            sender ! AppendEntriesRPCResult(currentTerm,true)

            //Apply newly committed entries to state machine (§5.3)

          }
        }

      }

      term match {
        case a if a < currentTerm => sender ! RequestVoteRPCResult(currentTerm,false)
        case a if a>=0  => {
          implicit val timeout = Timeout(5 seconds)

          val future = raftMemeberStateManager ? CheckTerm(term)
          val result=Await.result(future, timeout.duration).asInstanceOf[CheckTermResult]

          if ( memberState == MemberState.Candidate | memberState == MemberState.Leader) {
            val future = raftMemeberStateManager ? StepDown()
            val result=Await.result(future, timeout.duration).asInstanceOf[StepDownResult]
          }
      }




    }
    case ReceiveTimeout =>  {
      if  ( currentState == MemberState.Follower) convertToCandidate
    }
  }

}

object RPCHandler {
  case class RequestVoteRPC(term:Long,candidateId :RaftMemberId ,lastLogIndex :Long  ,lastLogTerm:Long  )
  case class RequestVoteRPCResult(term:Long, voteGranted:Boolean)

  case class  AppendEntriesRPC(term:Long , leaderId:RaftMemberId,prevLogIndex:Long, prevLogTerm:Long , entries: List[LogEntry] ,commitIndex:Long )
  case class  AppendEntriesRPCResult(term:Long, success:Boolean)

  case class ResetTimeout()

}


class RaftMemeberStateManager extends Actor {
  import RaftMemeberStateManager._

  val persistentStateDB:PersistentStateDB=null
  val currentValues:CurrentValues=null
  import currentValues._

  def stepDown

  def receive = {
    case CheckTerm(term) => {
      var update=false
      if (term> currentTerm ) {
        persistentStateDB.putState(TermInfoDBKey,TermInfo(term,votedFor))
        currentTerm=term
        update=true
      }
      sender ! CheckTermResult(update)
    }

    case StepDown => {

    }
  }
}

object RaftMemeberStateManager {
  case class CheckTerm(term:Long)
  case class CheckTermResult(updated:Boolean)

  case class StepDown()
  case class StepDownResult()


}