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
import scala.concurrent.Await
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import scala.concurrent.duration._
import RPCHandler._
import jkm.cineclub.raft.CurrentValues.MemberState

object RPCHandler {
  case class RequestVoteRPC(term:Long,candidateId :RaftMemberId ,lastLogIndex :Long  ,lastLogTerm:Long  )
  case class RequestVoteRPCResult(term:Long, voteGranted:Boolean)

  case class  AppendEntriesRPC(term:Long , leaderId:RaftMemberId,prevLogIndex:Long, prevLogTerm:Long , entries: List[LogEntry] ,commitIndex:Long )
  case class  AppendEntriesRPCResult(term:Long, success:Boolean)

  case class ResetTimeout()

}

class RPCHandler(val logEntryDB:LogEntryDB ,val persistentStateDB:PersistentStateDB, val cv:CurrentValues) extends Actor{


  def resetTimeout=context.setReceiveTimeout(cv.electionTimeout millisecond)

  def compareToLocalLog(lastLogIndex:Long  ,lastLogTerm:Long):Boolean = {false}

  val raftMemeberStateManager:ActorSelection=context.actorSelection("MemeberStateManager")

  import RaftMemeberStateManager._


  def checkTerm(term:Long) {
    implicit val timeout = Timeout(5 seconds)
    val future = raftMemeberStateManager ? CheckTerm(term)
    try {
      val result=Await.result(future, timeout).asInstanceOf[CheckTermResult]
    } catch {
      case e:AskTimeoutException =>
    }
  }

  def stepDown {
    implicit val timeout = Timeout(5 seconds)
    val future = raftMemeberStateManager ? StepDown()
    try {
      val result=Await.result(future, timeout).asInstanceOf[StepDownResult]
    } catch {
      case e:AskTimeoutException =>
    }
  }

  def becomeCandidate {
    implicit val timeout = Timeout(5 seconds)
    val future = raftMemeberStateManager ? BecomeCandidate()
    try {
      val result=Await.result(future, timeout).asInstanceOf[BecomeCandidateResult]
    } catch {
      case e:AskTimeoutException =>
    }
  }


  def receive = {
    case ResetTimeout => resetTimeout

    case RequestVoteRPC(term,candidateId ,lastLogIndex  ,lastLogTerm ) => {
      term match {
        case a if a<cv.currentTerm => sender ! RequestVoteRPCResult(cv.currentTerm,false)
        case _ => {

          checkTerm(term)

          //From now, It's in follower State

          if ( (cv.votedFor==null | cv.votedFor==candidateId) & compareToLocalLog(lastLogIndex,lastLogTerm) ) {

            persistentStateDB.putState(TermInfoDBKey,TermInfo(cv.currentTerm,votedFor=candidateId))
            cv.votedFor=candidateId

            sender ! RequestVoteRPCResult(cv.currentTerm,true)
            resetTimeout   //?

          }else{
            sender ! RequestVoteRPCResult(cv.currentTerm,false)
          }

        }
      }


    }
    case AppendEntriesRPC(term , leaderId,prevLogIndex, prevLogTerm , entries ,commitIndex ) => {

      term match {
        case a if a < cv.currentTerm => sender ! RequestVoteRPCResult(cv.currentTerm,false)
        case a if a>=0  => {
          checkTerm(term)
          if ( cv.memberState == MemberState.Candidate | cv.memberState == MemberState.Leader) stepDown
          resetTimeout

          val logEntrySome=logEntryDB.getEntry(prevLogIndex)

          if (logEntrySome.isEmpty | logEntrySome.get.term != prevLogTerm) {

            sender ! AppendEntriesRPCResult(cv.currentTerm,false)

          } else {

            if ( entries.size ==0 ) {
              sender ! AppendEntriesRPCResult(cv.currentTerm,true)
            }
            else {

              if (entries.head != logEntrySome.get) logEntryDB.deleteFrom(prevLogIndex+1)
              logEntryDB.appendEntries(entries)
              sender ! AppendEntriesRPCResult(cv.currentTerm,true)

              //Apply newly committed entries to state machine (§5.3)

            }
          }

        }
      }




    }
    case ReceiveTimeout =>  {
      if  ( cv.memberState == MemberState.Follower) becomeCandidate
    }
  }

}




class RaftMemeberStateManager extends Actor {
  import RaftMemeberStateManager._

  val persistentStateDB:PersistentStateDB=null
  val currentValues:CurrentValues=null
  import currentValues._

  def stepDown ={}

  def becomeCandidate={}

  def becomeLeader={}

  def receive = {
    case CheckTerm(term) => {
      var update=false
      if (term> currentTerm ) {
        persistentStateDB.putState(TermInfoDBKey,TermInfo(term,votedFor))
        currentTerm=term
        update=true
        if ( memberState == MemberState.Candidate | memberState == MemberState.Leader) stepDown
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

  case class BecomeCandidate()
  case class BecomeCandidateResult()


}