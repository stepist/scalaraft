package jkm.cineclub.raft


/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/20/13
 * Time: 11:52 PM
 * To change this template use File | Settings | File Templates.
 */
 /*

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

class CandidateActor(val logEntryDB:LogEntryDB ,val persistentStateDB:PersistentStateDB, val cv:CurrentValues) extends Actor{

  def resetTimeout=context.setReceiveTimeout(cv.electionTimeout milli)

  val raftMemeberStateManager:ActorSelection=context.actorSelection("MemeberStateManager")

  import RaftMemeberStateManager._

  val logEntryDB:LogEntryDB=null
  val persistentStateDB:PersistentStateDB=null

  def resetTimeout=context.setReceiveTimeout(electionTimeout)

  startup

  var aquiredVotes:Set[ActorRef]=null

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



  def startup={
    aquiredVotes=aquiredVotes + self
      currentTerm +=1
      votedFor=myId
      resetTimeout

    val lastLogEntry= logEntryDB.getLast().get

    if (memberShip.configType==RaftMembership.RaftMembershipConfigNormal ) {
        for ( memberId <- memberShip.newMembers) {
          context.actorSelection(addressTable(memberId)) ! RequestVoteRPC(currentTerm,myId ,lastLogEntry.index ,lastLogEntry.term )
        }
    }
  }

   def receive = {
     case  RequestVoteRPCResult(term, voteGranted) => {
       checkTerm(term)


           val a=sender
           if (voteGranted) aquiredVotes=aquiredVotes + a

           if (aquiredVotes.size > memberShip.newMembers.size/2 )  becomeLeader



     }
     case ReceiveTimeout =>  {
       if  ( currentState == MemberState.Candidate) startup
     }
   }
}
                  */