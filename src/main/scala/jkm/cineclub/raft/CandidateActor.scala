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
import RPCHandler._
import CurrentState._

class CandidateActor extends Actor{
  val electionTimeout= 500 milliseconds

  context.setReceiveTimeout(electionTimeout)

  val logEntryDB:LogEntryDB=null
  val persistentStateDB:PersistentStateDB=null

  def resetTimeout=context.setReceiveTimeout(electionTimeout)

  startup

  var aquiredVotes:Set[ActorRef]=null

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
         if (term>currentState) stepDown
         else {
           val a=sender
           if (voteGranted) aquiredVotes=aquiredVotes + a

           if (aquiredVotes.size > memberShip.newMembers.size/2 )  becomeLeader

         }

     }
     case ReceiveTimeout =>  {
       if  ( currentState == MemberState.Candidate) startup
     }
   }
}
            */