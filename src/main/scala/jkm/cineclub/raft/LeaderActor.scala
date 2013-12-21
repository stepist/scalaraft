package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/21/13
 * Time: 12:20 AM
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
import LeaderActor._

class LeaderActor extends Actor{

  val electionTimeout= 500 milliseconds

  context.setReceiveTimeout(electionTimeout)

  val logEntryDB:LogEntryDB=null
  val persistentStateDB:PersistentStateDB=null

  def resetTimeout=context.setReceiveTimeout(electionTimeout)


  var nextIndex:Long = 0
  var lastIndexMap : Map[RaftMemberId,Long]=null
  var lastCommittedIndex:Long = 0

  def startUp {
    val lastLogEntry=logEntryDB.getLast().get
    nextIndex= lastLogEntry.index+1

    lastIndexMap=Map()
    lastCommittedIndex=lastLogEntry.index

  }

  def calcLastCommittedIndex(lastIndexMap : Map[RaftMemberId,Long]) :Long ={
    0
  }

  def applyToStateMachine(logEntry:LogEntry):Boolean={
    true

  }

  def receive = {
    case ClientCommand(id,command) => {
      val logEntry= LogEntry(lastIndex,currentTerm,command)
      logEntryDB.appendEntry(logEntry)
      for ( child <- context.children) child ! Notification(lastIndex)
      bTimeout=waitingForCommit

      if (bTimeout) {
        sender ! ClientCommandResult(id,"timeout")
      }else {
        val ret=applyToStateMachine(logEntry)
        sender ! ClientCommandResult(id,ret)
      }


    }
    case  commands:List[ClientCommand] => {
      val logEntries =  commands.map{case ClientCommand(id,command) => LogEntry(lastIndex,currentTerm,command) }

    }

    case AppendOK(memberId,index) => {
      lastIndexMap = lastIndexMap + (memberId -> index )
      val newLastCommittedIndex = calcLastCommittedIndex(lastIndexMap)

      if ( newLastCommittedIndex > lastCommittedIndex) {
        for( index <-  (lastCommittedIndex+1) to newLastCommittedIndex ) {
          val logEntry = logEntryDB.getEntry(index).get
          applyToStateMachine(logEntry)
        }

        lastCommittedIndex= newLastCommittedIndex
      }

    }
  }


}




object LeaderActor {
  case class ClientCommand(id:Long,command:String)
  case class ClientCommandResult(id:Long,ret:String)
  case class AppendOK(memberId:RaftMemberId,index:Long)
  case class Notification(lastIndex:Long)
}



class LeaderSubActor extends Actor {

  val targetMember:RaftMemberId=null
  val electionTimeout= 500 milliseconds

  context.setReceiveTimeout(electionTimeout)

  val logEntryDB:LogEntryDB=null
  val persistentStateDB:PersistentStateDB=null

  def resetTimeout=context.setReceiveTimeout(electionTimeout)


  var nextIndex:Long = 0

  def stepDown

  def receive = {
    case Notification(lastIndex) => {

    }
    case AppendEntriesRPCResult(term, success) =>  {

      if (term>currentTerm)  stepDown
      else {
        if (success ) {
            context.parent ! AppendOK(targetMember,nextIndex-1)
        } else {
          context.actorSelection(addressTable(targetMember)) !  AppendEntriesRPC(currentTerm , myId,prevLogIndex, prevLogTerm , entries: List[LogEntry] ,commitIndex )

        }
      }
    }
  }
}   */