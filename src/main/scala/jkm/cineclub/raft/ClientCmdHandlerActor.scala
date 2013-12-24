package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/22/13
 * Time: 10:17 PM
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


class ClientCmdHandlerActor(val logEntryDB:LogEntryDB ,val cv:CurrentValues) extends Actor{

  import ClientCmdHandlerActor._


  def getLastIndex = {
    logEntryDB.getLast()
  }

  def receive ={
    case ClientCommand(uid,command) => {

      cv.memberState match {
        case MemberState.Leader => {

          val future = AppendNewEntryActor ? NewEntry
          val result= Await.. (future)

          if ( result !=Stepdown) {
            ret=result.ret
            sender ! ClientCommandResult(uid,ret,"ok")
          } else{
            sender ! ClientCommandResult(uid,"i dont know","i dont know")
          }


        }
        case MemberState.Follower => {
          sender ! ClientCommandResult(uid,cv.leaderId,"leaderId")
        }
        case MemberState.Candidate => {
          sender ! ClientCommandResult(uid,"i dont know","i dont know")
        }
      }

    }
  }
}



object ClientCmdHandlerActor {
  case class ClientCommand(uid:Long,command:String)
  case class ClientCommandResult(uid:Long,ret:String,code:String)
}
