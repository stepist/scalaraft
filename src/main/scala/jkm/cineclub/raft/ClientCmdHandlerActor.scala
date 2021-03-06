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
import jkm.cineclub.raft.CurrentValues.MemberState
import scala.Predef._
import jkm.cineclub.raft.StateMachine.Command
import jkm.cineclub.raft.RaftConfig.TcpAddress


class ClientCmdHandlerActorDependencyInjector(val raftCtx:RaftContext,val raftMemberActor:ActorRef) extends IndirectActorProducer {
  override def actorClass = classOf[Actor]  //RaftMember?
  override def produce = {
    new ClientCmdHandlerActor(raftCtx,raftMemberActor)
  }
}

class ClientCmdHandlerActor(val raftCtx:RaftContext,val raftMemberActor:ActorRef) extends Actor{

  val logEntryDB:LogEntryDB = raftCtx.logEntryDB
  val cv:CurrentValues = raftCtx.cv

  import ClientCmdHandlerActor._

  val raftMemberActorSelection = context.actorSelection(cv.addressTable(cv.myId))


  def getLastIndex = {
    logEntryDB.getLast()
  }

  def receive ={

    case ClientCommand(uid,command) => {
      println(ClientCommand(uid,command))

      cv.memberState match {
        case MemberState.Leader => {

          implicit val timeout = Timeout(20 seconds)

          val future = raftMemberActorSelection ? ClientCommand(uid,command)
          val clientCommandResult = Await.result(future,20 seconds).asInstanceOf[ClientCommandResult]

          // when timeout ,
          // catch  Exception

          clientCommandResult.code match {
            case "stepdown"  => sender ! ClientCommandResult(clientCommandResult.uid,null,"chaos")
            case "Busy" =>  sender ! ClientCommandResult(clientCommandResult.uid,null,"busy")
            case "ok" => sender ! ClientCommandResult(clientCommandResult.uid,clientCommandResult.ret,"ok")
          }

        }
        case MemberState.Follower => {
          sender ! ClientCommandResult(uid,cv.leaderId,"leaderId")
          println(ClientCommandResult(uid,cv.leaderId,"leaderId"))
        }
        case MemberState.Candidate => {
          sender ! ClientCommandResult(uid,null,"chaos")
          println(ClientCommandResult(uid,null,"chaos"))
        }
      }

    }
  }
}

class ClientAdminCmdHandlerActor extends Actor {    // it can be a routee..
  import  ClientCmdHandlerActor._

  def receive = {
    case ClientCommandGetMembershipInfo(raftClutserId) =>
  }
}


object ClientCmdHandlerActor {
  import StateMachine._

  case class ClientCommandGetMembershipInfo(raftClutserId:RaftClusterId)
  case class ClientCommandGetMembershipInfoResult(serviceAdresses:Map[RaftMemberId,TcpAddress])
  case class ClientCommand(uid:Long,command:String )
  case class ClientCommandResult(uid:Long,ret:String ,code:String)

  //  uid 0~20 are predefined
  //  uid 1 :  Membership change ..
  // etc
}
