package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/26/13
 * Time: 7:13 PM
 * To change this template use File | Settings | File Templates.
 */
import akka.actor.{ReceiveTimeout, ActorLogging, Actor, IndirectActorProducer}
import jkm.cineclub.raft.PersistentState._
import scala.Some
import jkm.cineclub.raft.DB.LogEntryDB
import jkm.cineclub.raft.DB.LogEntryDB.LogEntry
import scala.concurrent.duration._

class LeaderSubActorDependencyInjector(memberId:RaftMemberId,logEntryDB:LogEntryDB,cv:CurrentValues) extends IndirectActorProducer {
  override def actorClass = classOf[Actor]
  override def produce = {
    new LeaderSubActor(memberId,logEntryDB,cv)
  }
}

class LeaderSubActor(val memberId:RaftMemberId,implicit val logEntryDB:LogEntryDB,implicit val cv:CurrentValues) extends Actor with ActorLogging  {
  import RaftMemberLeader._
  import RaftRPC._
  import LeaderSubActor._

  val debugHeader=s"${cv.myId} : LeaderSubActor : ${memberId} : "
  type RPCUid = Long

  case class SentedRPC(uid:RPCUid,sentedTime:Long)
  val timer=new Timer()
  var nextIndex:Long= 0
  var sentedRPC:Option[SentedRPC]=None
  var uid = util.Random.nextLong
  var lastLogIndex:Long=0
  val timeoutValRatio=0.3




  def init{
    log.info("init")
    println("LeaderSubActor init "+memberId)
    timer.close
    nextIndex= 0
    sentedRPC=None
    lastLogIndex=0
  }

  def resetTimeout {
    timer.resetTimeout(cv.electionTimeout*timeoutValRatio millisecond)
  }

  def recvRpc{
    sentedRPC=None
  }

  def rpc(prevLogIndex:Long, prevLogTerm:Long , entries:List[LogEntry] ,commitIndex :Long ) {
    uid=uid+7

    val target=  context.actorSelection(cv.addressTable(memberId))
    val rpc =  AppendEntriesRPC( RPCTo(uid,memberId),
      cv.currentTerm ,cv.myId,prevLogIndex, prevLogTerm , entries ,commitIndex )

    target ! rpc
    sentedRPC=Some(SentedRPC(uid,System.currentTimeMillis()))

    resetTimeout
  }

  def snedRpc{
    if (sentedRPC.isEmpty | timer.isTimeouted ) {
      val prevLogEntry = logEntryDB.getEntry(nextIndex-1).get
      val prevLogIndex = prevLogEntry.index
      val prevLogTerm = prevLogEntry.term

      val entry = logEntryDB.getEntry(nextIndex).getOrElse(null)
      val entries = if (entry==null) Nil else List(entry)

      rpc(prevLogIndex,prevLogTerm,entries,cv.commitIndex)
    }
  }


  def initalState:Receive = {
    case StartLeaderSubActor(newlastLogIndex) =>{
      init
      lastLogIndex=newlastLogIndex
      nextIndex=lastLogIndex+1
      snedRpc
      context.become(handler)
      sender ! "ok"
    }
  }

  def handler:Receive={
    case StopLeaderSubActor =>{
      init
      context.become(initalState)
      sender ! "ok"
    }

    case NewLastLogIndex(newlastLogIndex)  if newlastLogIndex >= nextIndex  =>  {
      lastLogIndex=newlastLogIndex
      snedRpc
    }

    case AppendEntriesRPCResult(RPCFrom(uid,from),    term, success)  if  memberId==from  => {
      if (sentedRPC.isDefined & sentedRPC.get.uid == uid) {
        recvRpc

        success match {
          case true => {
            context.parent ! AppendOkNoti(memberId,nextIndex )
            if ( nextIndex < (lastLogIndex+1) ) {
              nextIndex = nextIndex+1
              snedRpc
            }
          }
          case false => {
            if ( nextIndex >= 2) {
              nextIndex=nextIndex-1
              snedRpc
            }
          }
        }
        resetTimeout
      }
    }

    case ReceiveTimeout => {
      snedRpc
    }
  }

  def receive = initalState

  override def preStart(): Unit={
    init
  }

}

object LeaderSubActor {
  case class StartLeaderSubActor(lastLogIndex:Long)
  case class StopLeaderSubActor()
}