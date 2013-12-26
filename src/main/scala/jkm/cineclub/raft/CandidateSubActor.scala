package jkm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/26/13
 * Time: 7:15 PM
 * To change this template use File | Settings | File Templates.
 */



import akka.actor.{ReceiveTimeout, ActorLogging, Actor, IndirectActorProducer}
import jkm.cineclub.raft.PersistentState._
import scala.Some
import scala.concurrent.duration._

class CandidateSubActorDependencyInjector(memberId:RaftMemberId,cv:CurrentValues) extends IndirectActorProducer {
  override def actorClass = classOf[Actor]
  override def produce = {
    new CandidateSubActor(memberId,cv)
  }
}

class CandidateSubActor(val memberId:RaftMemberId,implicit val cv:CurrentValues) extends Actor with ActorLogging  {
  import RaftRPC._
  import CandidateSubActor._

  val debugHeader=s"${cv.myId} : CandidateSubActor : ${memberId} : "
  type RPCUid = Long

  case class SentedRPC(uid:RPCUid,sentedTime:Long)
  val timer=new Timer()
  var sentedRPC:Option[SentedRPC]=None
  var uid = util.Random.nextLong
  val timeoutValRatio=0.3

  var lastLogIndex:Long=0
  var lastLogTerm:Long=0




  def init{
    log.info("init")
    println("CandidateSubActor init "+memberId)
    timer.close
    sentedRPC=None

    lastLogIndex=0
    lastLogTerm=0
  }

  def resetTimeout {
    timer.resetTimeout(cv.electionTimeout*timeoutValRatio millisecond)
  }

  def recvRpc{
    sentedRPC=None
  }

  def snedRpc{
    if (sentedRPC.isEmpty | timer.isTimeouted ) {
      uid=uid+7

      val target=  context.actorSelection(cv.addressTable(memberId))
      val rpc = RequestVoteRPC(RPCTo(uid,memberId),  cv.currentTerm,cv.myId ,lastLogIndex   ,lastLogTerm  )

      target ! rpc
      sentedRPC=Some(SentedRPC(uid,System.currentTimeMillis))

      resetTimeout
    }
  }


  def initalState:Receive = {
    case StartCandidateSubActor(newLastLogIndex,newLastLogTerm) =>{
      init

      lastLogIndex=newLastLogIndex
      lastLogTerm=newLastLogTerm

      snedRpc
      context.become(handler)
      sender ! "ok"
    }
    case StopCandidateSubActor =>{
      init
      sender ! "ok"
    }
  }

  def handler:Receive={
    case StopCandidateSubActor =>{
      init
      context.become(initalState)
      sender ! "ok"
    }


    case RequestVoteRPCResult(RPCFrom(uid,from),   term, voteGranted)  if  memberId==from  => {
      if (sentedRPC.isDefined & sentedRPC.get.uid == uid) {
        recvRpc
        context.parent ! VoteResult(memberId,voteGranted )
        init
        context.become(initalState)
      }
    }

    case ReceiveTimeout => {
      snedRpc
    }
  }

  override def preStart(): Unit={
    init
  }

  def receive = initalState

}

object CandidateSubActor {
  case class VoteResult(memberId:RaftMemberId,voteGranted:Boolean)
  case class StartCandidateSubActor(lastLogIndex:Long,lastLogTerm:Long)
  case class StopCandidateSubActor()
}