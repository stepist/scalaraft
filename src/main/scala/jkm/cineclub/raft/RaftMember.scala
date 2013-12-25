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
import jkm.cineclub.raft.CurrentValues.MemberState
import jkm.cineclub.raft.ClientCmdHandlerActor.{ClientCommandResult, ClientCommand}

class Timer(implicit val context:ActorContext,implicit val cv:CurrentValues) {
  import java.util.Date

  val guard=1.1
  var requestedTime:Long = 0
  var timeout:Duration = Duration.Undefined

  def resetTimeout={
    timeout=cv.electionTimeout.millis
    requestedTime=new Date().getTime
    context.setReceiveTimeout(timeout*guard)
  }

  def resetTimeout(a:Duration = cv.electionTimeout.millis)={
    timeout=a
    requestedTime=new Date().getTime
    context.setReceiveTimeout(timeout*guard)
  }

  def isTimeout:Boolean ={
    val elapsedTime= new Date().getTime - requestedTime
    elapsedTime.millis  >= timeout
  }

  def ifTimeout(exec:() => Unit) {
    val elapsedTime= new Date().getTime - requestedTime

    if (elapsedTime.millis  >= timeout) exec()
    else resetTimeout(timeout - elapsedTime.millis)
  }

  def close= {
    requestedTime = 0
    timeout = Duration.Undefined
    context.setReceiveTimeout(Duration.Undefined)
  }
}


import akka.actor.IndirectActorProducer

class RaftMemberDependencyInjector(val raftCtx:RaftContext) extends IndirectActorProducer {
  override def actorClass = classOf[Actor]  //RaftMember?
  override def produce = {
    new RaftMember(raftCtx)
  }
}
class RaftMember(val raftCtx:RaftContext)  extends Actor with ActorLogging  {

  import RaftRPC._

  implicit val logEntryDB:LogEntryDB = raftCtx.logEntryDB
  implicit val persistentStateDB:PersistentStateDB = raftCtx.persistentStateDB
  implicit val cv:CurrentValues = raftCtx.cv
  implicit val stateMachine:StateMachine = raftCtx.stateMachine


  val timer=new Timer

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


  def receive = followerBehavior

  def isLocalLogMoreCompleteThanCandidate(cLastLogIndex:Long,cLastLogTerm:Long):Boolean={
      val lastLogEntrySome= logEntryDB.getLast()
    if (lastLogEntrySome.isEmpty) return true  // when there is no logEntry in LogEntryDB ,   when server is launced for the first time.

    val LogEntry(vLastLogIndex,vLastLogTerm,_)=lastLogEntrySome.get

    (vLastLogTerm > cLastLogTerm) | (vLastLogTerm== cLastLogTerm)  & (vLastLogIndex > cLastLogIndex)
  }



  def rpcHandlerBehavior :Receive = {
    case RequestVoteRPC(RPCTo(uid,to), term,candidateId ,lastLogIndex  ,lastLogTerm ) => {
      println("RequestVoteRPC Receive "+candidateId+"  ->  "+to +"     "+RequestVoteRPC(RPCTo(uid,to), term,candidateId ,lastLogIndex  ,lastLogTerm ))
      println("(to,cv.myId)  =" +(to,cv.myId) +" "+(to==cv.myId) )
      println("cv.raftMembership.contains(candidateId)="+cv.raftMembership.contains(candidateId))

      val isValidRPCReq = to==cv.myId & term > 0 & cv.raftMembership.contains(candidateId) & lastLogIndex >=0 &  lastLogTerm>=0
      println("isValidRPCReq="+isValidRPCReq)

      if (isValidRPCReq)
      term match {
        case a if a < cv.currentTerm => sender ! RequestVoteRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,false)
        case _ => {



          if ( term > cv.currentTerm ) {
            setCurrentTerm(term)
            stepDown //?
          }

          val isCandidateProper:Boolean= (cv.votedFor==null | cv.votedFor==candidateId) &
            !isLocalLogMoreCompleteThanCandidate(lastLogIndex,lastLogTerm)

          if ( isCandidateProper ) {
            setVotedFor(candidateId)
            sender ! RequestVoteRPCResult(RPCFrom(uid,cv.myId), cv.currentTerm,true)
            println(RequestVoteRPCResult(RPCFrom(uid,cv.myId), cv.currentTerm,true))
            timer.resetTimeout
          }else{
            sender ! RequestVoteRPCResult(RPCFrom(uid,cv.myId), cv.currentTerm,false)
            println(RequestVoteRPCResult(RPCFrom(uid,cv.myId), cv.currentTerm,false))
          }

        }
      }
    }
    case AppendEntriesRPC(RPCTo(uid,to), term , leaderId,prevLogIndex, prevLogTerm , entries ,commitIndex ) => {
      println("received "+AppendEntriesRPC(RPCTo(uid,to), term , leaderId,prevLogIndex, prevLogTerm , entries ,commitIndex ))
      val isValidRPCReq = to==cv.myId & term > 0 & cv.raftMembership.contains(leaderId) & prevLogIndex >=0 &  prevLogTerm>=0 & commitIndex>=0 & entries!=null

      if (isValidRPCReq)
      term match {
        case a if a < cv.currentTerm =>{
          sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),    cv.currentTerm,false)
          println("send    "+AppendEntriesRPCResult(RPCFrom(uid,cv.myId),    cv.currentTerm,false))

        }
        case _ => {

          if ( term > cv.currentTerm ) setCurrentTerm(term)

          stepDown //?

          timer.resetTimeout

          val logEntrySome=logEntryDB.getEntry(prevLogIndex)
          println("-------------")

          if (logEntrySome.isEmpty | logEntrySome.get.term != prevLogTerm) {

            sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,false)
            println("send    "+AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,false))

          } else {

            if ( entries.size ==0 ) {
              sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,true)
              println("send    "+AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,true))
            } else {

              if (entries.head != logEntrySome.get) logEntryDB.deleteFrom(prevLogIndex+1)
              logEntryDB.appendEntries(entries)
              sender ! AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,true)
              println("send    "+AppendEntriesRPCResult(RPCFrom(uid,cv.myId),   cv.currentTerm,true))



            }
          }

          cv.commitIndex=commitIndex
          val lastAppliedIndex=stateMachine.getLastAppliedIndex

          if ( lastAppliedIndex < cv.commitIndex )
          {
            println("-----------------------------")
              for( index <-  (lastAppliedIndex+1) to cv.commitIndex)  {
                val entry=logEntryDB.getEntry(index).get
                stateMachine.applyEntry(entry)  // Async?
              }
          }
        }
      }
    }
  }




  def followerBehavior :Receive = rpcHandlerBehavior orElse {
    case ReceiveTimeout => {
      timer.ifTimeout(becomeCandidate)
    }
  }














  var requestVoteRPCUidTable:Map[RaftMemberId,Long]=null
  var requestVoteRPCUID:Long = util.Random.nextLong()
  var voteList:List[RaftMemberId]=null

  def becomeCandidate() {
    log.info("goto Candidate")
    context.become(candidateBehavior)
    cv.memberState=MemberState.Candidate

    setCurrentTerm(cv.currentTerm+1)
    setVotedFor(cv.myId)

    import util.Random
    timer.resetTimeout(cv.electionTimeout*(1.0+Random.nextFloat*0.7)  millis)

    val lastLogEntrySome= logEntryDB.getLast()
    val lastLogIndex = lastLogEntrySome.get.index
    val lastLogTerm = lastLogEntrySome.get.term

    requestVoteRPCUidTable= Map[RaftMemberId,Long]()
    voteList=List[RaftMemberId]()


    for ( memberId <- cv.raftMembership.members if memberId != cv.myId ) {
        requestVoteRPCUID += 3
        requestVoteRPCUidTable = requestVoteRPCUidTable + (memberId -> requestVoteRPCUID)
        context.actorSelection(cv.addressTable(memberId)) ! RequestVoteRPC(RPCTo(requestVoteRPCUID,memberId),  cv.currentTerm,cv.myId ,lastLogIndex   ,lastLogTerm  )
        println("RequestVoteRPC Send to "+ cv.addressTable(memberId) +"    "+cv.myId+"  ->  "+memberId +"      "+RequestVoteRPC(RPCTo(requestVoteRPCUID,memberId),  cv.currentTerm,cv.myId ,lastLogIndex   ,lastLogTerm  ))
    }
  }

  def candidateBehavior :Receive = rpcHandlerBehavior orElse {
    case ReceiveTimeout => {
      timer.ifTimeout(becomeCandidate)
    }
    case RequestVoteRPCResult(RPCFrom(uid,from),     term, voteGranted) => {
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




















  import LeaderSubActor._


  def stepDown={
    cv.memberState=MemberState.Follower

    setVotedFor(null)


    if (cv.memberState==MemberState.Leader ) {
      if ( nowInOperation ){
        tempCmdSendingAgentActor ! "StepDown"
        nowInOperation=false
        tempCmdSendingAgentActor=null
      }

      //kill LeaderSubActor
      implicit val timeout = Timeout(10 millis)
      for (leaderSubActor <- leaderSubActorTable.values)  {
        val future=leaderSubActor ?  StopLeaderSubActor
        val result = Await.result(future,10 millis).asInstanceOf[String]
        if (result!="ok") throw new RuntimeException("LeaderSubActor Stop failed")
      }

      //clear state
    }

    if (cv.memberState==MemberState.Candidate) {

    }

    timer.resetTimeout
    context.become(followerBehavior)
  }



  //var clientCmdHandlerActor:ActorRef=null  // it is injected
  var leaderSubActorTable:Map[RaftMemberId,ActorRef]= null  // it is created when this actor's initialization  or when membership changed




  import RaftMemberLeader._
  import scala.collection.mutable.{Map => MutableMap}
  var nowInOperation=false
  var lastLogIndex:Long=0
  var lastClientCommandUid:Long=0

  var tempCmdSendingAgentActor:ActorRef =null
  var isAtLeastOneEntryOfThisTermCommited=false
  var newLogEntry:LogEntry=null


  var lastAppendedIndexTable:MutableMap[RaftMemberId,Option[Long]]=null

  def initLastAppendedIndexTable() {
    lastAppendedIndexTable=MutableMap()
    for( member <- cv.raftMembership.members  if member != cv.myId ) {
      lastAppendedIndexTable.put(member,None)
    }
  }


  def becomeLeader={
    println
    println
    println("----------------------------")
    println("-------becomeLeader---------")
    println(cv.myId)
    println("----------------------------")
    println
    println
    nowInOperation=false
    lastLogIndex=0
    tempCmdSendingAgentActor=null
    isAtLeastOneEntryOfThisTermCommited=false
    newLogEntry=null


    timer.close
    context.become(leaderBehavior)
    cv.memberState=MemberState.Leader


    val lastAppliedIndex = stateMachine.getLastAppliedIndex
    if (cv.commitIndex < lastAppliedIndex ) cv.commitIndex=lastAppliedIndex


    lastLogIndex=logEntryDB.getLastIndex.get

    initLastAppendedIndexTable


    import scala.concurrent.Await
    import akka.pattern.ask
    import akka.util.Timeout
    import scala.concurrent.Future

    implicit val timeout = Timeout(20 millis)
    var futures:List[Future[Any]]= Nil
    println("leaderSubActorTable="+leaderSubActorTable)
    for (leaderSubActor <- leaderSubActorTable.values)  {
      futures = futures  :+ leaderSubActor ?  StartLeaderSubActor(lastLogIndex)
    }

    //val initLeaderSubActorTimeoutVal=20 millis
    for( future:Future[Any] <- futures ) {
      val result=Await.result(future,20 millis).asInstanceOf[String]
      if (result!="ok") throw new RuntimeException("fail to init LeaderSubActor")
    }



  }

  def createLeaderSubActors {
    leaderSubActorTable =Map[RaftMemberId,ActorRef]()
    for ( memberId <- cv.raftMembership.members if memberId != cv.myId ) {
      leaderSubActorTable = leaderSubActorTable + (memberId -> createLeaderSubActor(memberId))
    }
  }

  def createLeaderSubActor(memberId:RaftMemberId):ActorRef = {
    log.info("createLeaderSubActor "+memberId)
    context.actorOf(
      Props(classOf[LeaderSubActorDependencyInjector], memberId,logEntryDB,cv),
      "leader_sub_"+memberId)


  }




  def leaderBehavior :Receive = rpcHandlerBehavior orElse {
    case AppendEntriesRPCResult(RPCFrom(uid,from),      term, success)  if ( cv.raftMembership.contains(from)  & term >= cv.currentTerm)  => {
      if (term>cv.currentTerm) {
        setCurrentTerm(term)
        stepDown
      } else {
        leaderSubActorTable(from) ! AppendEntriesRPCResult(RPCFrom(uid,from),   term,success)
      }
    }




    case ClientCommand(uid,command) =>  {
      println("---------------------------------------------------------------")
      println("---------------------------------------------------------------")
      println("---------------------------------------------------------------")
      println("---------------------------------------------------------------")
      println(ClientCommand(uid,command))
      println("---------------------------------------------------------------")
      println("---------------------------------------------------------------")
      println("---------------------------------------------------------------")
      println("---------------------------------------------------------------")
      if (nowInOperation) {
        sender ! "Busy"
      } else {

        lastClientCommandUid=uid
        nowInOperation=true
        tempCmdSendingAgentActor=sender

        newLogEntry =LogEntry(lastLogIndex,cv.currentTerm,command)
        logEntryDB.appendEntry(newLogEntry)

        lastLogIndex=logEntryDB.getLastIndex().get

        for( leaderSubActor <-leaderSubActorTable.values)  leaderSubActor ! NewLastLogIndex(lastLogIndex)
      }
    }

      /*
    case Commited(lastCommitedIndex)  => {
      val lastAppliedEntry=stateMachine.getLastAppliedLogEntry
      for(index <-lastAppliedEntry.index to (lastCommitedIndex - 1) ) stateMachine.applyEntry(logEntryDB.getEntry(index).get)
      if ( lastLogIndex!=lastCommitedIndex)  stateMachine.applyEntry(logEntryDB.getEntry(lastCommitedIndex).get)
      if (isAtLeastOneEntryOfThisTermCommited)  cv.commitedIndex= lastCommitedIndex

      if (nowInOperation &  lastLogIndex==lastCommitedIndex) {
        isAtLeastOneEntryOfThisTermCommited=true

        ret=stateMachine.applyEntry(newLogEntry)


        tempCmdSendingAgentActor ! StateMachineResult(ret)
        tempCmdSendingAgentActor=null
        nowInOperation=false
      }

      if (isAtLeastOneEntryOfThisTermCommited) cv.commitIndex= lastCommitedIndex
    } */
    case AppendOkNoti(memberId,nextIndex ) =>{
      lastAppendedIndexTable.put(memberId,Some(nextIndex))

      val lastCommitedIndex=getLastCommitedIndex(lastAppendedIndexTable)

      if (nowInOperation &  lastLogIndex == lastCommitedIndex )  {
        isAtLeastOneEntryOfThisTermCommited=true
        val ret=stateMachine.applyEntry(newLogEntry)

        tempCmdSendingAgentActor ! ClientCommandResult(lastClientCommandUid,ret,"code")
        tempCmdSendingAgentActor=null
        nowInOperation=false
      }

      if (isAtLeastOneEntryOfThisTermCommited) cv.commitIndex= lastCommitedIndex
    }

  }

  def getLastCommitedIndex(table:MutableMap[RaftMemberId,Option[Long]]):Long={
    //cv.raftMembership.checkMajority()
    4
  }


  init

  def init {
    log.info("init")
    println("RaftMember init")
    createLeaderSubActors
    timer.resetTimeout
  }

}

import akka.actor.IndirectActorProducer

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
  import java.util.Date

  type RPCUid = Long

  case class SentedRPC(uid:RPCUid,sentedTime:Long)
  val timer=new Timer()
  var nextIndex:Long= 0
  var sentedRPC:Option[SentedRPC]=None
  var uid = util.Random.nextLong
  var lastLogIndex:Long=0

  init

  def init{
    log.info("init")
    println("LeaderSubActor init "+memberId)
    timer.close
    nextIndex= 0
    sentedRPC=None
    //uid = util.Random.nextLong
    lastLogIndex=0
  }


  def recvRpc{
    sentedRPC=None
  }

  def rpc(prevLogIndex:Long, prevLogTerm:Long , entries:List[LogEntry] ,commitedIndex :Long ) {
    uid=uid+7

    val target=  context.actorSelection(cv.addressTable(memberId))
    val rpc =  AppendEntriesRPC( RPCTo(uid,memberId),
      cv.currentTerm ,cv.myId,prevLogIndex, prevLogTerm , entries ,commitedIndex )
    println("send    "+rpc)

    target ! rpc
    sentedRPC=Some(SentedRPC(uid,new Date().getTime))

    timer.resetTimeout(cv.electionTimeout*0.5 millisecond)
  }

   def snedRpc{
     if (sentedRPC.isEmpty | timer.isTimeout ) {
       val prevLogEntry = logEntryDB.getEntry(nextIndex-1).get
       val prevLogIndex = prevLogEntry.index
       val prevLogTerm = prevLogEntry.term

       val entry = logEntryDB.getEntry(nextIndex).getOrElse(null)
       val entries = if (entry==null) Nil else List(entry)

       rpc(prevLogIndex,prevLogTerm,entries,cv.commitIndex)
     }
   }

  def receive = initalState

  def initalState:Receive = {
    case StartLeaderSubActor(qlastLogIndex) =>{
      init
      lastLogIndex=qlastLogIndex
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

     case NewLastLogIndex(newlastLogIndex)  if newlastLogIndex > nextIndex  =>  {
       lastLogIndex=newlastLogIndex
       snedRpc
     }

     case AppendEntriesRPCResult(RPCFrom(uid,from),    term, success)  if  memberId==from  => {
       println("received  "+AppendEntriesRPCResult(RPCFrom(uid,from),    term, success))
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

         timer.resetTimeout(cv.electionTimeout*0.5 millisecond)

       }
     }

     case ReceiveTimeout => {
       snedRpc
     }
   }
}

object LeaderSubActor {
  case class StartLeaderSubActor(lastLogIndex:Long)
  case class StopLeaderSubActor()
}



object RaftMemberLeader {
  case class NewLastLogIndex(lastLogIndex:Long)
  case class AppendOkNoti(memberId:RaftMemberId, lastAppendedIndex:Long)
  case class LastCommitedIndex(lastCommitedIndex:Long)
}

object RaftRPC {
  case class RPCTo(uid:Long,to:RaftMemberId)
  case class RPCFrom(uid:Long,from:RaftMemberId)

  case class RequestVoteRPC(to:RPCTo ,    term:Long,candidateId :RaftMemberId ,lastLogIndex :Long  ,lastLogTerm:Long  )
  case class RequestVoteRPCResult(from:RPCFrom,   term:Long, voteGranted:Boolean)

  case class AppendEntriesRPC(to:RPCTo,   term:Long , leaderId:RaftMemberId,prevLogIndex:Long, prevLogTerm:Long , entries: List[LogEntry] ,commitIndex:Long )
  case class AppendEntriesRPCResult(from:RPCFrom,   term:Long, success:Boolean)

}
