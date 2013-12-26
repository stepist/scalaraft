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
import akka.actor.Actor.Receive

class Timer(implicit val context:ActorContext) {

  val guard=1.1
  var requestedTimeMillis:Long = 0
  var timeoutVal:Duration = Duration.Undefined


  def resetTimeout(a:Duration) {
    timeoutVal=a
    requestedTimeMillis=System.currentTimeMillis()
    context.setReceiveTimeout(timeoutVal*guard)
  }

  def isTimeouted:Boolean ={
    if (isClosed) return false
    val elapsedTime= System.currentTimeMillis() - requestedTimeMillis
    elapsedTime.millis  >= timeoutVal
  }

  def ifTimeouted(exec:() => Unit) {
    if (!isClosed) {
        val elapsedTime= System.currentTimeMillis() - requestedTimeMillis

        if (elapsedTime.millis  >= timeoutVal) exec()
        else resetTimeout(timeoutVal - elapsedTime.millis)
    }
  }

  def isClosed = (timeoutVal == Duration.Undefined & requestedTimeMillis==0)
  def close= {
    requestedTimeMillis = 0
    timeoutVal = Duration.Undefined
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

  import scala.collection.mutable.{Map => MutableMap}
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




  def isLocalLogMoreCompleteThanCandidate(cLastLogIndex:Long,cLastLogTerm:Long):Boolean={
    val LogEntry(vLastLogIndex,vLastLogTerm,_)=logEntryDB.getLast().get
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
            timer.resetTimeout(cv.electionTimeout millis)
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

          timer.resetTimeout(cv.electionTimeout millis)

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
            val lastLogIndex = logEntryDB.getLastIndex().get
            println("-----------------------------")
              for( index <-  (lastAppliedIndex+1) to cv.commitIndex   if index <= lastLogIndex)  {
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
      timer.ifTimeouted(becomeCandidate)
    }
  }



  def intoFollowerState={
    // 1.State Change
    log.info("into Follower")
    cv.memberState=MemberState.Follower
    context.become(followerBehavior)

    // 2. Term Info Update
    setVotedFor(null)

    // 3. Timeout Update
    timer.resetTimeout(cv.electionTimeout millis)

  }

  def outofFollowerState ={
    timer.close
  }


  def intoCandidateState = {
    // 1.State Change
    log.info("into Candidate")
    cv.memberState=MemberState.Candidate
    context.become(candidateBehavior)

    // 2. Term Info Update
    setCurrentTerm(cv.currentTerm+1)
    setVotedFor(cv.myId)

    // 3. Timeout Update
    import util.Random
    timer.resetTimeout(cv.electionTimeout*(1.0+Random.nextFloat*0.6)  millis)


    val lastLogEntry= logEntryDB.getLast().get
    val lastLogIndex = lastLogEntry.index
    val lastLogTerm = lastLogEntry.term

    requestVoteRPCUidTable= Map[RaftMemberId,Long]()
    voteList=List[RaftMemberId]()


    for ( memberId <- cv.raftMembership.members if memberId != cv.myId ) {
      requestVoteRPCUID += 3
      requestVoteRPCUidTable = requestVoteRPCUidTable + (memberId -> requestVoteRPCUID)
      val rpc = RequestVoteRPC(RPCTo(requestVoteRPCUID,memberId),  cv.currentTerm,cv.myId ,lastLogIndex   ,lastLogTerm  )
      sendRequestVoteRPC(memberId,rpc)
    }
  }

  def sendRequestVoteRPC(memberId:RaftMemberId,rpc:RequestVoteRPC )= {
    context.actorSelection(cv.addressTable(memberId)) ! rpc
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
    timer.resetTimeout(cv.electionTimeout*(1.0+Random.nextFloat*0.6)  millis)

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
      timer.ifTimeouted(becomeCandidate)
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
        tempCmdSendingAgentActor ! ClientCommandResult(lastClientCommandUid,null,"stepdown")
        nowInOperation=false
        tempCmdSendingAgentActor=null
      }

      //kill LeaderSubActor
      implicit val timeout = Timeout(10 millis)
      for (leaderSubActor <- cv.leaderSubActorTable.values)  {
        val future=leaderSubActor ?  StopLeaderSubActor
        val result = Await.result(future,10 millis).asInstanceOf[String]
        if (result!="ok") throw new RuntimeException("LeaderSubActor Stop failed")
      }

      //clear state
    }

    if (cv.memberState==MemberState.Candidate) {

    }

    timer.resetTimeout(cv.electionTimeout millis)
    context.become(followerBehavior)
  }




  //var leaderSubActorTable:Map[RaftMemberId,ActorRef]= null  // it is created when this actor's initialization  or when membership changed




  import RaftMemberLeader._

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
    println("leaderSubActorTable="+cv.leaderSubActorTable)
    for (leaderSubActor <- cv.leaderSubActorTable.values)  {
      futures = futures  :+ leaderSubActor ?  StartLeaderSubActor(lastLogIndex)
    }

    //val initLeaderSubActorTimeoutVal=20 millis
    for( future:Future[Any] <- futures ) {
      val result=Await.result(future,20 millis).asInstanceOf[String]
      if (result!="ok") throw new RuntimeException("fail to init LeaderSubActor")
    }



  }

  def createLeaderSubActors {
    cv.leaderSubActorTable =Map[RaftMemberId,ActorRef]()
    for ( memberId <- cv.raftMembership.members if memberId != cv.myId ) {
      cv.leaderSubActorTable = cv.leaderSubActorTable + (memberId -> createLeaderSubActor(memberId))
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
        cv.leaderSubActorTable(from) ! AppendEntriesRPCResult(RPCFrom(uid,from),   term,success)
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
        sender ! ClientCommandResult(uid,null,"busy")
      } else {

        lastClientCommandUid=uid
        nowInOperation=true
        tempCmdSendingAgentActor=sender


        lastLogIndex = lastLogIndex+1
        newLogEntry =LogEntry(lastLogIndex,cv.currentTerm,command)
        println("newLogEntry="+newLogEntry)
        logEntryDB.appendEntry(newLogEntry)

        //lastLogIndex=logEntryDB.getLastIndex().get

        for( leaderSubActor <-cv.leaderSubActorTable.values)  leaderSubActor ! NewLastLogIndex(lastLogIndex)
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

        tempCmdSendingAgentActor ! ClientCommandResult(lastClientCommandUid,ret,"ok")
        tempCmdSendingAgentActor=null
        nowInOperation=false
      }

      if (isAtLeastOneEntryOfThisTermCommited) cv.commitIndex= lastCommitedIndex
    }

  }
  import PersistentState._
  def getLastCommitedIndex(table:MutableMap[RaftMemberId,Option[Long]]):Long={
    cv.raftMembership.configType match {
      case RaftMembership.RaftMembershipConfigNormal => {
        cv.raftMembership.newMembers.filter(_!=cv.myId).map(table(_)).map(_.getOrElse(-1.toLong)).min
      }
      case RaftMembership.RaftMembershipConfigJoint => {
        math.min(
          cv.raftMembership.newMembers.filter(_!=cv.myId).map(table(_)).map(_.getOrElse(-1.toLong)).min,
          cv.raftMembership.oldMembers.filter(_!=cv.myId).map(table(_)).map(_.getOrElse(-1.toLong)).min
        )
      }
      case _ => -1
    }
  }




  def init {
    log.info("init")
    println("RaftMember init")
    createLeaderSubActors
    timer.resetTimeout(cv.electionTimeout millis)
  }

  override def preStart(): Unit = {
    init
  }

  def receive = followerBehavior
}








class LeaderState(val raftCtx:RaftContext ,val timer:Timer , val raftMember:RaftMember) {
  import LeaderSubActor._
  import RaftMemberLeader._
  import RaftRPC._

  val cv = raftCtx.cv
  val logEntryDB = raftCtx.logEntryDB
  val persistentStateDB = raftCtx.persistentStateDB
  val stateMachine=raftCtx.stateMachine


  def intro ={

  }

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
    raftMember.context.become(leaderBehavior)
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
    println("leaderSubActorTable="+cv.leaderSubActorTable)
    for (leaderSubActor <- cv.leaderSubActorTable.values)  {
      futures = futures  :+ leaderSubActor ?  StartLeaderSubActor(lastLogIndex)
    }

    //val initLeaderSubActorTimeoutVal=20 millis
    for( future:Future[Any] <- futures ) {
      val result=Await.result(future,20 millis).asInstanceOf[String]
      if (result!="ok") throw new RuntimeException("fail to init LeaderSubActor")
    }



  }


  def leaderBehavior :Receive =  {
    case AppendEntriesRPCResult(RPCFrom(uid,from),      term, success)  if ( cv.raftMembership.contains(from)  & term >= cv.currentTerm)  => {
      if (term>cv.currentTerm) {
        raftMember.setCurrentTerm(term)
        raftMember.stepDown
      } else {
        cv.leaderSubActorTable(from) ! AppendEntriesRPCResult(RPCFrom(uid,from),   term,success)
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
        raftMember.sender ! ClientCommandResult(uid,null,"busy")
      } else {

        lastClientCommandUid=uid
        nowInOperation=true
        tempCmdSendingAgentActor=raftMember.sender


        lastLogIndex = lastLogIndex+1
        newLogEntry =LogEntry(lastLogIndex,cv.currentTerm,command)
        println("newLogEntry="+newLogEntry)
        logEntryDB.appendEntry(newLogEntry)

        //lastLogIndex=logEntryDB.getLastIndex().get

        for( leaderSubActor <-cv.leaderSubActorTable.values)  leaderSubActor ! NewLastLogIndex(lastLogIndex)
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

        tempCmdSendingAgentActor ! ClientCommandResult(lastClientCommandUid,ret,"ok")
        tempCmdSendingAgentActor=null
        nowInOperation=false
      }

      if (isAtLeastOneEntryOfThisTermCommited) cv.commitIndex= lastCommitedIndex
    }

  }
  import PersistentState._
  def getLastCommitedIndex(table:MutableMap[RaftMemberId,Option[Long]]):Long={
    cv.raftMembership.configType match {
      case RaftMembership.RaftMembershipConfigNormal => {
        cv.raftMembership.newMembers.filter(_!=cv.myId).map(table(_)).map(_.getOrElse(-1.toLong)).min
      }
      case RaftMembership.RaftMembershipConfigJoint => {
        math.min(
          cv.raftMembership.newMembers.filter(_!=cv.myId).map(table(_)).map(_.getOrElse(-1.toLong)).min,
          cv.raftMembership.oldMembers.filter(_!=cv.myId).map(table(_)).map(_.getOrElse(-1.toLong)).min
        )
      }
      case _ => -1
    }
  }

  def behavior :Receive= {
    case
  }

  def outro ={

  }
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
