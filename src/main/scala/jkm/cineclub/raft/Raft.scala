package jkm.cineclub.raft


import com.typesafe.config.{ConfigValue, ConfigList, ConfigFactory, Config}
import jkm.cineclub.raft.PersistentState.RaftMemberId
import scala.collection.JavaConversions._
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File
import jkm.cineclub.raft.DB._
import jkm.cineclub.raft.PersistentState._
import jkm.cineclub.raft.DB.LogEntryDB.LogEntry
import com.typesafe.scalalogging.slf4j.Logging
import akka.actor.{Inbox, Props, ActorSystem}
import jkm.cineclub.raft.ClientCmdHandlerActor.ClientCommand
import jkm.cineclub.raft.ClientCmdHandlerActor.ClientCommand
import jkm.cineclub.raft.DB.LogEntryDB.LogEntry
import jkm.cineclub.raft.RaftContext
import jkm.cineclub.raft.PersistentState.TermInfo

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/21/13
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
object  Raft extends App  with Logging{
  import RaftConfig._

  val raftMemberSystemName="raft"
  val raftServiceSystemName="service"
  val raftmemberActorName= "raftmember"
  val clientCmdHandlerActorName="clientHandler"


  val raftDB:RaftDB=RaftLevelDB



  def initPersistentStateDB(raftConfig:RaftConfig)= {
    val dbInfo= raftConfig.persistentStateDBInfo
    if (! raftDB.checkExistDB(dbInfo) ){
      logger.info("")
      logger.info("Create PersistentStateDB "+dbInfo)
      logger.info("")
      val db = raftDB.getPersistentStateDB(dbInfo)

      //db.putState(LastAppendedIndexDBKey,0)
      //db.putState(LastAppliedIndexDBKey,0)
      //db.putState(LeaderCommitIndexDBKey,0)
      db.putState(MyIdDBKey,raftConfig.id)
      db.putState(RaftMembershipDBKey,raftConfig.membership)
      db.putState(TermInfoDBKey, TermInfo(0,null))
      db.putState(ElectionTimeoutDBKey,raftConfig.electionTimeout)

      db.close
    }
  }

  def initLogEntryDB(raftConfig:RaftConfig) ={
    val dbInfo= raftConfig.logEntryDBInfo
    if (! raftDB.checkExistDB(dbInfo) ){
      logger.info("")
      logger.info("Create LogEntryLevelDB "+dbInfo)
      logger.info("")
      val logEntryLevelDB =raftDB.getLogEntryDB(dbInfo)
      logEntryLevelDB.appendEntry(LogEntry(0,0,"start raft"))
      logEntryLevelDB.close
    }
  }


  def checkMyID(raftConfig:RaftConfig)={
    val dbInfo= raftConfig.persistentStateDBInfo
    val db = raftDB.getPersistentStateDB(dbInfo)
    val myId=db.getState(MyIdDBKey).get
    db.close

    if (myId!=raftConfig.id) {
      logger.error("MyID mismatch DB Value= "+myId+", config Value="+raftConfig.id)
      throw new RuntimeException("MyID mismatch DB Value= "+myId+", config Value="+raftConfig.id)
    }
  }


  def initDBs(raftConfig:RaftConfig)= {
    initLogEntryDB(raftConfig)
    initPersistentStateDB(raftConfig)
    checkMyID(raftConfig)
  }


  def initCurrentValues(raftConfig:RaftConfig,currentValues:CurrentValues){
    val dbInfo= raftConfig.persistentStateDBInfo
    val db =raftDB.getPersistentStateDB(dbInfo)


    import jkm.cineclub.raft.CurrentValues

    currentValues.myId = db.getMyId

    currentValues.memberState=CurrentValues.MemberState.Follower
    currentValues.leaderId=null

    val termInfo =db.getTermInfo
    currentValues.currentTerm=termInfo.currentTerm
    currentValues.votedFor = termInfo.votedFor

    //"akka.tcp://raft@127.0.0.1:3010/user/raftmember"
    currentValues.addressTable=raftConfig.addressTable.map{ case (memberId,TcpAddress(ip,port))  => (memberId,s"akka.tcp://raft@$ip:$port/user/$raftmemberActorName")  }
    currentValues.addressTableRaw=raftConfig.addressTable



    currentValues.raftMembership=db.getRaftMembership

    currentValues.persistentStateDBInfo=raftConfig.persistentStateDBInfo
    currentValues.logEntryDBInfo=raftConfig.logEntryDBInfo

    currentValues.electionTimeout=db.getElectionTimeout

    db.close
  }


  def startRaftMember(a:RaftConfig){
    import jkm.cineclub.raft.CurrentValues
    val config=a //readConfig("raft.conf","raft.raft01")
    println("---- config ")
    printRaftConfig(config)
    println
    println

    val currentValues=new CurrentValues
    println("---- currentValues ")
    currentValues.printCurrentValues
    println
    initDBs(config)
    println("---- currentValues   after initDB")
    currentValues.printCurrentValues
    println()
    initCurrentValues(config,currentValues)
    println("---- currentValues   after initCurrentValues")
    currentValues.printCurrentValues
    println()



    implicit val raftCxt=RaftContext(
      logEntryDB = raftDB.getLogEntryDB(currentValues.logEntryDBInfo),
      persistentStateDB = raftDB.getPersistentStateDB(currentValues.persistentStateDBInfo) ,
      cv=currentValues,
      stateMachine = new DummyStateMachine(DBInfo(dbName =currentValues.logEntryDBInfo.dbName+".Statemachine" ,dbRootPath = null))
    )

    val address=currentValues.addressTableRaw.get(currentValues.myId).get

    println(address)
    val raftMemberSystem = ActorSystem(raftMemberSystemName,

      ConfigFactory.parseString("akka.remote.netty.tcp{ hostname=\""+address.hostname +"\", port="+address.port+" }")
        .withFallback(ConfigFactory.load())
    )


    val raftServiceSystem= ActorSystem(raftServiceSystemName,

      ConfigFactory.parseString("akka.remote.netty.tcp{ hostname=\""+config.serviceAddress.hostname +"\", port="+config.serviceAddress.port+" }")
        .withFallback(ConfigFactory.load())
    )


    val raftmemberActor = raftMemberSystem.actorOf(
      Props(classOf[RaftMemberDependencyInjector], raftCxt),
      raftmemberActorName)

    val clientCmdHandlerActor = raftServiceSystem.actorOf(
      Props(classOf[ClientCmdHandlerActorDependencyInjector], raftCxt,raftmemberActor),
      clientCmdHandlerActorName)
  }

  import jkm.cineclub.raft.CurrentValues

  startRaftMember(readConfig("raft.conf","raft.raft01"))
  startRaftMember(readConfig("raft.conf","raft.raft02"))
  startRaftMember(readConfig("raft.conf","raft.raft03"))


  Thread.sleep(2*1000)

  import akka.pattern.ask
  import scala.concurrent.duration._
  import akka.actor.dsl._
  import scala.concurrent.Await
  import akka.pattern.ask
  import akka.util.Timeout
  import scala.concurrent.duration._

  implicit val testSystem=ActorSystem("testSystem")

  //implicit val i = inbox()
  val target=testSystem.actorSelection(  s"akka.tcp://$raftServiceSystemName@127.0.0.1:3553/user/$clientCmdHandlerActorName" )
  println
  println
  println
  println
  println
  println
  println
  println
  println
  println
  println
  println
  println
  println


  //inbox.send(target, ClientCommand(342,"test"))
  target ! ClientCommand(342,"test")
 // inbox.send(,"adfasd")

 // println("Inbox received : "+inbox.receive(4 seconds) )


}


case class RaftContext(val logEntryDB:LogEntryDB, val persistentStateDB:PersistentStateDB, val  cv:CurrentValues , val stateMachine:StateMachine)


