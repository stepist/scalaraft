package jkm.cineclub.raft


import com.typesafe.config.{ConfigValue, ConfigList, ConfigFactory, Config}
import jkm.cineclub.raft.PersistentState.RaftMemberId
import scala.collection.JavaConversions._
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File
import jkm.cineclub.raft.DB.{PersistentStateDB, LogEntryDB, LogEntryLevelDB, PersistentStateLevelDB}
import jkm.cineclub.raft.PersistentState._
import jkm.cineclub.raft.DB.LogEntryDB.LogEntry
import com.typesafe.scalalogging.slf4j.Logging
import akka.actor.{Props, ActorSystem}

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/21/13
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
object  Raft extends App  with Logging{

  import RaftConfig._

  def checkExistLevelDB(dbInfo:DBInfo):Boolean ={
    val dbDir=new File(dbInfo.dbRootPath,dbInfo.dbName)

    if ( ! dbDir.exists() ) return false

    if ( ! dbDir.isDirectory ) {
      logger.error("Something's wrong with the Level DB Dir. It is not a directory "+dbInfo)
      throw new RuntimeException("Something wrong with the Level DB Dir. It is not a directory "+dbInfo)
    }

    if ( dbDir.listFiles().size == 0 ) return false

    if (new File(dbInfo.dbRootPath,dbInfo.dbName+"/CURRENT").exists() & new File(dbInfo.dbRootPath,dbInfo.dbName+"/LOCK").exists() ) {
      return  true
    }
    else {
      logger.error("Something's wrong with the Level DB Dir. LevelDB Files are not proper. "+dbInfo)
      throw new RuntimeException("Something wrong with the Level DB Dir. LevelDB Files are not proper. "+dbInfo)
    }
  }

  def initPersistentStateDB(raftConfig:RaftConfig)= {
    val dbInfo= raftConfig.persistentStateDBInfo
    if (! checkExistLevelDB(dbInfo) ){
      logger.info("")
      logger.info("Create PersistentStateDB "+dbInfo)
      logger.info("")
      val db = new PersistentStateLevelDB(dbInfo)

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
    if (! checkExistLevelDB(dbInfo) ){
      logger.info("")
      logger.info("Create LogEntryLevelDB "+dbInfo)
      logger.info("")
      val logEntryLevelDB = new LogEntryLevelDB(dbInfo)
      logEntryLevelDB.appendEntry(LogEntry(0,0,"start raft"))
      logEntryLevelDB.close
    }
  }


  def checkMyID(raftConfig:RaftConfig)={
    val dbInfo= raftConfig.persistentStateDBInfo
    val db = new PersistentStateLevelDB(dbInfo)
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
    val db = new PersistentStateLevelDB(dbInfo)


    import jkm.cineclub.raft.CurrentValues

    currentValues.myId = db.getMyId

    currentValues.memberState=CurrentValues.MemberState.Follower
    currentValues.leaderId=null

    val termInfo =db.getTermInfo
    currentValues.currentTerm=termInfo.currentTerm
    currentValues.votedFor = termInfo.votedFor

    //"akka.tcp://raft@127.0.0.1:3010/user/raftmember"
    currentValues.addressTable=raftConfig.addressTable.map{ case (memberId,TcpAddress(ip,port))  => (memberId,"akka.tcp://raft@%s:%d/user/raftmember".format(ip,port))  }
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
      logEntryDB = new LogEntryLevelDB(currentValues.logEntryDBInfo),
      persistentStateDB = new PersistentStateLevelDB(currentValues.persistentStateDBInfo),
      cv=currentValues,
      stateMachine = new DummyStateMachine(DBInfo(dbName =currentValues.logEntryDBInfo.dbName+".Statemachine" ,dbRootPath = null))
    )

    val address=currentValues.addressTableRaw.get(currentValues.myId).get

    println(address)
    val raftMemberSystem = ActorSystem("raft",

      ConfigFactory.parseString("akka.remote.netty.tcp{ hostname=\""+address.hostname +"\", port="+address.port+" }")
        .withFallback(ConfigFactory.load())
    )





    val raftServiceSystem= ActorSystem("service",

      ConfigFactory.parseString("akka.remote.netty.tcp{ hostname=\""+config.serviceAddress.hostname +"\", port="+config.serviceAddress.port+" }")
        .withFallback(ConfigFactory.load())
    )

    val raftmemberActor = raftMemberSystem.actorOf(
      Props(classOf[RaftMemberDependencyInjector], raftCxt),
      "raftmember")

    val clientCmdHandlerActor = raftServiceSystem.actorOf(
      Props(classOf[ClientCmdHandlerActorDependencyInjector], raftCxt,raftmemberActor),
      "clientHandler")
  }

  import jkm.cineclub.raft.CurrentValues

  startRaftMember(readConfig("raft.conf","raft.raft01"))
  startRaftMember(readConfig("raft.conf","raft.raft02"))
  startRaftMember(readConfig("raft.conf","raft.raft03"))





}


case class RaftContext(val logEntryDB:LogEntryDB, val persistentStateDB:PersistentStateDB, val  cv:CurrentValues , val stateMachine:StateMachine)


class RaftConfig {
  import RaftConfig._
  var id:RaftMemberId=null
  var serviceAddress:TcpAddress=null

  var persistentStateDBInfo:DBInfo=null
  var logEntryDBInfo:DBInfo=null

  var membership:RaftMembership=null
  var addressTable:Map[RaftMemberId,TcpAddress]=null

  var electionTimeout:Int = -1
}
object RaftConfig {
  case class TcpAddress(hostname:String,port:Int)
  case class DBInfo(dbName:String,dbRootPath:String) {
    override def toString=  "("+"dbName="+dbName+","+"dbRootPath="+dbRootPath+")"
  }

  def convertToTcpAddress(a:ConfigValue): TcpAddress= {
    val address=a.unwrapped().asInstanceOf[java.util.ArrayList[Object]].toList
    val hostname=address(0).asInstanceOf[String]
    val port = address(1).asInstanceOf[Int]
    TcpAddress(hostname,port)
  }

  implicit def convertConfigToTcpAddress(a:Config): TcpAddress= {
    val hostname=a.getString("hostname")
    val port = a.getInt("port")
    TcpAddress(hostname,port)
  }

  implicit def convertConfigToDBInfo(a:Config):DBInfo = {
    val dbName=a.getString("dbName")
    var dbRootPath=a.getString("rootPath")
    if (dbRootPath.isEmpty) dbRootPath=null
    DBInfo(dbName,dbRootPath)
  }

  def getRaftMembership(a:Config):RaftMembership={
    RaftMembership(
      RaftMembership.getConfigType(a.getString("configType")),
      a.getStringList("newMembers").toList ,
      a.getStringList("oldMembers").toList )
  }

  def readConfig(configName:String,prefix:String):RaftConfig ={
    val conf=ConfigFactory.load(configName)
    if (conf==null) return null

    val raftConfig=new RaftConfig

    def addPrefix(path:String) =prefix+"."+ path

    raftConfig.id=conf.getString(addPrefix("id"))
    raftConfig.serviceAddress=conf.getConfig(addPrefix("serviceAddress"))
    raftConfig.persistentStateDBInfo=conf.getConfig(addPrefix("persistentStateDB"))
    raftConfig.logEntryDBInfo=conf.getConfig(addPrefix("logEntryDB"))
    raftConfig.membership=getRaftMembership(conf.getConfig(addPrefix("init.membership")))
    raftConfig.addressTable=conf.getConfig(addPrefix("init.addressTable")).entrySet().toList.map( x=> (x.getKey,convertToTcpAddress(x.getValue))).toMap
    raftConfig.electionTimeout=conf.getInt( addPrefix("init.electionTimeout"))
    raftConfig
  }

  def printRaftConfig(raftConfig:RaftConfig) ={
    println("id="+raftConfig.id)
    println("serverAddress="+raftConfig.serviceAddress)
    println("persistentStateDBInfo="+raftConfig.persistentStateDBInfo)
    println("logEntryDBInfo="+raftConfig.logEntryDBInfo)
    println("members="+raftConfig.membership)
    println("addressTable="+raftConfig.addressTable)
    println("electionTimeout="+raftConfig.electionTimeout)
  }

}
