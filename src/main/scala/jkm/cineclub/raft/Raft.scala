package jkm.cineclub.raft


import com.typesafe.config.{ConfigValue, ConfigList, ConfigFactory, Config}
import jkm.cineclub.raft.PersistentState.RaftMemberId
import scala.collection.JavaConversions._
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File
import jkm.cineclub.raft.DB.PersistentStateLevelDB
import jkm.cineclub.raft.PersistentState._

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/21/13
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
object  Raft extends App {

  case class TcpAddress(hostname:String,port:Int)
  case class DBInfo(dbName:String,dbRootPath:String)

  class RaftConfig {
    var id:RaftMemberId=null
    var serviceAddress:TcpAddress=null

    var persistentStateDBInfo:DBInfo=null
    var logEntryDBInfo:DBInfo=null

    var membership:RaftMembership=null
    var addressTable:Map[RaftMemberId,TcpAddress]=null

    def printValues() ={
      println("id="+id)
      println("serverAddress="+serviceAddress)
      println("persistentStateDBInfo="+persistentStateDBInfo)
      println("logEntryDBInfo="+logEntryDBInfo)
      println("members="+membership)
      println("addressTable="+addressTable)
    }
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
    raftConfig.addressTable=conf.getConfig("raft.addressTable").entrySet().toList.map( x=> (x.getKey,convertToTcpAddress(x.getValue))).toMap
    raftConfig
  }

  //val config=readConfig("raft.conf","raft.raft01")

  def initPersistentStateDB(raftConfig:RaftConfig)= {
    val dbInfo= raftConfig.persistentStateDBInfo
    val dbFile=new File(dbInfo.dbRootPath,dbInfo.dbName)
    if (! dbFile.exists ){
      val db = new PersistentStateLevelDB(dbInfo.dbName,dbInfo.dbRootPath)

      //db.putState(LastAppendedIndexDBKey,0)
      //db.putState(LastAppliedIndexDBKey,0)
      //db.putState(LeaderCommitIndexDBKey,0)
      db.putState(MyIdDBKey,raftConfig.id)
      db.putState(RaftMembershipDBKey,raftConfig.membership)
      db.putState(TermInfoDBKey, TermInfo(0,null))

      db.close
    }
  }

  def initLogEntryDB(raftConfig:RaftConfig) ={
    val dbInfo= raftConfig.logEntryDBInfo
    val db :DB = factory.open(new File(dbInfo.dbRootPath,dbInfo.dbName), new Options())
    db.close
  }


  def checkMyID(raftConfig:RaftConfig)={
    var check:Boolean=false

    val dbInfo= raftConfig.persistentStateDBInfo
    val dbFile=new File(dbInfo.dbRootPath,dbInfo.dbName)
    val db = new PersistentStateLevelDB(dbInfo.dbName,dbInfo.dbRootPath)
    try{
      val myId=db.getState(MyIdDBKey).get
      check = myId==raftConfig.id
      if (!check) println("MyID mismatch DB Value= "+myId+", config Value="+raftConfig.id)
    }finally{
      db.close
    }

    if (!check) throw new RuntimeException
  }


  def initDBs(raftConfig:RaftConfig)= {

    initLogEntryDB(raftConfig)
    initPersistentStateDB(raftConfig)

    checkMyID(raftConfig)
  }


  def initCurrentValues(raftConfig:RaftConfig){
    val dbInfo= raftConfig.persistentStateDBInfo
    val db = new PersistentStateLevelDB(dbInfo.dbName,dbInfo.dbRootPath)


     import jkm.cineclub.raft.CurrentValues

    CurrentValues.myId = db.getMyId

    CurrentValues.memberState=CurrentValues.MemberState.Follower
    CurrentValues.leaderId=null

    val termInfo =db.getTermInfo
    CurrentValues.currentTerm=termInfo.currentTerm
    CurrentValues.votedFor = termInfo.votedFor

    //"akka.tcp://raft@127.0.0.1:3010/user/raftmember"
    CurrentValues.addressTable=raftConfig.addressTable.map{ case (memberId,TcpAddress(ip,port))  => (memberId,"akka.tcp://raft@%s:%d/user/raftmember".format(ip,port))  }
    CurrentValues.raftMembership=db.getRaftMembership

    db.close
  }



  import jkm.cineclub.raft.CurrentValues
  val config=readConfig("raft.conf","raft.raft01")
  config.printValues()
  println
  println
  CurrentValues.printCurrentValues
  println
  initDBs(config)
  CurrentValues.printCurrentValues
  println()
  initCurrentValues(config)
  CurrentValues.printCurrentValues
  println()



}
