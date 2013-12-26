package jkm.cineclub.raft

import jkm.cineclub.raft.PersistentState._
import com.typesafe.config.{ConfigFactory, Config, ConfigValue}
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/26/13
 * Time: 9:04 PM
 * To change this template use File | Settings | File Templates.
 */
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
