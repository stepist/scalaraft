package jkm.cineclub.raft.DB

import jkm.cineclub.raft.RaftConfig.DBInfo
import com.typesafe.scalalogging.slf4j.Logging
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/26/13
 * Time: 9:18 PM
 * To change this template use File | Settings | File Templates.
 */
object RaftLevelDB extends RaftDB with Logging {
  def checkExistDB(dbInfo:DBInfo):Boolean ={
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

  def getLogEntryDB(dbInfo: DBInfo): LogEntryDB = {
    new LogEntryLevelDB(dbInfo)
  }

  def getPersistentStateDB(dbInfo: DBInfo): PersistentStateDB = {
    new PersistentStateLevelDB(dbInfo)
  }
}
