package jkm.cineclub.raft.DB

import jkm.cineclub.raft.PersistentState
import org.iq80.leveldb.{WriteBatch, WriteOptions, Options, DB}
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File
import jkm.cineclub.raft.PersistentState._

import scala.pickling._
import binary._

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/17/13
 * Time: 9:47 PM
 * To change this template use File | Settings | File Templates.
 */

class PersistentStateLevelDB(val dbName:String,val dbRootPath:String=null) extends PersistentStateDB{

  val db :DB = factory.open(new File(dbRootPath,dbName), new Options())
  implicit val writeOption=new WriteOptions().sync(true)

  def getBP(value:Array[Byte]) = binary.toBinaryPickle(value)

  def getState(dbKey:PersistentStateDBKey):Option[Any] ={
    if (dbKey==null) return None
    val value=db.get(dbKey.getBytes)
    if (value==null) return None

    dbKey match{
      case LastAppendedIndexDBKey => Some(getBP(value).unpickle[LastAppendedIndex] )
      case LastAppliedIndexDBKey => Some(getBP(value).unpickle[LastAppliedIndex] )
      case LeaderCommitIndexDBKey => Some(getBP(value).unpickle[LeaderCommitIndex] )
      case MyIdDBKey =>  Some(getBP(value).unpickle[MyId] )
      case RaftMemberIdDBKey =>  Some(getBP(value).unpickle[RaftMemberId])
      case RaftMembershipDBKey => Some(getBP(value).unpickle[RaftMembership])
      case TermInfoDBKey => Some(getBP(value).unpickle[TermInfo])
      case _ => None
    }
  }

  def getStates(dbKeys:List[PersistentStateDBKey]):List[(PersistentStateDBKey,Option[Any])] = dbKeys.map(x => (x,getState(x)))


  def makeBytesValue(dbKey:PersistentStateDBKey,data:Any):Array[Byte]= {
    dbKey match{
      case LastAppendedIndexDBKey if data.isInstanceOf[LastAppendedIndex] => data.asInstanceOf[LastAppendedIndex].pickle.value
      case LastAppliedIndexDBKey if data.isInstanceOf[LastAppliedIndex]=>  data.asInstanceOf[LastAppendedIndex].pickle.value
      case LeaderCommitIndexDBKey if data.isInstanceOf[LeaderCommitIndex] =>  data.asInstanceOf[LeaderCommitIndex].pickle.value
      case MyIdDBKey if data.isInstanceOf[MyId]  => data.asInstanceOf[MyId].pickle.value
      case RaftMemberIdDBKey if data.isInstanceOf[RaftMemberId] => data.asInstanceOf[RaftMemberId].pickle.value
      case RaftMembershipDBKey if data.isInstanceOf[RaftMembership] => data.asInstanceOf[RaftMembership].pickle.value
      case TermInfoDBKey if data.isInstanceOf[TermInfo] => data.asInstanceOf[TermInfo].pickle.value
      case _ => null
    }
  }

  def putState(dbKey:PersistentStateDBKey,data:Any):Boolean= {
    if (dbKey==null | data==null) return false

    val key=dbKey.getBytes

    val value=makeBytesValue(dbKey,data)
    if (value==null) return false

    db.put(key,value)
    true
  }

  def putStates( pairs:Map[PersistentStateDBKey,Any]) ={
    val batch :WriteBatch= db.createWriteBatch()
    try{
      for( (dbKey,data) <- pairs) {
        val key=dbKey.getBytes
        val value=makeBytesValue(dbKey,data)
        batch.put(key,value)
      }
      db.write(batch,writeOption)
    } finally {batch.close()}
  }

  def close=db.close

}




object TestPersistentStateLevelDB extends App{

  case class AA(a:Int,b:String)

  def aaa(a:Any){
    //val c= a.asInstanceOf[AA].pickle.value
    val c= a.pickle.value
    val d= binary.toBinaryPickle(c).unpickle[AA]
    println(d)
  }
  aaa(AA(1,"abcd"))

}