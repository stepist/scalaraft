package kjm.cineclub.raft


import org.scalatest._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File
import org.iq80.leveldb.Options
import jkm.cineclub.raft.DB.{PersistentStateLevelDB, PersistentStateDB}
import jkm.cineclub.raft.PersistentState
import jkm.cineclub.raft.RaftConfig.DBInfo


/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/18/13
 * Time: 12:07 AM
 * To change this template use File | Settings | File Templates.
 */
class PersistentStateDBSpec extends FlatSpec with Matchers {


  def initLogEntryDB(dbName:String) {
    try{
      factory.destroy(new File(dbName), new Options())
    }catch{
      case e:Exception => println(e)
    }
  }

  def createLogEntryDB(dbName:String) : PersistentStateDB ={
    new PersistentStateLevelDB(DBInfo(dbName=dbName,dbRootPath = null))
  }


  val dbName="TestPersistentStateLevelDB"


  it should "get an PersistentState RaftMemberId" in {
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    type AAA=PersistentState.RaftMemberId
    var dbKey =PersistentState.RaftMemberIdDBKey
    val value:AAA="raftmemberid"

    val re=db.putState(dbKey,value)

    assert(re)

    val s=db.getState(dbKey)
    assert(s.isDefined)

    assert(s.get.isInstanceOf[AAA])

    assert(s.get.asInstanceOf[AAA]==value)

    db.close
  }

  it should "get an PersistentState MyId" in {
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    type AAA=PersistentState.MyId
    var dbKey =PersistentState.MyIdDBKey
    val value:AAA="myid"

    val re=db.putState(dbKey,value)

    assert(re)

    val s=db.getState(dbKey)
    assert(s.isDefined)

    assert(s.get.isInstanceOf[AAA])

    assert(s.get.asInstanceOf[AAA]==value)

    db.close
  }

  it should "get an PersistentState RaftMembership" in {
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    type AAA=PersistentState.RaftMembership
    var dbKey =PersistentState.RaftMembershipDBKey
    val value:AAA=PersistentState.RaftMembership(1, List("a","b","c"),List("a","d"))

    val re=db.putState(dbKey,value)

    assert(re)

    val s=db.getState(dbKey)
    assert(s.isDefined)

    assert(s.get.isInstanceOf[AAA])

    assert(s.get.asInstanceOf[AAA]==value)

    db.close
  }

  it should "get an PersistentState TermInfo" in {
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    type AAA=PersistentState.TermInfo
    var dbKey =PersistentState.TermInfoDBKey
    val value:AAA=PersistentState.TermInfo(123,"myraftid")

    val re=db.putState(dbKey,value)

    assert(re)

    val s=db.getState(dbKey)
    assert(s.isDefined)

    assert(s.get.isInstanceOf[AAA])

    assert(s.get.asInstanceOf[AAA]==value)

    db.close
  }

  it should "get an PersistentState LeaderCommitIndex" in {
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    type AAA=PersistentState.LeaderCommitIndex
    var dbKey =PersistentState.LeaderCommitIndexDBKey
    val value:AAA=342

    val re=db.putState(dbKey,value)

    assert(re)

    val s=db.getState(dbKey)
    assert(s.isDefined)

    assert(s.get.isInstanceOf[AAA])

    assert(s.get.asInstanceOf[AAA]==value)

    db.close
  }

  it should "get an PersistentState LastAppliedIndex" in {
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    type AAA=PersistentState.LastAppliedIndex
    var dbKey =PersistentState.LastAppliedIndexDBKey
    val value:AAA=345

    val re=db.putState(dbKey,value)

    assert(re)

    val s=db.getState(dbKey)
    assert(s.isDefined)

    assert(s.get.isInstanceOf[AAA])

    assert(s.get.asInstanceOf[AAA]==value)

    db.close
  }

  it should "get an PersistentState LastAppendedIndex" in {
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    type AAA=PersistentState.LastAppendedIndex
    var dbKey =PersistentState.LastAppendedIndexDBKey
    val value:AAA=3

    val re=db.putState(dbKey,value)

    assert(re)

    val s=db.getState(dbKey)
    assert(s.isDefined)

    assert(s.get.isInstanceOf[AAA])

    assert(s.get.asInstanceOf[AAA]==value)

    db.close
  }

  /*
  type PersistentStateDBKey=String
  val RaftMemberIdDBKey:PersistentStateDBKey="RaftMemberId"
  val MyIdDBKey:PersistentStateDBKey= "MyId"
  val RaftMembershipDBKey:PersistentStateDBKey= "RaftMembership"
  val TermInfoDBKey:PersistentStateDBKey= "TermInfo"
  val LeaderCommitIndexDBKey:PersistentStateDBKey= "LeaderCommitIndex"
  val LastAppliedIndexDBKey:PersistentStateDBKey="LastAppliedIndex"
  val LastAppendedIndexDBKey:PersistentStateDBKey= "LastAppendedIndex"

   */
  it should "put and get some PersistentState" in {
    import jkm.cineclub.raft.PersistentState._
    initLogEntryDB(dbName)
    val db: PersistentStateDB=createLogEntryDB(dbName)

    val qq:LeaderCommitIndex=111

    var pairs=Map[PersistentStateDBKey,Any]()
    pairs = pairs + (RaftMemberIdDBKey -> "raftmemberid")
    pairs = pairs + (MyIdDBKey -> "myid")
    pairs = pairs + (RaftMembershipDBKey -> RaftMembership(0, List("ad","bd","qq"),List("qer","dd")))
    pairs = pairs + (TermInfoDBKey -> TermInfo(1233,"myraftid33"))
    pairs = pairs + (LeaderCommitIndexDBKey -> 111.toLong)
    pairs = pairs + (LastAppliedIndexDBKey -> 3412.toLong)
    pairs = pairs + (LastAppendedIndexDBKey -> 53.toLong)


    db.putStates(pairs)

    val list=List(RaftMemberIdDBKey,MyIdDBKey,RaftMembershipDBKey,TermInfoDBKey,LeaderCommitIndexDBKey,LastAppliedIndexDBKey,LastAppendedIndexDBKey)

    val result=db.getStates(list)

    assert(result.size==list.size)

    val keys=result.map(_._1)

    for(key<- list) assert(keys.contains(key))

    for ( item <- result)  assert( Some(pairs(item._1)) == item._2)

    db.close
  }

}
