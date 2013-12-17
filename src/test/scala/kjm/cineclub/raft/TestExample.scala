package kjm.cineclub.raft

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/15/13
 * Time: 6:06 PM
 * To change this template use File | Settings | File Templates.
 */




import collection.mutable.Stack
import org.scalatest._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File
import org.iq80.leveldb.Options
import jkm.cineclub.raft.DB.{LogEntryLevelDB, LogEntryDB,LogEntryMemroyDB}


class LogEntryDBSpec extends FlatSpec with Matchers {

  /*
  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  } */

  def initLogEntryDB(dbName:String) {
    /*
    try{
      factory.destroy(new File(dbName), new Options())
    }catch{
      case e:Exception => println(e)
    }*/
  }
  def createLogEntryDB(dbName:String) : LogEntryDB ={
    //new LogEntryLevelDB(dbName)
    new LogEntryMemroyDB()
  }

  it should "append an LogEntry " in {
    import LogEntryDB._
    val dbName="TestLevelDB2"
    initLogEntryDB(dbName)
    val logEntryDB:LogEntryDB= createLogEntryDB(dbName)

    for(i <- 1 to 10) logEntryDB.appendEntry(LogEntry(i,i+1,"test"+i))

    for(i <- 1 to 10){
      val expectedResult=  Some(LogEntry(i,i+1,"test"+i))
      assertResult(expectedResult) {
        logEntryDB.getEntry(i)
      }
    }

    logEntryDB.close
  }


  it should "append  LogEntryes " in {
    import LogEntryDB._
    val dbName="TestLevelDB2"
    initLogEntryDB(dbName)
    val logEntryDB:LogEntryDB= createLogEntryDB(dbName)

    var logEntries:List[LogEntry] = (20 to 30).map(i => LogEntry(i,i+1,"test"+i)).toList
    logEntryDB.appendEntries(logEntries)

    for(i <- 20 to 30){
      val expectedResult=  Some(LogEntry(i,i+1,"test"+i))
      assertResult(expectedResult) {
        logEntryDB.getEntry(i)
      }
    }

    logEntryDB.close
  }


  it should "get last index " in {
    import LogEntryDB._
    val dbName="TestLevelDB2"
    initLogEntryDB(dbName)
    val logEntryDB:LogEntryDB= createLogEntryDB(dbName)

    val lastIndex:Long=50

    for(i <- 30 to lastIndex.toInt) logEntryDB.appendEntry(LogEntry(i,i+1,"test"+i))

    assertResult(      Some(lastIndex)        ) {
      logEntryDB.getLastIndex()
    }

    logEntryDB.close
  }

  it should "get last entry " in {
    import LogEntryDB._
    val dbName="TestLevelDB2"
    initLogEntryDB(dbName)
    val logEntryDB:LogEntryDB= createLogEntryDB(dbName)

    val lastIndex:Int=50

    for(i <- 30 to lastIndex) logEntryDB.appendEntry(LogEntry(i,i+1,"test"+i))

    assertResult(      Some(LogEntry(lastIndex,lastIndex+1,"test"+lastIndex))        ) {
      logEntryDB.getLast()
    }

    logEntryDB.close
  }

  it should "get last N entry " in {
    import LogEntryDB._
    val dbName="TestLevelDB2"
    initLogEntryDB(dbName)
    val logEntryDB:LogEntryDB= createLogEntryDB(dbName)

    val lastIndex:Int=50
    val n:Int=5

    for(i <- 20 to lastIndex) logEntryDB.appendEntry(LogEntry(i,i+1,"test"+i))

    val lastN=logEntryDB.getLastN(n)
    println(lastN)

    assert(lastN.length == n)

    for( i<- 0 to n-1 ) {
      val ri=lastIndex-i
      assert( lastN(i) ==  Some(LogEntry(ri,ri+1,"test"+ri) ))
    }

    logEntryDB.close
  }


  it should "get last N entry form some index" in {
    import LogEntryDB._
    val dbName="TestLevelDB2"
    initLogEntryDB(dbName)
    val logEntryDB:LogEntryDB= createLogEntryDB(dbName)

    val lastIndex:Int=50
    val fromIndex:Long=40
    val n:Int=5

    for(i <- 20 to lastIndex) logEntryDB.appendEntry(LogEntry(i,i+1,"test"+i))

    val lastNfrom=logEntryDB.getLastNFrom(n,fromIndex)

    assert(lastNfrom.length == n)

    for( i<- 0 to n-1 ) {
      val ri=fromIndex.toInt-i
      assert( lastNfrom(i) ==  Some(LogEntry(ri,ri+1,"test"+ri) ))
    }

    logEntryDB.close
  }




  it should "ooooo " in {
    import LogEntryDB._
    val dbName="TestLevelDB2"
    initLogEntryDB(dbName)
    val logEntryDB:LogEntryDB= createLogEntryDB(dbName)

    logEntryDB.close
  }



  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    }
  }
}