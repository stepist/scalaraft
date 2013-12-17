package jkm.cineclub.raft.DBWrapper

import scala.collection.mutable.LinkedList

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 9/19/13
 * Time: 4:42 PM
 * To change this template use File | Settings | File Templates.
 */

class MsgBuffer[T](capacity:Int) {
  var queue= LinkedList[T]()

  private def popHead:Option[T] = {
    if (queue.isEmpty) return None
    val re = queue.head
    queue=queue.drop(1)
    Some(re)
  }

  def put(req:T):Option[T] ={
    queue = queue :+ req
    if (queue.size<capacity) None
    else popHead
  }

  def get():Option[T] = {
    popHead
  }

  def isEmpty = queue.isEmpty
}

class MsgBuffer2[T](capacity:Int) {    // capacity > 0
  var queue= List[T]()

  private def popHead:Option[T] =
    queue match {
      case Nil => None
      case h :: t => { queue=t;Some(h)}
    }

  def put(req:T):Option[T] ={
    queue = queue :+ req
    if (queue.size<=capacity) None
    else popHead
  }

  def get():Option[T] = popHead

  def isEmpty = queue.isEmpty
}


object MsgBuffer extends App{

  // val buffer=new MsgBuffer[String](0)

  //val evicted = buffer.put("aaa")
  //println(evicted)



  val buf2=new MsgBuffer2[String](0)
  println(buf2.put("a"))
  println("queue="+buf2.queue)
  println(buf2.put("b"))
  println("queue="+buf2.queue)
  println(buf2.get)
  println("queue="+buf2.queue)

  println
  println

  val buf3=new MsgBuffer2[String](1)
  println(buf3.put("a"))
  println("queue="+buf3.queue)
  println(buf3.put("b"))
  println("queue="+buf3.queue)
  println(buf3.get)
  println("queue="+buf3.queue)





}