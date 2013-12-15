package jkm.cineclub.raft.DBWrapper

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 9/19/13
 * Time: 4:42 PM
 * To change this template use File | Settings | File Templates.
 */

class MsgBuffer[T](capacity:Int) {
  var queue= mutable.LinkedList[T]()

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


object MsgBuffer extends App{

   val buffer=new MsgBuffer[String](0)

  val evicted = buffer.put("aaa")
  println(evicted)



}