package jkm.cineclub.raft.DBWrapper

import akka.actor._
import scala.concurrent.duration._
import scala.Some


/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 9/18/13
 * Time: 11:29 PM
 * To change this template use File | Settings | File Templates.
 */

class ReqRspHandlerDependencyInjector(operator:ActorRef, timeout:Duration,msgBufferSize:Int) extends IndirectActorProducer {
  override def actorClass = classOf[ReqRspHandler]
  override def produce = new ReqRspHandler(operator,timeout,msgBufferSize)
}

class ReqRspHandler(val operator:ActorRef,val timeout:Duration,val msgBufferSize:Int) extends Actor {
  import ReqRspHandler._
  //val timeout:Duration= 5 seconds
  case class Request(requester:ActorRef,req:Req)

  //val operator : ActorSelection = context.actorSelection("adfasfd")
  val requestBuffer=new MsgBuffer[Request](msgBufferSize)

  var requested:Option[Request] =None
  var requestedTime:Long = 1

  def sendReq(request:Request){
    requested=Some(request)
    operator ! request.req
    requestedTime =System.currentTimeMillis()
    context.setReceiveTimeout(timeout)
  }

  def requestedDone() {
    context.setReceiveTimeout(Duration.Undefined)
    requested=None
  }

  def sendUnprocessedRsp(request:Request,cause:Int){
    request.requester ! Rsp(request.req.reqUID,cause)
  }

  def checkReqRspPair(req:Req,rsp:Rsp):Boolean = req.reqUID==rsp.reqUID

  def receive = {
    case req:Req => {
      val requester=sender
      val request=Request(requester,req)

      if (requested.isEmpty) sendReq(request)
      else for (evictedRequest <-requestBuffer.put(request)) sendUnprocessedRsp(evictedRequest,Busy)
    }

    case rsp:Rsp => {
      for (request <- requested ) {
        if (checkReqRspPair(request.req,rsp)) {
          request.requester ! rsp
          requestedDone
          for(request <- requestBuffer.get ) sendReq(request)
        }
      }
    }
    case ReceiveTimeout => {
      //val timeDiffSec=  (System.currentTimeMillis()-requestedTime).toDouble/1000.0  seconds
      val timeDiffMillis=  (System.currentTimeMillis()-requestedTime) millis
      //if (timeDiffMillis >  timeout*0.9 ) {
      val q=5
      if (q >3)
        for(request <- requested ) {
          sendUnprocessedRsp(request,Timeout)
          requestedDone
          for(request <- requestBuffer.get ) sendReq(request)
        }

    }
  }
}

object ReqRspHandler{
  val Ok=0
  val Timeout=1
  val Busy=2
  val Error=3

  class Req(val reqUID:Int)
  class Rsp(val reqUID:Int,val processed:Int)
  object Rsp{
    def apply(reqUID:Int,processed:Int) {
      new Rsp(reqUID,processed)
    }
  }

}
