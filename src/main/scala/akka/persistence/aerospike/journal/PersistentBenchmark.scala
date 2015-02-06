package akka.persistence.aerospike.journal

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.persistence.SnapshotOffer
import akka.util.Timeout

case class BenchMsg(data: String)
case class GetCounter()

object MyPersistentActor {
	def props(snapshotInterval: Int, snapshotSize: Int, maxMessages: Int): Props = Props(new MyPersistentActor(snapshotInterval,snapshotSize,maxMessages))
}

class MyPersistentActor (snapshotInterval: Int, snapshotSize: Int, maxMessages: Int) extends PersistentActor {
	
	override def persistenceId = "my-persistent-actor"
	
	var counter = 0
	
	def receiveRecover: Receive = {
    	case cmd:String => { updateStatus(cmd,true) }
    	case SnapshotOffer(_, snapshot: String) => counter = snapshot.toInt
	}
	
	def receiveCommand: Receive = {
    	case cmd:BenchMsg =>
    	  persist(cmd) { c => 
    		updateStatus(c.data,false)
    	  }
    	  sender() ! "OK"
    	case gc:GetCounter => {
    	  println("GetCounter")
    	  sender() ! counter
    	}
	}
	
	def updateStatus (cmd:String, recover:Boolean): Unit = {
        if(!recover) {
          display(cmd,recover);
          counter+=1; 
          //println(s"$maxMessages")
	      if(counter%snapshotInterval==0) {
	        //if(counter < maxMessages) {
		        println("SAVE SNAPSHOT")
		        saveSnapshot(String.format("%0"+snapshotSize+"d", int2Integer(counter)))
		    	//deleteMessages(getCurrentPersistentMessage.sequenceNr-1,true)
	        //}
	      }
        } else
          display(cmd,recover);
	}
	
	def display (str: String, recover:Boolean) = {
	  if(recover)
    	println("RECOVER:"+str.substring(str.length()-24,str.length()))
      else
        println(str.substring(str.length()-24,str.length()))
	}
}

object PersistentBenchmark extends App { 
	val system = ActorSystem("benchmark")
	
    val config = system.settings.config.getConfig("akka.persistence.benchmark")
    
	implicit val timeout = Timeout(config.getInt("timeout") seconds)
	
	val processor = system.actorOf(MyPersistentActor.props(config.getInt("snapshotInterval"),config.getInt("snapshotSize"),config.getInt("numMessages")))
	
	val f = processor ? GetCounter()
	var i = Await.result(f,timeout.duration).asInstanceOf[Int]
	
	while(i <= config.getInt("numMessages")) {
		val formatted = String.format("%0"+config.getInt("sizeMessage")+"d", int2Integer(i))
		try {
			val future = processor ? BenchMsg(formatted)
			val result = Await.result(future, timeout.duration).asInstanceOf[String]
			i = i + 1
		} catch {
		  case e: TimeoutException => e.printStackTrace()
		}
		
	}
	
	system.awaitTermination
}	