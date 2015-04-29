package akka.persistence.aerospike.journal

import java.util.concurrent._

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.persistence._
import akka.util.Timeout

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry

case class BenchMsg(data: String)
case class GetCounter()
case class CleanJournal(sequenceNr: Long)

object MyPersistentActor {
	def props(persistenceId: String, snapshotInterval: Int, snapshotSize: Int, maxMessages: Int, debug: Int): Props = Props(new MyPersistentActor(persistenceId,snapshotInterval,snapshotSize,maxMessages,debug))
}

class MyPersistentActor (pid:String, snapshotInterval: Int, snapshotSize: Int, maxMessages: Int, debug: Int) extends PersistentActor {
	
	override def persistenceId = pid
	
	var counter = 0
	var seqNrSnap: Long = 0L
	def journalSize = lastSequenceNr - seqNrSnap
	
	println("init snapshot")
	val baseSnaphot = Array.fill[Byte](snapshotSize-10)('0')
	println("end init snapshot")
	
	def receiveRecover: Receive = {
    	case cmd:BenchMsg => { updateStatus(cmd.data,true) }
    	case SnapshotOffer(metadata, snapshot: Array[Byte]) => { println("snapshot offer size="+snapshot.length); counter = new String(snapshot).toInt; seqNrSnap = metadata.sequenceNr }
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
    	case SaveSnapshotSuccess(metadata) => {
    		deleteSnapshots(SnapshotSelectionCriteria.create(seqNrSnap-1, Long.MaxValue))
    		deleteMessages(seqNrSnap-1)
    	}
	}
	
	def updateStatus (cmd:String, recover:Boolean): Unit = {
        if(!recover) {
          display(cmd,recover);
          counter+=1
          //println(s"$maxMessages")
	      if(journalSize >= snapshotInterval) {
	        //if(counter < maxMessages) {
		        println("SAVE SNAPSHOT")
		        seqNrSnap = lastSequenceNr
		        saveSnapshot(baseSnaphot++String.format("%0"+10+"d", int2Integer(counter)).getBytes())
	        //}
	      }
        } else
          display(cmd,recover);
	}
	
	def display (str: String, recover:Boolean) = {
	  if(recover) {
	    val c = str.substring(str.length()-24,str.length()).toInt
	    if(c%debug==0) {
    	  println("RECOVER:"+str.substring(str.length()-24,str.length()))
	    }
	  } else {
        if(counter%debug==0) {
          println(str.substring(str.length()-24,str.length()))
	    }
      }
	}
}

object PersistentBenchmark extends App { 
	val system = ActorSystem("benchmark")
	
    val config = system.settings.config.getConfig("akka.persistence.benchmark")
    
	implicit val timeout = Timeout(config.getInt("timeout") seconds)
	
  val formatted = String.format("%0"+(config.getInt("sizeMessage")-10)+"d", int2Integer(0))
  
	val processor = system.actorOf(MyPersistentActor.props(config.getString("persistenceId"),config.getInt("snapshotInterval"),
	    config.getInt("snapshotSize"),config.getInt("numMessages"),config.getInt("debug")))
	
	val metrics = new MetricRegistry()
	val messagescounter = metrics.meter("messages")
		
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
	reporter.start(10, TimeUnit.SECONDS)
	
	println("Actor="+processor)
	//val f = processor ? GetCounter()
	var i = 0 //Await.result(f,Duration.Inf).asInstanceOf[Int]
	
	while(i <= config.getInt("numMessages")) {
		val frame = formatted + String.format("%010d", int2Integer(i))
		try {
			val future = processor ? BenchMsg(frame)
			Await.result(future, Duration.Inf).asInstanceOf[String]
			i = i + 1
			messagescounter.mark
		} catch {
		  case e: TimeoutException => e.printStackTrace()
		}	
	}
	
	println("That's all folk")
	reporter.report()
	reporter.stop()
	reporter.close()
}	