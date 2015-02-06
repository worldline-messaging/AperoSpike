package akka.persistence.aerospike.journal

import java.util.concurrent.TimeUnit
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.tapad.aerospike._
import com.tapad.aerospike.DefaultValueMappings._
import scala.concurrent.ExecutionContext.Implicits.global
import com.aerospike.client.Value
import scala.concurrent._
import scala.util.{Failure, Success}
import scala.concurrent.duration._


    
  class RecordIterator(messagesns: AsSetOps[String,Array[Byte]],processorId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[Map[String, Array[Byte]]] {
    val seqinterval = 100
    val nbseq = ((toSequenceNr-fromSequenceNr)/seqinterval)+1
    
    var currentSnr = fromSequenceNr
    val bins = Seq ("payload")
    
    var toSnr = math.min(currentSnr+seqinterval-1, toSequenceNr)
    var currentrec = 0
    var keyset = keySeq(currentSnr,toSnr)
    var records = if (toSequenceNr >= fromSequenceNr) awaitrecords(messagesns.multiGetBinsL(keyset,bins)) else List.empty
    
    private def keySeq(fromSequence: Long, toSequence: Long): Seq[String] = {
      (fromSequence to toSequence).map {
        i => List(genmsgkey(processorId,i,"A"), genmsgkey(processorId,i,"B"), genmsgkey(processorId,i,"C"))
      }.flatten.toSeq
    }
    
    /**
     * Await the results for seqinterval.NB: The cassandra implementation is also synchronous in the Async
     * Filter only the real records (containing the payload field)
     * @param frecords
     * @return
     */
    private def awaitrecords(frecords: Future[List[(String, Map[String, Array[Byte]])]]): List[(String, Map[String, Array[Byte]])] = {
      //println("awaitrecords")
      val r = Await.result(frecords, Duration.Inf).filter(r => r._2.contains("payload"))
      //println("end of awaitrecords:"+r)
      r
    }
    
    @annotation.tailrec
    final def hasNext: Boolean = {
      if (currentrec < records.size) { //inside a sequence interval
        println("Inside sequence")
        true
      } else { //we end the current sequence
        println("Outside sequence")
        currentSnr += seqinterval
        if(currentSnr > toSequenceNr) {
          println("EOI")
          false
        } else {
	      toSnr = math.min(currentSnr+seqinterval-1, toSequenceNr)
	      currentrec = 0
	      println("Next sequence:"+currentSnr+" to:"+toSnr)
		  keyset = keySeq(currentSnr,toSnr)
		  records = awaitrecords(messagesns.multiGetBinsL(keyset,bins))
	      hasNext
        }
      } 
    }

    def next(): Map[String, Array[Byte]] = {
      var row = records(currentrec)
      currentrec += 1
      row._2
    }
  }

object TestAeroSpike4 extends App {
  
	/*implicit val mapping = new ValueMapping[String] {
      def toAerospikeValue(s: String): Value = Value.get(s)
      def fromStoredObject(obj: Object): String = obj.toString()
    }*/
	
	var sequences: Map[String, Int] = Map.empty
	
	def seqnumbernext (persistID:String) : Int = {
		val sn: Int = sequences.getOrElse(persistID, 0) + 1 
		sequences = sequences + (persistID ->  sn)
		sn
	}
	
	println("Asynchronous Test")
	val metrics = new MetricRegistry();
	val messagesin = metrics.meter("messagesin");
	val messagesout = metrics.meter("messagesout");
	
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
	val client = AerospikeClient(Seq("localhost"))
    val messagesns = client.namespace("Akka").set[String,Array[Byte]]("messages")

	val r = scala.util.Random
	
    val persistenceID = "persistenceID0xDEADBEEF"
    
    println("Inserting elements");
	reporter.start(10, TimeUnit.SECONDS)
    for( i <- 0 to 948){
    	val sn = seqnumbernext(persistenceID)
        val key = genmsgkey(persistenceID,sn,"A")
    	
    	val bins : Map[String, Array[Byte]] = Map ("payload" -> String.format("%0"+1024+"d", int2Integer(i)).getBytes)
    	val write : Future[Unit] = messagesns.putBins(key,bins)
    	write.onComplete {
    	  case Failure(ex)      => ex.printStackTrace()
    	  case Success(value)   => messagesout.mark() 
    	}
    	Await.result(write, 10 seconds)
    }
	
	println("Reading elements");
	
	var cnt = -1
	val iter = new RecordIterator(messagesns,persistenceID,1,948)
	while(iter.hasNext) {
	  val m = iter.next
	  val payload = new String(m("payload"))
	  println(payload)
	  messagesin.mark() 
	  if(m.contains("payload")) {
	    val c = payload.toInt
		if((c-cnt) != 1) {
		  System.err.println("Invalid counter")
		  System.exit(1)
		}
	    cnt=c
	  } 
	}
	if(cnt==999999)
		println("COUNTER IS OK")
	else
		System.err.println("COUNTER IS NOT OK")
	reporter.report()
	reporter.stop()
	reporter.close()	
}
