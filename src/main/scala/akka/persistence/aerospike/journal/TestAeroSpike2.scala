package akka.persistence.aerospike.journal

import java.util.concurrent.TimeUnit
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.aerospike.client.AerospikeException
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.listener.WriteListener
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

object TestAeroSpike2 extends App {
	println("Asynchronous Test")
	
	val metrics = new MetricRegistry();
	val messagescounter = metrics.meter("messages");
		
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
    val client = new AsyncClient("127.0.0.1", 3000)

    reporter.start(10, TimeUnit.SECONDS)
	
    val wh = new WriteHandler()
	
    for( i: Long <- 0L to 99999999L){
        val key = new Key("Akka", "Test1", "putkey"+i)
        val bin1 = new Bin("bin1", String.format("%0"+1024+"d", long2Long(i)))
        client.put(null, wh, key, bin1)
        if(i>=100000 && i%50000==0) {
          println("Starting Delete from "+(i-100000)+" to "+(i-50000))
          val f = Future {
	          for(j:Long <- i-100000 to i-50000) {
	            val key2 = new Key("Akka", "Test1", "putkey"+j)
	            client.delete(null,key2)
	          }
          } onComplete {
            case Success(_) => println("Delete from "+(i-100000)+" to "+(i-50000)+" OK")
            case Failure(e) => println("Delete from "+(i-100000)+" to "+(i-50000)+" FAILED"); e.printStackTrace
          }
        }
    }
	
	reporter.report()
	reporter.stop()
	reporter.close()

	class WriteHandler extends WriteListener {
      override def onSuccess(key: Key) { 
        messagescounter.mark()
      }
      override def onFailure(e: AerospikeException) {
        e.printStackTrace()
      }
	}
}
