package akka.persistence.aerospike.journal

import java.util.concurrent.TimeUnit
import com.tapad.aerospike._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import com.aerospike.client.Value
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.aerospike.client.async.MaxCommandAction
import com.tapad.aerospike.DefaultValueMappings._
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.large.LargeSet
import com.aerospike.client.Key

object TestAeroSpikeLDT1 extends App {
	
	implicit val mapping = new ValueMapping[String] {
      def toAerospikeValue(s: String): Value = Value.get(s)
      def fromStoredObject(obj: Object): String = obj.toString()
    }
	
	val metrics = new MetricRegistry();
	val messagescounter = metrics.meter("messages");
		
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
    val client = new AsyncClient("127.0.0.1", 3000)

    reporter.start(10, TimeUnit.SECONDS)
	
    for( i <- 0 to 2000){
        val set = client.getLargeSet(null, new Key("testLDT", "largeset", "largeset"+i), "messages", null)
    	for( j <- 0 to 600){
    		set.add(Value.get(String.format("%0"+4096+"d", int2Integer(j)).getBytes()))
    	}
    	/*val write : Future[Unit] = test1.put("putkey"+i, )
    	write.onComplete {
    	  case Failure(ex)      => ex.printStackTrace()
    	  case Success(value)   => messagescounter.mark() 
    	}
    	//Await.result(write, 10 seconds)*/
    }
	
	reporter.report()
	reporter.stop()
	reporter.close()
	
}