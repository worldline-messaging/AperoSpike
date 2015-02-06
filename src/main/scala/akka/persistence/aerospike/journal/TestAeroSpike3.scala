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
import com.aerospike.client.AerospikeClient

object TestAeroSpike3 extends App {
	
	println("Synchronous Test")
	val metrics = new MetricRegistry();
	val messagescounter = metrics.meter("messages");
		
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
    val client = new AerospikeClient("127.0.0.1", 3000)

    reporter.start(10, TimeUnit.SECONDS)
	
    for( i <- 0 to 999999){
        val key = new Key("Akka", "Test1", "putkey"+i)
        val bin1 = new Bin("bin1", String.format("%0"+1024+"d", int2Integer(i)))
        client.put(null, key, bin1)
        messagescounter.mark()
    }
	
	reporter.report()
	reporter.stop()
	reporter.close()
}
