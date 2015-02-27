package akka.persistence.aerospike.journal

import java.util.concurrent.TimeUnit
import com.tapad.aerospike._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import com.aerospike.client.Value
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.aerospike.client.async.MaxCommandAction
import com.tapad.aerospike.DefaultValueMappings._

object Test1 extends App {
	
  def splitArray[T](xs: Array[T], splitSize: Int): Seq[(Int,Array[T])] = {
    var i = -1
    (0 to xs.length-1 by splitSize).map { j =>
      val rest = math.min(splitSize,xs.length-j)
      println("j="+j+" rest="+rest)
      ({i +=1;i},xs.slice(j, j+rest))
    }
  }
  
  val array = Array(0,1,2,3,4,5,6,7,8,9,10)
  
  splitArray(array, 3).foreach { a =>
    print(a._1+":"); a._2.foreach(e => print(e+",")); println
  }
  
  String.format("%0"+1000000+"d", int2Integer(0))
}