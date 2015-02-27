package akka.persistence.aerospike.journal

import scala.concurrent.Future
import akka.persistence.PersistentRepr
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.nio.ByteBuffer

/**
 * @author a140168
 * The recovery part of the journal
 *
 */
trait AperoSpikeRecovery { this: AperoSpikeJournal =>
  import config._

  /**
   * Replay messages between an interval of sequence numbers
   * @param processorId
   * @param fromSequenceNr
   * @param toSequenceNr
   * @param max
   * @param replayCallback
   * @return a future
   */
  def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    Future { replayMessages(processorId, fromSequenceNr, toSequenceNr, max)(replayCallback) }

  /**
   * Read the highest sequence number
   * @param processorId
   * @param fromSequenceNr
   * @return
   */
  def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] =
    Future { readHighestSequenceNr(processorId, fromSequenceNr ) }

  /**
   * Read the highest sequence number for an actor
   * Take the last item of the iterator
   * @param processorId
   * @param fromSequenceNr
   * @return
   */
  def readHighestSequenceNr(processorId: String, fromSequenceNr: Long): Long =
    new MessageIterator(processorId, math.max(1L, fromSequenceNr), Long.MaxValue, Long.MaxValue, config.seqinterval).foldLeft(fromSequenceNr) { case (acc, msg) => msg.sequenceNr }

  /**
   * @param processorId
   * @param fromSequenceNr
   * @param toSequenceNr
   * @param max
   * @param replayCallback
   */
  def replayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit =  
    new MessageIterator(processorId, fromSequenceNr, toSequenceNr, max,config.seqinterval).foreach(replayCallback)
    
  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, seqinterval:Long) extends scala.collection.Iterator[PersistentRepr] {
    println("processorId="+processorId+" fromSequenceNr="+fromSequenceNr +" toSequenceNr="+toSequenceNr+ " max="+max)
    import PersistentRepr.Undefined

    private val iter = new RecordIterator(processorId, fromSequenceNr, toSequenceNr,seqinterval)
    private var mcnt = 0L

    private var c: PersistentRepr = null
    
    //fetch()

    def hasNext: Boolean = {
      mcnt < max && iter.hasNext
    }
    
    def next(): PersistentRepr = {
      fetch()
      mcnt += 1
      //println("\tnext="+c)
      c
    }

    private def fetch(): Unit = {
      
      val row = iter.next()
      val marker = keymsgmarker(row._1)
      val snr = keymsgsequence(row._1)
        
      //println (row)
      c = persistentFromByteBuffer(row._2("payload"))
        
      if (row._2.contains("confirm")) {
	    val channelId = new String(row._2("confirm"))
	    println("channelId="+channelId)
	    c = c.update(confirms = channelId.split(":").toList)
      }
        
      if(row._2.contains("deleted")) {
        println("logically delete "+snr) 
        c = c.update(deleted = true)
      }
    }
  }

  
  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RecordIterator(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, seqinterval:Long) extends scala.collection.Iterator[(String, Map[String, Array[Byte]])] {
    val nbseq = ((toSequenceNr-fromSequenceNr)/seqinterval)+1
    
    val lowest = Await.result(asyncReadLowestSequenceNr(processorId, 1L),Duration.Inf)
    var currentSnr = if(lowest > fromSequenceNr) lowest else fromSequenceNr
    
    val bins = Seq ("payload","confirm","deleted")
    
    var toSnr = math.min(currentSnr+seqinterval-1, toSequenceNr)
    var currentrec = 0
    var keyset = keySeq(currentSnr,toSnr)
    var records = if (toSequenceNr >= fromSequenceNr) awaitrecords(messagesns.multiGetBinsL(keyset,bins)) else List.empty
    
    private def keySeq(fromSequence: Long, toSequence: Long): Seq[String] = {
      (fromSequence to toSequence).map {
        i => List(genmsgkey(processorId,i,"A")
            /*genmsgkey(processorId,i,"B") 
            genmsgkey(processorId,i,"C")*/)
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
      if(records.size == 0) {
        false
      } else if (currentrec < records.size) { //inside a sequence interval
        //println("Inside sequence")
        true
      } else { //we end the current sequence
        //println("Outside sequence")
        currentSnr += seqinterval
        if(currentSnr > toSequenceNr) {
          //println("EOI")
          false
        } else {
	      toSnr = math.min(currentSnr+seqinterval-1, toSequenceNr)
	      currentrec = 0
	      //println("Next sequence:"+currentSnr+" to:"+toSnr)
		  keyset = keySeq(currentSnr,toSnr)
		  records = awaitrecords(messagesns.multiGetBinsL(keyset,bins))
	      hasNext
        }
      } 
    }

    def next(): (String, Map[String, Array[Byte]]) = {
      var row = records(currentrec)
      currentrec += 1
      row
    }
  }
}