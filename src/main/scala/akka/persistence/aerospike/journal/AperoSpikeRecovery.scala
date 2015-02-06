package akka.persistence.aerospike.journal

import scala.concurrent.Future
import akka.persistence.PersistentRepr
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

trait AperoSpikeRecovery { this: AperoSpikeJournal =>
  import config._

  def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    Future { replayMessages(processorId, fromSequenceNr, toSequenceNr, max)(replayCallback) }

  def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] =
    Future { readHighestSequenceNr(processorId, fromSequenceNr ) }

  def readHighestSequenceNr(processorId: String, fromSequenceNr: Long): Long =
    new MessageIterator(processorId, math.max(1L, fromSequenceNr), Long.MaxValue, Long.MaxValue).foldLeft(fromSequenceNr) { case (acc, msg) => msg.sequenceNr }

  def readLowestSequenceNr(processorId: String, fromSequenceNr: Long): Long =
    new MessageIterator(processorId, fromSequenceNr, Long.MaxValue, Long.MaxValue).find(!_.deleted).map(_.sequenceNr).getOrElse(fromSequenceNr)

  def replayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit =  
    new MessageIterator(processorId, fromSequenceNr, toSequenceNr, max).foreach(replayCallback)


  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends scala.collection.Iterator[PersistentRepr] {
    //println(debugprfx+" processorId="+processorId+" fromSequenceNr="+fromSequenceNr +" toSequenceNr="+toSequenceNr+ " max="+max)
    import PersistentRepr.Undefined

    private val iter = new RecordIterator(processorId, fromSequenceNr, toSequenceNr)
    private var mcnt = 0L

    private var c: PersistentRepr = null
    private var n: PersistentRepr = PersistentRepr(Undefined)

    fetch()

    def hasNext: Boolean = {
      //println("\t"+debugprfx+" hasNext n="+n+" mcnt="+mcnt+" returns "+(n != null && mcnt < max))
      n != null && mcnt < max
    }
    
    def next(): PersistentRepr = {
      fetch()
      mcnt += 1
      //println("\tnext="+c)
      c
    }

    /**
     * Make next message n the current message c, complete c
     * (ignoring orphan markers) and pre-fetch new n.
     */
    private def fetch(): Unit = {
      c = n
      n = null
      //println("\t"+debugprfx+" fetch c="+c)
      while (iter.hasNext && n == null) {
        val row = iter.next()
        val marker = keymsgmarker(row._1)
        val snr = keymsgsequence(row._1)
        //println("\tmarker="+marker+" snr="+snr+" row="+row)
        if (marker == "A") {
          val m = persistentFromByteBuffer(row._2("payload"))
          n = m
          //println("\t\t"+debugprfx+" m="+m+" c="+c+" n="+n)
        } else if (marker == "B" && c.sequenceNr == snr) {
          c = c.update(deleted = true)
        } else if (c.sequenceNr == snr) {
          val channelId = new String(row._2("payload"))
          println("channelId="+channelId)
          c = c.update(confirms = channelId.split(":").toList)
        }
      }
    }
  }

  
  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RecordIterator(processorId: String, fromSequenceNr: Long, toSequenceNr: Long) extends scala.collection.Iterator[(String, Map[String, Array[Byte]])] {
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
    /*println("RecordIterator processorId="+processorId+" fromSequenceNr="+fromSequenceNr +" toSequenceNr="+toSequenceNr)
    
    val seqinterval = 100
    var currentSnr = fromSequenceNr
    
    var toSnr = math.min(currentSnr+seqinterval-1, toSequenceNr)
    
    var currentrec = 0

    val bins = Seq ("payload")
	
    var keyset = keySeq(currentSnr,toSnr)
    //println("keySet="+keyset)
    
    var records = if (toSequenceNr >= fromSequenceNr) awaitrecords(messagesns.multiGetBinsL(keyset,bins)) else List.empty
    
    /**
     * Build a set of string containing keys
     * @param fromSequence
     * @param toSequence
     * @return
     */
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
      if(records.size == 0) {
        false
      } else if(currentrec >  seqinterval) {
        
      }
      /*if(records.size==0) {
        false
      } else if (currentrec < records.size) { //inside a sequence interval
        // more entries available
        //println("inside a sequence interval");
        true
      } else if (currentrec >= records.size && currentSnr >= toSequenceNr ) { //outside sequence
        // no more entries available
        //println("outside sequence");
        false
      } else if (currentrec >= records.size && currentSnr >= toSnr ) { // outside a sequence interval...test the next sequence interval
        //println("outside a sequence interval:["+currentrec+","+currentSnr+","+toSnr+"]");
        currentSnr += 1
        toSnr = math.min(currentSnr+seqinterval-1, toSequenceNr)
        currentrec = 0
        keyset = keySeq(currentSnr,toSnr)
        //println("keySet="+keyset)
        
        records = awaitrecords(messagesns.multiGetBinsL(keyset,bins)) 
        //println("records="+records)
        if(records.size==0)
          false
        else if(records.size < seqinterval)
          true
        else 
          hasNext
      } else { //Not outside on the sequence interval
        if(currentrec >= records.size) //But outside the record size
          false
        else {  
          currentSnr += 1
          hasNext
        }
      }*/
    }

    def next(): (String, Map[String, Array[Byte]]) = {
      //println("currentrec="+currentrec)
      var row = records(currentrec)
      currentrec += 1
      row
    }

  }*/
}