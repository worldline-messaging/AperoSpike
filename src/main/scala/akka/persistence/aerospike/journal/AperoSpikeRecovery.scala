package akka.persistence.aerospike.journal

import scala.concurrent.Future
import akka.persistence.PersistentRepr
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * @author a140168
 * The recovery part of the journal
 *
 */
trait AperoSpikeRecovery { this: AperoSpikeJournal =>
  import config._

  //This variable will be used to consolidate the state of the messages in the journal
  var deletions: Map[String, (Long, Boolean)] = Map.empty
  
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
   * Read the lowest sequence number for an actor
   * The first message that is not deleted
   * @param processorId
   * @param fromSequenceNr
   * @return
   */
  def readLowestSequenceNr(processorId: String, fromSequenceNr: Long): Long =
    new MessageIterator(processorId, fromSequenceNr, Long.MaxValue, Long.MaxValue,config.seqinterval).find(!_.deleted).map(_.sequenceNr).getOrElse(fromSequenceNr)

  /**
   * @param processorId
   * @param fromSequenceNr
   * @param toSequenceNr
   * @param max
   * @param replayCallback
   */
  def replayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit =  {
    //take the actual deletions
    val deletions = this.deletions
    //take the last deletion for this actor and the type of deletion (permanent/logical)
    val (deletedTo, permanent) = deletions.getOrElse(processorId, (0L, false))
    //if we're in permanent deletion, we start at last deleted + 1. Else we start at fromSequenceNr
    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    //count the number of messages
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    //take in account the max parameter
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr
    
    //recreate iterator updating delete status and replay messages that are in the interval
    new MessageIterator(processorId, fromSequenceNr, toSequenceNr, max, config.seqinterval).map(p => 
      if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(adjustedFrom) {
        case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) replayCallback(p); p.sequenceNr
      }
  }

  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, seqinterval:Long) extends scala.collection.Iterator[PersistentRepr] {
    //println(debugprfx+" processorId="+processorId+" fromSequenceNr="+fromSequenceNr +" toSequenceNr="+toSequenceNr+ " max="+max)
    import PersistentRepr.Undefined

    private val iter = new RecordIterator(processorId, fromSequenceNr, toSequenceNr,seqinterval)
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
          //println("channelId="+channelId)
          c = c.update(confirms = channelId.split(":").toList)
        }
      }
    }
  }

  
  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RecordIterator(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, seqinterval:Long) extends scala.collection.Iterator[(String, Map[String, Array[Byte]])] {
    val nbseq = ((toSequenceNr-fromSequenceNr)/seqinterval)+1
    
    var currentSnr = fromSequenceNr
    val bins = Seq ("payload")
    
    var toSnr = math.min(currentSnr+seqinterval-1, toSequenceNr)
    var currentrec = 0
    var keyset = keySeq(currentSnr,toSnr)
    var records = if (toSequenceNr >= fromSequenceNr) awaitrecords(messagesns.multiGetBinsL(keyset,bins)) else List.empty
    
    private def keySeq(fromSequence: Long, toSequence: Long): Seq[String] = {
      (fromSequence to toSequence).map {
        i => List(genmsgkey(processorId,i,"A"),
            /* TODO B marker will be used when the deletion wont be based on the LRU of areospike: genmsgkey(processorId,i,"B") ,*/ 
            genmsgkey(processorId,i,"C"))
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