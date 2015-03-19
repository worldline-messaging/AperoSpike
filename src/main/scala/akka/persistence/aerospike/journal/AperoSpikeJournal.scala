package akka.persistence.aerospike.journal

import akka.persistence.journal.AsyncWriteJournal
import scala.concurrent._
import akka.persistence.PersistentRepr
import com.tapad.aerospike._
import com.tapad.aerospike.DefaultValueMappings._
import com.aerospike.client.Value
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.Config
import scala.collection.JavaConversions._
import akka.serialization.SerializationExtension
import akka.persistence.PersistentConfirmation
import scala.collection.generic.CanBuildFrom
import java.nio.ByteBuffer
import scala.concurrent.duration.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

/**
 * The aperospike configuration file for the journal
 * 
 */
class AperoSpikeJournalConfig(config: Config) {
	val urls = config.getStringList("urls")
	val namespace = config.getString("namespace")
	val set = config.getString("set")
	val ttl = config.getInt("ttl")
	val ttlDeleteNP = config.getInt("ttlDeleteNP")
	val seqinterval = config.getLong("seqinterval")
	val selectorThreads = config.getInt("selectorThreads")
	val touchOnDelete = config.getBoolean("touchOnDelete")
}

/**
 * The aperospike journal.
 * This implementation is not complete and is based on the LRU of areospike to manage the deletion.   
 */
class AperoSpikeJournal extends AsyncWriteJournal with AperoSpikeRecovery {
	val serialization = SerializationExtension(context.system)
	
	val config = new AperoSpikeJournalConfig(context.system.settings.config.getConfig("aperospike-journal"))
	
	val asyncTaskThreadPool = Executors.newCachedThreadPool(new ThreadFactory() {
		override def newThread(runnable: Runnable) = {
                        val thread = new Thread(runnable)
                        thread.setDaemon(true)
                        thread
                }
    })
	
	val client = AerospikeClient(config.urls,new ClientSettings(blockingMode=true,selectorThreads=config.selectorThreads, taskThreadPool=asyncTaskThreadPool))
	
    val messagesns = client.namespace(config.namespace).set[String,Array[Byte]](config.set)
    
	/**
	 * Write a sequence of messages
	 * @param messages
	 * @return A future
	 */
	def asyncWriteMessages(messages: scala.collection.immutable.Seq[PersistentRepr]): Future[Unit] = {
	  val puts = messages.map { m =>
	    val key = genmsgkey(m.persistenceId,m.sequenceNr,"A")
	    val bins : Map[String, Array[Byte]] = Map ("payload" -> serialization.serialize(m).get)
	    messagesns.putBins(key, bins, Option(config.ttl))
	  }
	  Future.sequence(puts).map(_ => ())
	}
	
	/**
	 * Make a iterable of items be treated by futures, but serialize these treatments in the order of the collection.
	 * It's like flatMap { l(0) } flatMap { l(1) } flatMap { l(2) } .... flatMap { l(n-1) }
	 * http://www.michaelpollmeier.com/execute-scala-futures-in-serial-one-after-the-other-non-blocking/
	 * @param l
	 * @param fn
	 * @param ec
	 * @return
	 */
	def serialiseFutures[A, B](l: Iterable[A])(fn: A ⇒ Future[B])
    (implicit ec: ExecutionContext): Future[List[B]] =
      l.foldLeft(Future(List.empty[B])) {
        (previousFuture, next) =>
        for {
          previousResults <- previousFuture
          next <- fn(next)
        } yield previousResults :+ next
      }
	
	/**
	 * Write a confirmation
	 * If the record does not exist, create it.
	 * If the record exists, update it.
	 * The channel id is saved in the payload.
	 * @param confirmation
	 * @return a future
	 */
	def asyncWriteConfirmation (confirmation: PersistentConfirmation): Future[Unit] = {
	  val key = genmsgkey(confirmation.persistenceId,confirmation.sequenceNr,"A")
	  messagesns.getBins(key, Seq("confirm")).flatMap {
	    r => {
	      if(!r.contains("confirm")) {
	        println("Confirm is empty ("+confirmation.persistenceId+","+confirmation.sequenceNr+")")
            val bins : Map[String, Array[Byte]] = Map ("confirm" -> confirmation.channelId.getBytes())
            messagesns.putBins(key, bins)
          } else {
            println("Confirm:"+new String(r("confirm"))+" ("+confirmation.persistenceId+","+confirmation.sequenceNr+")")
            val bins : Map[String, Array[Byte]] = Map ("confirm" -> (new String(r("confirm"))+":"+confirmation.channelId).getBytes())
            messagesns.putBins(key, bins)
          }
	    }
	  }
	}
	
	/**
	 * Write a sequence of confirmation
	 * @param confirmations
	 * @return a future
	 */
	def asyncWriteConfirmations(confirmations: scala.collection.immutable.Seq[PersistentConfirmation]): Future[Unit] = {
	  serialiseFutures(confirmations)(asyncWriteConfirmation).map(_ => ())
    }
	
	/**
	 * @param messageIds
	 * @param permanent
	 * @return a future
	 */
	def asyncDeleteMessages(messageIds: scala.collection.immutable.Seq[akka.persistence.PersistentId],permanent: Boolean): Future[Unit] = {
	  Future.failed(new UnsupportedOperationException("Individual deletions not supported"))
	}
	
	/**
	 * Save the deletions in memory. 
	 * @see AperoSpikeRecovery
	 * @param processorId
	 * @param toSequenceNr
	 * @param permanent
	 * @return a future
	 */
	def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
	  asyncReadLowestSequenceNr(processorId, 1L).flatMap { lowest =>
		asyncDeleteMessagesFromTo(processorId,lowest,toSequenceNr,permanent)
	  }
    }
    
    /**
     * 
     */
	def asyncDeleteMessagesFromTo(processorId: String, fromSequenceNr:Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
	  println("Delete from="+fromSequenceNr+" to="+toSequenceNr+" for "+processorId)
	  if(permanent) {
	    /*val keys = (fromSequenceNr to toSequenceNr).map(sequence => Seq(genmsgkey(processorId,sequence,"A"),
	        genmsgkey(processorId,sequence,"B"), Do not need to delete the B marker
	        genmsgkey(processorId,sequence,"C"))).flatten*/
	    if(config.touchOnDelete) {
	      val keys = (fromSequenceNr to toSequenceNr).map(sequence => genmsgkey(processorId,sequence,"A"))
	      val deletes = keys.map(key => messagesns.touch(key,Option(1))) //Ttl at 1 second, so the evictor can do immediatly his job
		  Future.sequence(deletes).map(_ => ()).flatMap {
	        case _ => asyncWriteLastDeletedSequenceNr(processorId,toSequenceNr)
	      }
	    } else {
	      asyncWriteLastDeletedSequenceNr(processorId,toSequenceNr)
	    }
	  } else {
	    println("logical delete for "+processorId+ " from="+fromSequenceNr+" to="+toSequenceNr)
	    val keys = (fromSequenceNr to toSequenceNr).map(sequence => genmsgkey(processorId,sequence,"A"))
	    val puts = keys.map(key => messagesns.putBins(key, 
		    Map ("deleted" ->Array('L')), 
		    Option(config.ttlDeleteNP)))
	    Future.sequence(puts).map(_ => ())
	  }
	}
	
	/**
     * Read the lowest sequence number for an actor
     * The first message that is not deleted
     * @param processorId
     * @param fromSequenceNr
     * @return
     */
    def asyncReadLowestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = {
      val from = long2bytearray(fromSequenceNr)
      messagesns.get(genmsgkey(processorId,0,"D"),"payload").map { r =>
        val lowest = ByteBuffer.wrap(r.getOrElse(from)).getLong()
        println("lowest seqnum="+lowest+" for "+processorId)
        lowest
      }
    }
    
	/**
	 * @param processorId
	 * @param toSequenceNr
	 */
	def asyncWriteLastDeletedSequenceNr(processorId: String, toSequenceNr: Long): Future[Unit] = {
	  val ba = long2bytearray(toSequenceNr+1)
	  println("Last delete="+(toSequenceNr+1)+" for "+processorId)
	  messagesns.putBins(genmsgkey(processorId,0,"D"), Map ("payload" -> ba), Option(-1)) //Should never be deleted
	}
	
	private def long2bytearray(lng: Long): Array[Byte] = {
	  val bb = ByteBuffer.allocate(8) //8, In java Long is 64bits
	  bb.putLong(lng)
	  bb.array()
	}
	
	/**
	 * deserialize a buffer
	 * @param b
	 * @return
	 */
	def persistentFromByteBuffer(b: Array[Byte]): PersistentRepr = {
      serialization.deserialize(b, classOf[PersistentRepr]).get
    }
}