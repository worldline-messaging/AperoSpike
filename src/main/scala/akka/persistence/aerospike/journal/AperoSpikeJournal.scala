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

/**
 * The aperospike configuration file for the journal
 * 
 */
class AperoSpikeJournalConfig(config: Config) {
	val urls = config.getStringList("urls")
	val namespace = config.getString("namespace")
	val set = config.getString("set")
	val seqinterval = config.getLong("seqinterval")
}

/**
 * The aperospike journal.
 * This implementation is not complete and is based on the LRU of areospike to manage the deletion. 
 * TODO: It is possible to improve the management of the deletion by including secondary indexes. 
 * But the secondary indexes will certainly have an impact on performance. 
 */
class AperoSpikeJournal extends AsyncWriteJournal with AperoSpikeRecovery {
	val serialization = SerializationExtension(context.system)
	
	val config = new AperoSpikeJournalConfig(context.system.settings.config.getConfig("aperospike-journal"))
	 
	val client = AerospikeClient(config.urls)
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
	    messagesns.putBins(key, bins)
	  }
	  Future.sequence(puts).map(_ => ())
	}
	
	/**
	 * Make a iterable of items be treated by futures, but serialize these treatments in the order of the collection.
	 * It's like andThen { l(0) } andThen { l(1) } andThen { l(2) } .... andThen { l(n-1) }
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
      val key = genmsgkey(confirmation.persistenceId,confirmation.sequenceNr,"C")
	  messagesns.getBins(key, Seq("payload")).flatMap {
	    r => {
	      if(r.isEmpty) {
	        //println("Confirm is empty ("+confirmation.persistenceId+","+confirmation.sequenceNr+")")
            val bins : Map[String, Array[Byte]] = Map ("payload" -> confirmation.channelId.getBytes())
            messagesns.putBins(key, bins)
          } else {
            //println("Confirm:"+new String(r("payload"))+" ("+confirmation.persistenceId+","+confirmation.sequenceNr+")")
            val bins : Map[String, Array[Byte]] = Map ("payload" -> (new String(r("payload"))+":"+confirmation.channelId).getBytes())
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
	 * Not supported.
	 * TODO: Try with the secondary index of aerospike ???
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
	  Future.successful(deletions = deletions + (processorId -> (toSequenceNr, permanent)))
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