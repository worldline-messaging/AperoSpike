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

class AperoSpikeJournalConfig(config: Config) {
	val urls = config.getStringList("urls")
	val namespace = config.getString("namespace")
	val set = config.getString("set")
}

class AperoSpikeJournal extends AsyncWriteJournal with AperoSpikeRecovery {
	val serialization = SerializationExtension(context.system)
	
	val config = new AperoSpikeJournalConfig(context.system.settings.config.getConfig("aperospike-journal"))
	 
	val client = AerospikeClient(config.urls)
    val messagesns = client.namespace(config.namespace).set[String,Array[Byte]](config.set)
	
	def asyncWriteMessages(messages: scala.collection.immutable.Seq[PersistentRepr]): Future[Unit] = {
	  val puts = messages.map { m =>
	    val key = genmsgkey(m.persistenceId,m.sequenceNr,"A")
	    val bins : Map[String, Array[Byte]] = Map ("payload" -> serialization.serialize(m).get)
	    messagesns.putBins(key, bins)
	  }
	  Future.sequence(puts).map(_ => ())
	}
	
	def serialiseFutures[A, B](l: Iterable[A])(fn: A ⇒ Future[B])
    (implicit ec: ExecutionContext): Future[List[B]] =
      l.foldLeft(Future(List.empty[B])) {
        (previousFuture, next) =>
        for {
          previousResults <- previousFuture
          next <- fn(next)
        } yield previousResults :+ next
      }
	
	def asyncWriteConfirmation (confirmation: PersistentConfirmation): Future[Unit] = {
      val key = genmsgkey(confirmation.persistenceId,confirmation.sequenceNr,"C")
	  messagesns.getBins(key, Seq("payload")).flatMap {
	    r => {
	      if(r.isEmpty) {
	        println("Confirm is empty ("+confirmation.persistenceId+","+confirmation.sequenceNr+")")
            val bins : Map[String, Array[Byte]] = Map ("payload" -> confirmation.channelId.getBytes())
            messagesns.putBins(key, bins)
          } else {
            println("Confirm:"+new String(r("payload"))+" ("+confirmation.persistenceId+","+confirmation.sequenceNr+")")
            val bins : Map[String, Array[Byte]] = Map ("payload" -> (new String(r("payload"))+":"+confirmation.channelId).getBytes())
            messagesns.putBins(key, bins)
          }
	    }
	  }
	}
	
	def asyncWriteConfirmations(confirmations: scala.collection.immutable.Seq[PersistentConfirmation]): Future[Unit] = {
	  serialiseFutures(confirmations)(asyncWriteConfirmation).map(_ => ())
      /*val puts =  confirmations.map { c =>
        val key = genmsgkey(c.persistenceId,c.sequenceNr,"C")
        messagesns.getBins(key, Seq("payload")) map {
          r => {
            if(r.isEmpty) {
              val bins : Map[String, Array[Byte]] = Map ("payload" -> c.channelId.getBytes())
              messagesns.putBins(key, bins)
            } else {
              val bins : Map[String, Array[Byte]] = Map ("payload" -> (new String(r("payload"))+":"+c.channelId).getBytes())
              messagesns.putBins(key, bins)
            }
          }
        }
      }
	  Future.sequence(puts).map(_ => ())*/
    }
	
	def asyncDeleteMessages(messageIds: scala.collection.immutable.Seq[akka.persistence.PersistentId],permanent: Boolean): Future[Unit] = {
	  if(permanent) {
		val keys = messageIds.map(mid => Seq(genmsgkey(mid.persistenceId,mid.sequenceNr,"A"),genmsgkey(mid.persistenceId,mid.sequenceNr,"B"),genmsgkey(mid.persistenceId,mid.sequenceNr,"C"))).flatten
		val deletes = keys.map(key => messagesns.delete(key))
		Future.sequence(deletes).map(_ => ())
	  } else {
	    val keys = messageIds.map(mid => genmsgkey(mid.persistenceId,mid.sequenceNr,"B"))
	    val puts = keys.map(key => messagesns.putBins(key, Map ("payload" -> Array(0x00))))
	    Future.sequence(puts).map(_ => ())
	  }
	} 
	  
	def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
	  val fromSequenceNr = readLowestSequenceNr(processorId, 1L)
	  asyncDeleteMessagesFromTo(processorId,fromSequenceNr,toSequenceNr,permanent)
    }
	
	def asyncDeleteMessagesFromTo(processorId: String, fromSequenceNr:Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
	  if(permanent) {
	    val keys = (fromSequenceNr to toSequenceNr).map(sequence => Seq(genmsgkey(processorId,sequence,"A"),genmsgkey(processorId,sequence,"B"),genmsgkey(processorId,sequence,"C"))).flatten
	    val deletes = keys.map(key => messagesns.delete(key))
		Future.sequence(deletes).map(_ => ())
	  } else {
	    val keys = (fromSequenceNr to toSequenceNr).map(sequence => genmsgkey(processorId,sequence,"B"))
	    val puts = keys.map(key => messagesns.putBins(key, Map ("payload" -> Array(0x00))))
	    Future.sequence(puts).map(_ => ())
	  }  
	}
	
	def persistentFromByteBuffer(b: Array[Byte]): PersistentRepr = {
      serialization.deserialize(b, classOf[PersistentRepr]).get
    }
	
}