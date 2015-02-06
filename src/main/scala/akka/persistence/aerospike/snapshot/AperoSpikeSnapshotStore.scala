package akka.persistence.aerospike.snapshot

import akka.actor._
import akka.pattern.pipe
import akka.persistence._
import akka.persistence.serialization.Snapshot
import akka.serialization.SerializationExtension
import scala.concurrent.Future
import com.tapad.aerospike.AerospikeClient
import com.typesafe.config.Config
import com.tapad.aerospike.ScanFilter
import java.nio.ByteBuffer
import com.google.common.primitives.Bytes
import scala.util.{Failure,Success}
import scala.collection.JavaConversions._
import com.tapad.aerospike.DefaultValueMappings._
import scala.concurrent.duration._
import scala.concurrent.Await
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Promise

trait AperoSpikeSnapshotStoreEndpoint extends Actor {
  import SnapshotProtocol._
  import context.dispatcher

  val extension = Persistence(context.system)
  val publish = extension.settings.internal.publishPluginCommands

  final def receive = {
    case LoadSnapshot(processorId, criteria, toSequenceNr) ⇒
      val p = sender
      loadAsync(processorId, criteria.limit(toSequenceNr)) map {
        sso ⇒ LoadSnapshotResult(sso, toSequenceNr)
      } recover {
        case e ⇒ LoadSnapshotResult(None, toSequenceNr)
      } pipeTo (p)
    case SaveSnapshot(metadata, snapshot) ⇒
      val p = sender
      val md = metadata.copy(timestamp = System.currentTimeMillis)
      saveAsync(md, snapshot) map {
        _ ⇒ SaveSnapshotSuccess(md)
      } recover {
        case e ⇒ SaveSnapshotFailure(metadata, e)
      } pipeTo (p)
    case d @ DeleteSnapshot(metadata) ⇒
      deleteAsync(Seq(metadata)) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
    case d @ DeleteSnapshots(processorId, criteria) ⇒
      deleteAsync(processorId, criteria) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
  }
  
  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit]
  def deleteAsync(metadata: Seq[SnapshotMetadata]): Future[Unit]
  def deleteAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Unit]
}

class AeroSpikeSnapshotStoreConfig(config: Config) {
  val urls = config.getStringList("urls")
  val namespace = config.getString("namespace")
  val set = config.getString("set")
}

class AperoSpikeSnapshotStore extends AperoSpikeSnapshotStoreEndpoint with ActorLogging {
  val config = new AeroSpikeSnapshotStoreConfig(context.system.settings.config.getConfig("aperospike-snapshot-store"))
  val serialization = SerializationExtension(context.system)

  import context.dispatcher
  import config._

  val client = AerospikeClient(config.urls)
  val snapshotsns = client.namespace(config.namespace).set[String,Array[Byte]](config.set)

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = for {
    mds <- Future(metadata(processorId, criteria))
    res <- loadNAsync(mds)
  } yield res

  def loadNAsync(metadata: Seq[SnapshotMetadata]): Future[Option[SelectedSnapshot]] = metadata match {
    case Seq() => Future.successful(None)
    case md +: mds => load1Async(md) map {
      case Snapshot(s) => Some(SelectedSnapshot(md, s))
    } recoverWith {
      case e => loadNAsync(mds) // try older snapshot
    }
  }

  def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    println("load1Async metadata="+metadata)
    val record = snapshotsns.get(gensnptkey(metadata.persistenceId,metadata.sequenceNr,metadata.timestamp), "payload")
    record.map(r => deserialize(r.get))
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val key = gensnptkey(metadata.persistenceId,metadata.sequenceNr,metadata.timestamp)
    println("saveAsync key="+key+" snapshot="+snapshot)
    val bins : Map[String, Array[Byte]] = Map ("payload" -> serialize(Snapshot(snapshot)))
    snapshotsns.putBins(key, bins)
  }
  
  def deleteAsync(metadata: Seq[SnapshotMetadata]): Future[Unit] = {
    val deletes = metadata.map( md => {
	  val key = gensnptkey(md.persistenceId,md.sequenceNr,md.timestamp)
	  snapshotsns.delete(key)
	})
	Future.sequence(deletes).map(_ => ())
  }

  def deleteAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = for {
    mds <- Future(metadata(processorId, criteria))
    res <- deleteAsync(mds)
  } yield res

  private def serialize(snapshot: Snapshot): Array[Byte] =
    serialization.findSerializerFor(snapshot).toBinary(snapshot)

  private def deserialize(bytes: Array[Byte]): Snapshot =
    serialization.deserialize(bytes, classOf[Snapshot]).get

  private def metadata(processorId: String, criteria: SnapshotSelectionCriteria): List[SnapshotMetadata] = {
    //println("metadata processorId="+processorId+" criteria="+criteria)
    loadMetadata(processorId, criteria.maxSequenceNr).map { record =>
      SnapshotMetadata(
          keysnptpersistenceId(record._1),
    	  keysnptsequence(record._1), 
    	  keysnpttimestamp(record._1)) }.filter(_.timestamp <= criteria.maxTimestamp)
  }
  
  private def loadMetadata(processorId: String, maxSequenceNr: Long): List[(String,Map[String, Array[Byte]])] = {
    //println("loadMetadata processorId="+processorId+" maxSequenceNr="+maxSequenceNr);
    val bins = Seq () //DO NOT INCLUDE PAYLOAD!!!
   	val filter = new ScanFilter [String, Map[String, Array[Byte]]] {
   	  def filter(key: String, record:Map[String, Array[Byte]]): Boolean = {
   	    println("metadata key="+key+" record="+record+" filter="+(keysnptpersistenceId(key) == processorId && keysnptsequence(key) <= maxSequenceNr));
   	    (keysnptpersistenceId(key) == processorId && keysnptsequence(key) <= maxSequenceNr)
   	  }
   	}
    val records = Await.result(snapshotsns.scanAllRecords(bins,filter),Duration.Inf).sortWith{(leftE,rightE) => 
     keysnptsequence(leftE._1) > keysnptsequence(rightE._1)
    } //The cassandra implementation uses also a synchronous call to select the snapshot metadata
    //println("loadMetadata records="+records);
    records
  }

}
