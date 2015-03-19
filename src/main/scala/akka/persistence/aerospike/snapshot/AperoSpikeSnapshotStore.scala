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
import com.tapad.aerospike.ClientSettings
import com.tapad.aerospike.AsSetOps
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

/**
 * @author a140168
 * The snapshot endpoint
 *
 */
trait AperoSpikeSnapshotStoreEndpoint extends Actor {
  import SnapshotProtocol._
  import context.dispatcher

  val extension = Persistence(context.system)
  val publish = extension.settings.internal.publishPluginCommands
  
  final def receive = {
    case LoadSnapshot(processorId, criteria, toSequenceNr) =>
      val p = sender
      loadAsync(processorId, criteria.limit(toSequenceNr)) map {
        sso => LoadSnapshotResult(sso, toSequenceNr)
      } recover {
        case e => LoadSnapshotResult(None, toSequenceNr)
      } pipeTo (p)
    case SaveSnapshot(metadata, snapshot) =>
      println ("Save the Snapshot")
      val p = sender
      val md = metadata.copy(timestamp = System.currentTimeMillis)
      saveAsync(md, snapshot) map {
        _ => SaveSnapshotSuccess(md)
      } recover {
        case e => SaveSnapshotFailure(metadata, e)
      } pipeTo (p)
    case d @ DeleteSnapshot(metadata) =>
      deleteAsync(Seq(metadata)) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
    case d @ DeleteSnapshots(processorId, criteria) =>
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

/**
 * @author a140168
 * The snapshot configuration
 *
 */
class AeroSpikeSnapshotStoreConfig(config: Config) {
  val urls = config.getStringList("urls")
  val namespace = config.getString("namespace")
  val set = config.getString("set")
  val selectorThreads = config.getInt("selectorThreads")
}

/**
 * @author a140168
 * The snapshot store for aerospike
 * This implementation uses a cache in memory. Only the metadata are cached. Not the snapshot. 
 * It prevents from loading all the data (scanAll) at each time loadAsync is called.
 * The cache is loaded at the start. And then is updated on snapshot save and delete.
 * TODO: We can use another solution than a cache with the aerospike secondary index. But it can loose performance.
 */
class AperoSpikeSnapshotStore extends AperoSpikeSnapshotStoreEndpoint with ActorLogging {
  val config = new AeroSpikeSnapshotStoreConfig(context.system.settings.config.getConfig("aperospike-snapshot-store"))
  val serialization = SerializationExtension(context.system)

  import context.dispatcher
  import config._

  val asyncTaskThreadPool = Executors.newCachedThreadPool(new ThreadFactory() {
		override def newThread(runnable: Runnable) = {
                        val thread = new Thread(runnable)
                        thread.setDaemon(true)
                        thread
                }
    })
    
  val client = AerospikeClient(config.urls,new ClientSettings(blockingMode=true,selectorThreads=config.selectorThreads,taskThreadPool=asyncTaskThreadPool))
  val snapshotsns = client.namespace(config.namespace).set[String,Array[Byte]](config.set)

  var cache: Map[String, scala.collection.mutable.ListBuffer[String]] = Map.empty
  
  override def preStart() {
    println("initialize the cache")
    val bins = Seq () //DO NOT INCLUDE PAYLOAD!!!
    val filter = new ScanFilter [String, Map[String, Array[Byte]]] {
   	  def filter(key: String, record:Map[String, Array[Byte]]): Boolean = { childsnapshot(key)==false }
   	}
    val records = Await.result(snapshotsns.scanAllRecords[List](bins,filter),Duration.Inf)
    records.foreach(r => {
      val l = cache.getOrElse(keysnptpersistenceId(r._1), scala.collection.mutable.ListBuffer.empty[String])
      l += r._1
      cache = cache + (keysnptpersistenceId(r._1) -> l)
    })
    println("cache="+cache)
  }
  
  /**
   * load a snapshot corresponding to criteria
   */
  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = for {
    mds <- Future(metadata(processorId, criteria))
    res <- loadNAsync(mds)
  } yield res

  /**
   * load a sequence of snapshots
   */
  def loadNAsync(metadata: Seq[SnapshotMetadata]): Future[Option[SelectedSnapshot]] = metadata match {
    case Seq() => Future.successful(None)
    case md +: mds => load1Async(md) map {
      case Snapshot(s) => Some(SelectedSnapshot(md, s))
    } recoverWith {
      case e => e.printStackTrace(); loadNAsync(mds) // try older snapshot
    }
  }

  /**
   * load a snapshot
   */
  def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    println("load1Async metadata="+metadata+ " key="+gensnptkey(metadata.persistenceId,metadata.sequenceNr,metadata.timestamp))
    val baseKey = gensnptkey(metadata.persistenceId,metadata.sequenceNr,metadata.timestamp)
    
    //get the keys for the potential children
    val record = snapshotsns.getBins(baseKey, Seq("numchild"))
    val fkeys = record map { r => 
      val numchild = if(!r.keySet.exists(_=="numchild")) 1 else new String(r("numchild")).toInt
      if(numchild > 1) {
        baseKey +: (1 to numchild-1).map { i => baseKey+"_"+i }
      } else { //no splitting
        List(baseKey)
      }
    }
    
    //get the payloads for the keys
    val payloads = fkeys.flatMap { keys =>
      println("load snapshot keys="+keys)
      val frecords = snapshotsns.multiGetBinsL(keys,Seq("payload"))
      val res = frecords.map { records =>
        records.map { r =>
          r._2("payload")
        }
      }
      res
    }

    //rebuild the payload and deserialize
    payloads.map { pls =>
      deserialize(pls.flatten.toArray)
    }
  }

  /**
   * save a snapshot on aerospike and then update the metadata cache
   */
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val key = gensnptkey(metadata.persistenceId,metadata.sequenceNr,metadata.timestamp)
    val array = serialize(Snapshot(snapshot))
    println("saveAsync key="+key+" size="+array.length)
    splitAndSave(snapshotsns,key,"payload",array,900000).andThen { case _ => 
      val l = cache.getOrElse(keysnptpersistenceId(key), scala.collection.mutable.ListBuffer.empty[String])
      l += key
      cache = cache + (keysnptpersistenceId(key) -> l)
      //println("cache update for "+keysnptpersistenceId(key)+"="+cache(keysnptpersistenceId(key)))
    }
  }
  
  /**
  * @param namespace
  * @param baseKey
  * @param binName
  * @param payload
  * @param maxSize
  * @return
  */
  def splitAndSave(namespace: AsSetOps[String,Array[Byte]], baseKey: String, binName: String, payload: Array[Byte], maxSize: Int) : Future[Unit] = {
    if(payload.length <= maxSize) { //No need to split
      val bins : Map[String, Array[Byte]] = Map (binName -> payload)
      namespace.putBins(baseKey, bins)
    } else { //Need to split
      val arrays = splitArray[Byte](payload,maxSize) //split the array
      arrays.foreach{a => println( "split snapshot "+baseKey+" {"+a._1+","+a._2.length+"}") }
      val puts = arrays.map { p => {
        if(p._1==0) { //No suffix for the first array, but the numchild bin
          val bins : Map[String, Array[Byte]] = Map (binName -> p._2,"numchild" -> new String(""+arrays.length).getBytes)
          println("save snapshot baseKey="+baseKey)
          namespace.putBins(baseKey, bins)
        } else { //For the next array, we suffix the keys
          val bins : Map[String, Array[Byte]] = Map (binName -> p._2)
          val childkey = gensnptkeychild(baseKey,p._1)
          println("save snapshot childKey="+childkey)
          namespace.putBins(childkey, bins)
        }
      } }
      Future.sequence(puts).map(_ => ())
    }
  }
  
  def splitArray[T](xs: Array[T], splitSize: Int): Seq[(Int,Array[T])] = {
    var i = -1
    (0 to xs.length-1 by splitSize).map { j =>
      val rest = math.min(splitSize,xs.length-j)
      println("j="+j+" rest="+rest)
      ({i +=1;i},xs.slice(j, j+rest))
    }
  }
  
  
  /**
   * delete a snapshot from aerospike and then delete from the metadata cache
   */
  def deleteAsync(metadata: Seq[SnapshotMetadata]): Future[Unit] = {
    val deletes = metadata.map( md => {
	  val key = gensnptkey(md.persistenceId,md.sequenceNr,md.timestamp)
	  println("deleteAsync metadata="+Seq(md.persistenceId,md.sequenceNr,md.timestamp))
	  deleteSingleAsync(key).andThen { case _ =>
	    val l = cache.getOrElse(keysnptpersistenceId(key), scala.collection.mutable.ListBuffer.empty[String])
	    l -= key
	    cache = cache + (keysnptpersistenceId(key) -> l)
	    //println("cache delete for "+keysnptpersistenceId(key)+"="+cache(keysnptpersistenceId(key)))
	  }
	})
	Future.sequence(deletes).map(_ => ())
  }

  private def deleteSingleAsync (baseKey: String) : Future [Unit] = {
    
    //get the keys for the potential children
    val record = snapshotsns.getBins(baseKey, Seq("numchild"))
    val fkeys = record map { r => 
      val numchild = if(!r.keySet.exists(_=="numchild")) 1 else new String(r("numchild")).toInt
      if(numchild > 1) {
        baseKey +: (1 to numchild-1).map { i => baseKey+"_"+i }
      } else { //no splitting
        List(baseKey)
      }
    }
   
    //delete
    val fdeletes = fkeys.flatMap { keys =>
      val deletes = keys.map { key =>
        println( "delete snapshot "+key)
        snapshotsns.delete(key)
      }
      Future.sequence(deletes).map(_ => ())
    }
    
    fdeletes
  }
  /**
   * delete snapshots corresponding to criteria
   */
  def deleteAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = for {
    mds <- Future(metadata(processorId, criteria))
    res <- deleteAsync(mds)
  } yield res

  private def serialize(snapshot: Snapshot): Array[Byte] =
    serialization.findSerializerFor(snapshot).toBinary(snapshot)

  private def deserialize(bytes: Array[Byte]): Snapshot =
    serialization.deserialize(bytes, classOf[Snapshot]).get

  /**
   * load metadata from the cache corresponding to criteria
   */
  private def metadata(processorId: String, criteria: SnapshotSelectionCriteria): List[SnapshotMetadata] = {
    //println("metadata processorId="+processorId+" criteria="+criteria)
    loadMetadata(processorId, criteria.maxSequenceNr).map { record =>
      SnapshotMetadata(
          keysnptpersistenceId(record._1),
    	  keysnptsequence(record._1), 
    	  keysnpttimestamp(record._1)) }.filter(_.timestamp <= criteria.maxTimestamp)
  }
  
  /**
   * load metadata inferior a sequence number
   */
  private def loadMetadata(processorId: String, maxSequenceNr: Long): List[(String,Map[String, Array[Byte]])] = {
    //println("loadMetadata processorId="+processorId+" maxSequenceNr="+maxSequenceNr);
    val l = cache.getOrElse(escapepid(processorId), scala.collection.mutable.ListBuffer.empty[String])
    if(!l.isEmpty) { //No element in cache for processorId
      //println("Cache is NOT empty for "+processorId+ " cache="+l)
      val res = l.map (e => (e,Map.empty[String,Array[Byte]])).toList.
        filter {r => keysnptsequence(r._1) <= maxSequenceNr }.
          sortWith{(leftE,rightE) => 
	        keysnptsequence(leftE._1) > keysnptsequence(rightE._1)
      }
      //println("result="+res)
      res
    } else { 
      List.empty   
    }
  }
}
