package akka.persistence.aerospike.snapshot

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

class AperoSpikeSnapshotStoreSpec extends SnapshotStoreSpec {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "aperospike-journal"
      |akka.persistence.snapshot-store.plugin = "aperospike-snapshot-store"
      |akka.test.single-expect-default = 10s
      |aperospike-journal.urls = ["localhost"]
      |aperospike-snapshot-store.urls = ["localhost"]
    """.stripMargin)
}