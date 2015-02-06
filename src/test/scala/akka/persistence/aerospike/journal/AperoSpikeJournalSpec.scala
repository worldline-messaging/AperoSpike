package akka.persistence.aerospike.journal

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import akka.persistence.journal.JournalPerfSpec
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._

class AperoSpikeJournalSpec extends JournalSpec with JournalPerfSpec with BeforeAndAfterAll {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "aperospike-journal"
      |akka.persistence.snapshot-store.plugin = "aperospike-snapshot-store"
      |akka.test.single-expect-default = 10s
      |aperospike-journal.urls = ["localhost"]
      |aperospike-snapshot-store.urls = ["localhost"]
    """.stripMargin)

  override def awaitDurationMillis: Long = 20.seconds.toMillis
}