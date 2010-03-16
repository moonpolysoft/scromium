package scromium

import org.specs._
import org.specs.mock.Mockito
import api._
import serializers.Serializers._


class IntegrationSpec extends Specification with Mockito with TestHelper {
  "Scromium" should {
    "" in {
      Cassandra.start

      Keyspace("Keyspace1") { ks =>
        implicit val consistency = WriteConsistency.Any
        ks.insert("row", "Standard1" -> "column", "value")
      }
      Keyspace("Keyspace1") { ks =>
        implicit val consistency = ReadConsistency.One
        val result : GetColumn = ks.get("row", "Standard1") / "column" !;
        result.valueAs[String] must beEqualTo("value")
      }
    }
  }
}