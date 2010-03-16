package scromium

import org.specs._
import org.specs.mock.Mockito
import api._
import serializers.Serializers._


class IntegrationSpec extends Specification with Mockito with TestHelper {
  "Scromium" should {
    "execute a full suite" in {
      Cassandra.start

      Keyspace("Keyspace1") { ks =>
        implicit val consistency = WriteConsistency.Any
        ks.insert("row", "Standard1" -> "column", "value")
        ks.batch("row").add("Standard1" -> "column1", "value1").add("Standard1" -> "column2", "value2").add("Standard1" -> "column3", "value3")!
      }
      Keyspace("Keyspace1") { ks =>
        implicit val consistency = ReadConsistency.One
        val result : GetColumn = ks.get("row", "Standard1") / "column" !;
        result.valueAs[String] must beEqualTo("value")
        val slices = ks.rangeSlices("Standard1").keys("row", "row").columnRange("column", "column3")!;
        slices.length must beEqualTo(1)
        val (row, columns) = slices(0)
        row must beEqualTo("row")
        columns.length must beEqualTo(4)
        columns(0).valueAs[String] must beEqualTo("value")
        columns(1).valueAs[String] must beEqualTo("value1")
        columns(2).valueAs[String] must beEqualTo("value2")
        columns(3).valueAs[String] must beEqualTo("value3")
      }
    }
  }
}