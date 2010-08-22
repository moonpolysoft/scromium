package scromium

import serializers.Serializers._
import scromium.client._
import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._

class SelectorSpec extends Specification with Mockito {
  val rows = List(ByteArray(0), ByteArray(1), ByteArray(2))
  val clock = mock[Clock]
  clock.timestamp returns 1
  
  "Selector" should {
    "read" in {
      "a whole row" in {
        val selector = new Selector(rows)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", None, None, None))
      }

      "for a single column" in {
        val c = ByteArray(4)
        val selector = new Selector(rows).column(c)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", Some(List(c)), None, None))
      }

      "for multiple columns" in {
        val c = List(ByteArray(4), ByteArray(5), ByteArray(6))
        val selector = new Selector(rows).columns(c)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", Some(c), None, None))
      }

      "a slice" in {
        val slice = Slice(ByteArray(4), ByteArray(6))
        val selector = new Selector(rows).slice(slice)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", None, None, Some(slice)))
      }
    }
    
    "delete" in {
      "a whole row" in {
        val selector = new Selector(rows)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", None, None, None, clock))
      }

      "for a single column" in {
        val c = ByteArray(4)
        val selector = new Selector(rows).column(c)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", Some(List(c)), None, None, clock))
      }

      "for multiple columns" in {
        val c = List(ByteArray(4), ByteArray(5), ByteArray(6))
        val selector = new Selector(rows).columns(c)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", Some(c), None, None, clock))
      }

      "a slice" in {
        val slice = Slice(ByteArray(4), ByteArray(6))
        val selector = new Selector(rows).slice(slice)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", None, None, Some(slice), clock))
      }
    }
  }
  
  "SuperSelector" should {
    val rows = List(ByteArray(0), ByteArray(1), ByteArray(2))
    
    "read" in {
      "a whole row" in {
        val selector = new SuperSelector(rows)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", None, None, None))
      }

      "for a single supercolumn" in {
        val c = ByteArray(4)
        val selector = new SuperSelector(rows).column(c)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", Some(List(c)), None, None))
      }

      "for multiple supercolumns" in {
        val c = List(ByteArray(4), ByteArray(5), ByteArray(6))
        val selector = new SuperSelector(rows).columns(c)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", Some(c), None, None))
      }

      "a supercolumn slice" in {
        val slice = Slice(ByteArray(4), ByteArray(6))
        val selector = new SuperSelector(rows).slice(slice)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", None, None, Some(slice)))
      }

      "a single subcolumn" in {
        val sc = ByteArray(4)
        val c = ByteArray(5)
        val selector = new SuperSelector(rows).column(sc).subcolumn(c)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", Some(List(sc)), Some(List(c)), None))
      }

      "multiple subcolumns" in {
        val sc = ByteArray(4)
        val c = List(ByteArray(5), ByteArray(6))
        val selector = new SuperSelector(rows).column(sc).subcolumns(c)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", Some(List(sc)), Some(c), None))
      }

      "a subcolumn slice" in {
        val sc = ByteArray(4)
        val slice = Slice(ByteArray(5), ByteArray(7))
        val selector = new SuperSelector(rows).column(sc).slice(slice)
        val read = selector.toRead("cf")
        read must ==(Read(rows, "cf", Some(List(sc)), None, Some(slice)))
      }
    }
    
    "delete" in {
      "a whole row" in {
        val selector = new SuperSelector(rows)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", None, None, None, clock))
      }

      "for a single supercolumn" in {
        val c = ByteArray(4)
        val selector = new SuperSelector(rows).column(c)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", Some(List(c)), None, None, clock))
      }

      "for multiple supercolumns" in {
        val c = List(ByteArray(4), ByteArray(5), ByteArray(6))
        val selector = new SuperSelector(rows).columns(c)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", Some(c), None, None, clock))
      }

      "a supercolumn slice" in {
        val slice = Slice(ByteArray(4), ByteArray(6))
        val selector = new SuperSelector(rows).slice(slice)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", None, None, Some(slice), clock))
      }

      "a single subcolumn" in {
        val sc = ByteArray(4)
        val c = ByteArray(5)
        val selector = new SuperSelector(rows).column(sc).subcolumn(c)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", Some(List(sc)), Some(List(c)), None, clock))
      }

      "multiple subcolumns" in {
        val sc = ByteArray(4)
        val c = List(ByteArray(5), ByteArray(6))
        val selector = new SuperSelector(rows).column(sc).subcolumns(c)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", Some(List(sc)), Some(c), None, clock))
      }

      "a subcolumn slice" in {
        val sc = ByteArray(4)
        val slice = Slice(ByteArray(5), ByteArray(7))
        val selector = new SuperSelector(rows).column(sc).slice(slice)
        val delete = selector.toDelete("cf", clock)
        delete must ==(Delete(rows, "cf", Some(List(sc)), None, Some(slice), clock))
      }
    }
  }
}