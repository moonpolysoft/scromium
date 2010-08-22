package scromium

import serializers.Serializers._
import scromium.client._
import org.specs._

class SelectorSpec extends Specification {
  "Selector" should {
    val rows = List(ByteArray(0), ByteArray(1), ByteArray(2))
    
    "read for a single column" in {
      val c = ByteArray(4)
      val selector = new Selector(rows).column(c)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", Some(List(c)), None, None))
    }
    
    "read for multiple columns" in {
      val c = List(ByteArray(4), ByteArray(5), ByteArray(6))
      val selector = new Selector(rows).columns(c)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", Some(c), None, None))
    }
    
    "read a slice" in {
      val slice = Slice(ByteArray(4), ByteArray(6))
      val selector = new Selector(rows).slice(slice)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", None, None, Some(slice)))
    }
  }
  
  "SuperSelector" should {
    val rows = List(ByteArray(0), ByteArray(1), ByteArray(2))
    
    "read for a single supercolumn" in {
      val c = ByteArray(4)
      val selector = new SuperSelector(rows).column(c)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", Some(List(c)), None, None))
    }
    
    "read for multiple supercolumns" in {
      val c = List(ByteArray(4), ByteArray(5), ByteArray(6))
      val selector = new SuperSelector(rows).columns(c)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", Some(c), None, None))
    }
    
    "read a supercolumn slice" in {
      val slice = Slice(ByteArray(4), ByteArray(6))
      val selector = new SuperSelector(rows).slice(slice)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", None, None, Some(slice)))
    }
    
    "read a single subcolumn" in {
      val sc = ByteArray(4)
      val c = ByteArray(5)
      val selector = new SuperSelector(rows).column(sc).subcolumn(c)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", Some(List(sc)), Some(List(c)), None))
    }
    
    "read multiple subcolumns" in {
      val sc = ByteArray(4)
      val c = List(ByteArray(5), ByteArray(6))
      val selector = new SuperSelector(rows).column(sc).subcolumns(c)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", Some(List(sc)), Some(c), None))
    }
    
    "read a subcolumn slice" in {
      val sc = ByteArray(4)
      val slice = Slice(ByteArray(5), ByteArray(7))
      val selector = new SuperSelector(rows).column(sc).slice(slice)
      val read = selector.toRead("cf")
      read must ==(Read(rows, "cf", Some(List(sc)), None, Some(slice)))
    }
  }
}