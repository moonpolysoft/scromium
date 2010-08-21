package scromium

import serializers._
import client._

trait Readable {
  def toRead(cf : String) : Read
}

trait Deletable {
  def toDelete(cf : String, clock : Clock) : Delete
}

class Selector(val rows : List[Array[Byte]]) extends Readable with Deletable {
  
  def column[C](column : C)(implicit ser : Serializer[C]) =
    new ColumnSelector(rows, List(ser.serialize(column)))
    
  def columns[C](columns : List[C])(implicit ser : Serializer[C]) =
    new ColumnSelector(rows, columns.map(ser.serialize(_)))
    
  def slice(slice : Slice) = 
    new SliceSelector(rows, slice)
    
  def toRead(cf : String) = new Read(rows, cf)
  
  def toDelete(cf : String, clock : Clock) = new Delete(rows, cf, clock=clock)
}

class SuperSelector(rows : List[Array[Byte]]) extends Readable with Deletable {
  
  def column[C](column : C)(implicit ser : Serializer[C]) =
    new SuperColumnSelector(rows, ser.serialize(column))
  
  def columns[C](columns : List[C])(implicit ser : Serializer[C]) =
    new ColumnSelector(rows, columns.map(ser.serialize(_)))

  def slice(slice : Slice) = 
    new SliceSelector(rows, slice)
    
  def toRead(cf : String) = new Read(rows, cf)
  
  def toDelete(cf : String, clock : Clock) = new Delete(rows, cf, clock=clock)
}

//privatish

class ColumnSelector(
  val rows : List[Array[Byte]], 
  val columns : List[Array[Byte]]) extends Readable with Deletable {
  
  def toRead(cf : String) =
    new Read(rows, cf, Some(columns))
  
  def toDelete(cf : String, clock : Clock) = 
    new Delete(rows, cf, Some(columns), clock=clock)
}

class SliceSelector(
  val rows : List[Array[Byte]], 
  val slice : Slice) extends Readable with Deletable {
  
  def toRead(cf : String) =
    new Read(rows, cf, slice=Some(slice))
    
  def toDelete(cf : String, clock : Clock) =
    new Delete(rows, cf, slice=Some(slice), clock=clock)
}

class SuperColumnSelector(
  val rows : List[Array[Byte]], 
  val column : Array[Byte]) extends Readable with Deletable {
  
  def subcolumn[C](subcolumn : C)(implicit ser : Serializer[C]) =
    new SubColumnSelector(rows, column, List(ser.serialize(subcolumn)))
    
  def subcolumns[C](subcolumns : List[C])(implicit ser : Serializer[C]) =
    new SubColumnSelector(rows, column, subcolumns.map(ser.serialize(_)))
    
  def slice(slice : Slice) =
    new SubColumnSliceSelector(rows, column, slice)
    
  def toRead(cf : String) =
    new Read(rows, cf, Some(List(column)))
    
  def toDelete(cf : String, clock : Clock) =
    new Delete(rows, cf, Some(List(column)), clock=clock)
}

class SubColumnSelector(
  val rows : List[Array[Byte]], 
  val column : Array[Byte], 
  val subColumns : List[Array[Byte]]) extends Readable with Deletable {
  
  def toRead(cf : String) =
    new Read(rows, cf, Some(List(column)), subColumns = Some(subColumns))
  
  def toDelete(cf : String, clock : Clock) =
    new Delete(rows, cf, Some(List(column)), subColumns = Some(subColumns), clock=clock)
}

class SubColumnSliceSelector(
  val rows : List[Array[Byte]], 
  val column : Array[Byte], 
  val slice : Slice) extends Readable with Deletable {
  
  def toRead(cf : String) =
    new Read(rows, cf, Some(List(column)), slice = Some(slice))
    
  def toDelete(cf : String, clock : Clock) =
    new Delete(rows, cf, Some(List(column)), slice=Some(slice), clock=clock)
}
