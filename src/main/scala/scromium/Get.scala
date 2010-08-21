/*package scromium

import serializers._
import client._

object Get {
  def apply[R](row : R)(implicit rSer : Serializer[R]) = new RowGet(rSer.serialize(row))
}

trait Get {
  def toCommand(cf : String) : Read
}

class RowGet(val row : Array[Byte]) extends Get {
  def column[C](name : C)(implicit cSer : Serializer[C]) =
    new ColumnGet(row, cSer.serialize(name))
  
  def columns[C](names : List[C])(implicit cSer : Serializer[C]) =
    new MultiColumnGet(row, names.map(cSer.serialize(_)))
  
  def slice(slice : Slice) =
    new SliceColumnGet(row, slice)
    
  def toCommand(cf : String) = Read(row, cf)
}

class SliceColumnGet(val row : Array[Byte], val slice : Slice) extends Get {
  def toCommand(cf : String) = Read(row, cf, slice = Some(slice))
}

class MultiColumnGet(val row : Array[Byte], val columns : List[Array[Byte]]) extends Get {
  def toCommand(cf : String) = Read(row, cf, columns = Some(columns))
}

class ColumnGet(val row : Array[Byte], val column : Array[Byte]) {
  def subcolumn[C](name : C)(implicit cSer : Serializer[C]) =
    new SubColumnGet(row, column, cSer.serialize(name))
    
  def subcolumns[C](names : List[C])(implicit cSer : Serializer[C]) =
    new MultiSubColumnGet(row, column, names.map(cSer.serialize(_)))
    
  def toCommand(cf : String) = Read(row, cf, Some(column))
}

class SubColumnGet(val row : Array[Byte], val column : Array[Byte], val subColumn : Array[Byte]) extends Get {
  def toCommand(cf : String)  = Read(row, cf, Some(column), subColumn =  Some(subColumn))
}

class MultiSubColumnGet(val row : Array[Byte], val column : Array[Byte], val subColumns : List[Array[Byte]]) extends Get {
  def toCommand(cf : String) = Read(row, cf, Some(column), subColumns = Some(subColumns))
}

class SliceSubColumnGet(val row : Array[Byte], val column : Array[Byte], val slice : Slice) extends Get {
  def toCommand(cf : String) = Read(row, cf, Some(column), slice = Some(slice))
}*/