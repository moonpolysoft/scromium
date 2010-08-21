package scromium

import serializers._

class Selector(val rows : List[Array[Byte]]) {
  
  def column[C](column : C)(implicit ser : Serializer[C]) =
    new ColumnSelector(rows, List(ser.serialize(column)))
    
  def columns[C](columns : List[C])(implicit ser : Serializer[C]) =
    new ColumnSelector(rows, columns.map(ser.serialize(_)))
    
  def slice(slice : Slice) = 
    new SliceSelector(rows, slice)
}

class SuperSelector(rows : List[Array[Byte]]) {
  
  def column[C](column : C)(implicit ser : Serializer[C]) =
    new SuperColumnSelector(rows, ser.serialize(column))
  
  def columns[C](columns : List[C])(implicit ser : Serializer[C]) =
    new ColumnSelector(rows, columns.map(ser.serialize(_)))

  def slice(slice : Slice) = 
    new SliceSelector(rows, slice)
}

//privatish

class ColumnSelector(val rows : List[Array[Byte]], val column : List[Array[Byte]])

class SliceSelector(val rows : List[Array[Byte]], val slice : Slice)

class SuperColumnSelector(val rows : List[Array[Byte]], val column : Array[Byte]) {
  
  def subcolumn[C](subcolumn : C)(implicit ser : Serializer[C]) =
    new SubColumnSelector(rows, column, List(ser.serialize(subcolumn)))
    
  def subcolumns[C](subcolumns : List[C])(implicit ser : Serializer[C]) =
    new SubColumnSelector(rows, column, subcolumns.map(ser.serialize(_)))
    
  def slice(slice : Slice) =
    new SubColumnSliceSelector(rows, column, slice)
}

class SubColumnSelector(val rows : List[Array[Byte]], val column : Array[Byte], val subColumn : List[Array[Byte]])

class SubColumnSliceSelector(val rows : List[Array[Byte]], val column : Array[Byte], val slice : Slice)
