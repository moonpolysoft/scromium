package scromium.api

case class Row[T](val key : String, val columns : Seq[T])