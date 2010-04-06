package scromium.connection


trait ConnectionPool {
  def withConnection[T](block : Connection => T) : T
}
