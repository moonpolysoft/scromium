package scromium.connection


trait ConnectionPool {
  def withConnection[T](block : Client => T) : T
}
