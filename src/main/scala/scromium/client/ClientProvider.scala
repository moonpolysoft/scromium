package scromium.client


trait ClientProvider {
  def withClient[T](block : Client => T) : T
}
