package scromium

import java.util.concurrent.TimeUnit._

object Clock {
  private val ratio = NANOSECONDS.convert(1, MICROSECONDS)
  
  /**
   * Returns the current epoch time in microseconds.
   */
  def timestamp = System.nanoTime / ratio
}