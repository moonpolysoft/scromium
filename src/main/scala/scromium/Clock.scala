package scromium

object Clock {
  /**
   * Returns the current epoch time in microseconds. (Well, really just
   * milliseconds with some clock-related skew at the end)
   */
  def timestamp = (System.currentTimeMillis * 1000) + ((System.nanoTime / 1000) % 1000)
}