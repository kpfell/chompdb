package chompdb.integration

class RandomUtils {
  private val _random = new scala.util.Random
  def randomInt(low: Int, high: Int): Int = _random.nextInt(high-low) + low
  def randomLong(low: Long, high: Long): Long = math.abs(_random.nextLong) % (high-low) + low
  
  /** Pick a random element out of a Seq[T] */
  def pick[T](s: Seq[T]) = s(randomInt(0, s.size))
  
  /** Pick a random element out of an Iterable[T] */
  def pick[T](s: Iterable[T]) = {
    val iter = s.iterator
    var i = randomInt(0, s.size)
    while (i > 0) { iter.next(); i -= 1 }
    iter.next
  }
}
