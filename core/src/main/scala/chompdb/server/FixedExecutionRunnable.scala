package chompdb.server

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

class FixedExecutionRunnable(delegate: Runnable, maxRunCount: Int) extends Runnable {
  private val runCount = new AtomicInteger()
  @transient var self: ScheduledFuture[_] = null

  def run() {
    delegate.run()
    if (runCount.incrementAndGet() == maxRunCount) {
      self.cancel(false)
    }
  }

  def runNTimes(executor: ScheduledExecutorService, duration: Duration) {
    self = executor.scheduleAtFixedRate(FixedExecutionRunnable.this, 0L, duration.toMillis, MILLISECONDS)
  }
}