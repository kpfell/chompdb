package chompdb.server

import chompdb._
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ScheduledExecutorTest extends WordSpec with ShouldMatchers {
	"ScheduledExecutor" should {
		"create a new ScheduledExecutor object" in {
			val executor = new ScheduledExecutor()

			executor.getClass.getSimpleName should be === "ScheduledExecutor"
		}
	}
}