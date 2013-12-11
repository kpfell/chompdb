package chompdb.server

import chompdb._
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class TimerTaskTest extends WordSpec with ShouldMatchers {
	"TimerTask" should {
		"create a new TimerTask object" in {
			val timerTask = new TimerTask()

			timerTask.getClass.getSimpleName should be === "TimerTask"
		}
	}
}