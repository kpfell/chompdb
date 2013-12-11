package chompdb.server

import chompdb._
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import f1lesystem.LocalFileSystem

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompDBTest extends WordSpec with ShouldMatchers {
	val tmpRoot = new LocalFileSystem.TempRoot {
		override val rootName = "ChompDBTest"
	}

	val testCatalog = Catalog("TestCatalog", tmpRoot.fs, tmpRoot.root)

	val testDatabase = testCatalog.database("TestDatabase")

	"Server" should {
		"create a new ChompDB object" in {
			val chomp = new ChompDB(Seq(testDatabase), 1, 1, 0, 1, new ScheduledExecutor(),
				tmpRoot.fs//, tmpRoot.root
			)

			chomp.getClass.getSimpleName should be === "ChompDB"
		}
	}
}