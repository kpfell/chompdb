package chompdb.store

import chompdb._
import chompdb.sharding._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ShardedStoreTest extends WordSpec with ShouldMatchers {
  import TestUtils.stringToByteArray
  import TestUtils.byteArrayToString

  trait TestShardedStore extends ShardedStore with LocalFileSystem.TempRoot {
    override val rootName = "ShardedStoreTest"
    override val baseDir = root
  }

  def newStore(f: ShardedStore => Unit) = {
    val store = new TestShardedStore {
      val shardsTotal = 20
      val writers = 3
      val writerIndex = 0
    }
    try f(store)
    finally store.close()
  }

  "ShardedStore" should {

    "calculate owned shards" in newStore { store =>
      store.currentWriterIndex should be === 0
      store.ownedShards should be === Seq(0, 3, 6, 9, 12, 15, 18)
      store.close()
    }

    "cycle through shard writers" in newStore { store =>

      var expectedId = 0
      store.ownedShards.zipWithIndex foreach { case (shardId, index) =>
        store.currentWriterIndex should be === index % store.ownedShards.size

        val id = store.put(shardId.toString)
        id should be === expectedId

        expectedId += store.writers
      }
      store.close()
    }
  }
}