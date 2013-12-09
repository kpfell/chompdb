package chompdb.sharding
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers


@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class IdGeneratorTest extends WordSpec with ShouldMatchers {

  "IdGenerator" should {
    "calculate next id and return last id" in {
      val next = new IdGenerator(new Sharded {
        val shardsIndex = 1
        val shardsTotal = 10
      })
      
      next.nextId() should be === 1
      next.lastId should be === 1
      
      next.nextId() should be === 11
      next.lastId should be === 11
      
      next.nextId() should be === 21
      next.lastId should be === 21
    }
    
    "split" in {
      val shard1 = new Sharded {
        val shardsIndex = 1
        val shardsTotal = 10
      }
      val shard2 = shard1.split(index = 2, splits = 3) 
      
      shard2.shardsIndex should be === 5
      shard2.shardsTotal should be === 30
      
      locally {
        val next = new IdGenerator(shard2)
        next.nextId() should be === 5
        next.nextId() should be === 35
        next.nextId() should be === 65
      }
      
      val shard3 = shard1.split(index = 3, splits = 3) 
      locally {
        val next = new IdGenerator(shard3)
        next.nextId() should be === 6
        next.nextId() should be === 36
        next.nextId() should be === 66
      }
    }
  }
}