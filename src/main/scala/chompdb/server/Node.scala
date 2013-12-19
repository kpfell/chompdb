package chompdb.server

import chompdb.store._
import chompdb.DatabaseVersionShard
import f1lesystem.FileSystem
import f1lesystem.LocalFileSystem
import scala.collection._

case class Node(id: String)

// SWITCH TO TRAIT WHEN IMPLEMENTING
class NodeProtocol {
	def available(node: Node): Set[DatabaseVersionShard] = {
		val versionedStore = new VersionedStore { 
			val fs = new LocalFileSystem()
			val root = fs.root
		}

		versionedStore
			.versions
			.map( version => versionedStore
				.versionPath(version)
				.listFiles
				.filter(_.extension == "blob")
				.map( blobFile => 
					DatabaseVersionShard(version, 
						blobFile.basename.toInt, 
						blobFile, 
						blobFile.parent / (blobFile.basename + ".index")
					)					
				)
			)
			.map(_.toSet)
			.fold(Set[DatabaseVersionShard]())(_ ++ _)
	}
// 	def mapReduce(dv: DatabaseVersion, ids: Set[Long])(map: T => U)(reduce: (U, U) => U): U
}

// new AkkaProtocol(node.endpoint) {
// 	  // endpoint = "ec2-##-###-###-###.compute-1.amazonaws.com:8282"
// }