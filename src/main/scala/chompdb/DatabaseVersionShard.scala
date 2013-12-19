package chompdb

import f1lesystem.FileSystem

case class DatabaseVersionShard(
	version: Long,
	id: Int, 
	blobFile: FileSystem#File, 
	indexFile: FileSystem#File
)