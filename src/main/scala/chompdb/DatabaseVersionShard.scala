package chompdb

import f1lesystem.FileSystem

// PROBLEM: No way to determine what database a shard belongs to
case class DatabaseVersionShard(
	version: Long,
	id: Int, 
	blobFile: FileSystem#File, 
	indexFile: FileSystem#File
)