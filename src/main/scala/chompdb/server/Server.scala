package chompdb.server

import chompdb._
import f1lesystem.FileSystem
import scala.concurrent.Future

/*
object Server {
  sealed trait ChompStatus
  case object Loading extends ChompStatus
}

trait Server {
  import Server._

  val fs: FileSystem

  def root: fs.Path

  /** Returns domains for which database is responsible */
  def chomps: Map[String, Chomp]

  /** If an update is available on any domain, updates the domain's
   *  shards from its remote store and hotswaps in the new versions.
   */
  def attemptUpdate(domain: String): Future[Domain]

  /** If an update is available on any domain, updates the domain's
   *  shards from its remote store and hotswaps in the new versions.
   */
  def updateAll(): Future[Seq[Domain]]

  /** Returns true if all of the supplied `domains` are fully loaded. */
  def fullyLoaded(domains: Iterable[String]): Boolean

  /** Returns true if any of the supplied `domains` are loading. */
  def someLoading(domains: Iterable[String]): Boolean

  /** Returns the status of all domains */
  def status: Map[String, DomainStatus]

  /** "Walks through the supplied local directory, recursively deleting
   *  all directories with names that aren't present in the supplied
   * `domains`."
   */
  def purgeUnusedDomains(): Unit

  def launchUpdater(): Unit
}
*/

/*
trait DatabaseManager {
  def apply(localRoot: String, domains: Iterable[String], options: Map[String, Any]): Database
}
*/

/*
 * options:
 *
 *   replication -> 3
 *
 *   domains
 *     "foo" ->
 *       remoteStore -> ...
 *       localStore  -> ...
 *       serializer  -> ...
 *       status ->
 *       shardIndex -> ...
 *          hosts -> shards
 *          shards -> hosts
 *       domainData ->
 *          version -> 123456789
 *          shards ->
 *              1  -> ..
 *
 */
