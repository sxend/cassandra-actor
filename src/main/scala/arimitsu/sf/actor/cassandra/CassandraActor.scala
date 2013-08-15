package arimitsu.sf.actor.cassandra

import akka.actor.Actor
import org.apache.cassandra.thrift._
import org.apache.thrift.async.{AsyncMethodCallback, TAsyncClientManager}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.thrift.transport.TNonblockingSocket
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{get_slice_call, set_keyspace_call}
import scala.collection.JavaConversions._
import arimitsu.sf.actor.cassandra.CassandraActorConversions._
import scala.concurrent.Promise

/**
 * User: sxend
 * Date: 13/07/09
 * Time: 21:34
 */
object CassandraActor {
  def apply(host: String, port: Int): CassandraActor = {
    new CassandraActor(host, port)
  }
}

/**
 * Cassandra Actor
 * One-Instance, One-Connection.
 * @param host
 * @param port
 */
class CassandraActor(val host: String, val port: Int) extends Actor {

  private[this] val transport = new TNonblockingSocket(host, port)
  private[this] val asyncClient: AsyncClient = new AsyncClient.Factory(new TAsyncClientManager(), new TBinaryProtocol.Factory()).getAsyncClient(transport)
  private[this] val methodQueue = new mutable.SynchronizedQueue[CassandraMethod]()
  private[this] val finish = new AtomicBoolean(false)

  def receive = {
    case getSlice: GetSlice => {
      val promise = Promise[Option[Seq[ColumnOrSuperColumn]]]()
      asyncClient.get_slice(getSlice.key, getSlice.columnParent, getSlice.predicate, getSlice.consistencyLevel, new AsyncMethodCallback[get_slice_call] {
        def onComplete(response: get_slice_call) {
          promise.success(Option(response.getResult))
          next()
        }

        def onError(exception: Exception) = {
          promise.failure(exception)
          next()
        }
      })
      sender ! promise.future
    }
    case setKeySpace: SetKeySpace => {
      val promise = Promise[Any]()
      asyncClient.set_keyspace(setKeySpace.keySpace, new AsyncMethodCallback[set_keyspace_call] {
        def onComplete(response: set_keyspace_call) {
          promise.success(response.getResult())
          next()
        }

        def onError(exception: Exception) {
          promise.failure(exception)
          next()
        }
      })
      sender ! promise.future
    }
  }

  def next() {
    if (!methodQueue.isEmpty) {
      self ! Next
    } else {
      finish.set(true)
    }

  }
}

private[cassandra] sealed trait CassandraTask

private[cassandra] case class Next() extends CassandraTask

private[cassandra] case class Exec() extends CassandraTask

private[cassandra] sealed trait CassandraMethod extends CassandraTask

case class Add() extends CassandraMethod

case class AtomicBatchMutate() extends CassandraMethod

case class BatchMutate() extends CassandraMethod

case class DescribeClusterName() extends CassandraMethod

case class DescribeKeySpace() extends CassandraMethod

case class DescribeKeySpaces() extends CassandraMethod

case class DescribePartitioner() extends CassandraMethod

case class DescribeRing() extends CassandraMethod

case class DescribeSchemaVersions() extends CassandraMethod

case class DescribeSnitch() extends CassandraMethod

case class DescribeSplits() extends CassandraMethod

case class DescribeSplitsEx() extends CassandraMethod

case class DescribeTokenMap() extends CassandraMethod

case class DescribeVersion() extends CassandraMethod

//case class ExecuteCql3Query(query: String, compression: Compression, consistencyLevel: ConsistencyLevel, success: Option[CqlResult] => Unit, failure: Throwable => Unit) extends CassandraMethod

case class ExecuteCqlQuery() extends CassandraMethod

case class ExecutePreparedCql3Query() extends CassandraMethod

case class ExecutePreparedCqlQuery() extends CassandraMethod

//case class Get(key: String, columnPath: ColumnPath, consistencyLevel: ConsistencyLevel, success: Option[ColumnOrSuperColumn] => Unit, failure: Throwable => Unit) extends CassandraMethod

case class GetCount() extends CassandraMethod

case class GetIndexedSlices() extends CassandraMethod

case class GetPagedSlice() extends CassandraMethod

case class GetRangeSlices() extends CassandraMethod

case class GetSlice(key: String, columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel) extends CassandraMethod

case class Insert() extends CassandraMethod

case class Login() extends CassandraMethod

case class MultiGetCount() extends CassandraMethod

case class MultiGetSlice() extends CassandraMethod

case class PrepareCql3Query() extends CassandraMethod

case class PrepareCqlQuery() extends CassandraMethod

case class Remove() extends CassandraMethod

case class RemoveCounter() extends CassandraMethod

case class SetCqlVersion() extends CassandraMethod

case class SetKeySpace(keySpace: String) extends CassandraMethod

case class SystemAddColumnFamily() extends CassandraMethod

case class SystemAddKeySpace() extends CassandraMethod

case class SystemDropColumnFamily() extends CassandraMethod

case class SystemDropKeySpace() extends CassandraMethod

case class SystemUpdateColumnFamily() extends CassandraMethod

case class SystemUpdateKeySpace() extends CassandraMethod

case class TraceNextQuery() extends CassandraMethod

case class Truncate() extends CassandraMethod