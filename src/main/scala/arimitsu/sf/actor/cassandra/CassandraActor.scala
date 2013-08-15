package arimitsu.sf.actor.cassandra

import akka.actor.Actor
import org.apache.cassandra.thrift._
import org.apache.thrift.async.{AsyncMethodCallback, TAsyncClientManager}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.thrift.transport.TNonblockingSocket
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{execute_cql3_query_call, get_slice_call, get_call, set_keyspace_call}
import scala.collection.JavaConversions._
import arimitsu.sf.actor.cassandra.CassandraActorConversions._

/**
 * User: sxend
 * Date: 13/07/09
 * Time: 21:34
 */
object CassandraActor {
  def apply(host: String, port: Int, keySpace: String): CassandraActor = {
    new CassandraActor(host, port, keySpace)
  }
}

class CassandraActor(private val host: String, private val port: Int, private val keySpace: String) extends Actor {

  private[this] val transport = new TNonblockingSocket(host, port)
  private[this] val asyncClient: AsyncClient = new AsyncClient.Factory(new TAsyncClientManager(), new TBinaryProtocol.Factory()).getAsyncClient(transport)
  private[this] val queryQueue = new mutable.SynchronizedQueue[Query]()
  private[this] val finish = new AtomicBoolean(false)

  asyncClient.set_keyspace(keySpace, new AsyncMethodCallback[set_keyspace_call] {
    def onError(exception: Exception) {
      throw exception
    }

    def onComplete(response: set_keyspace_call) {
      finish.set(true)
      self ! Exec
    }
  })

  def receive = {
    case query: Query => {
      queryQueue.enqueue(query)
      self ! Exec
    }
    case Exec => {
      if (!queryQueue.isEmpty && finish.compareAndSet(true, false)) {
        self ! Next
      }
    }
    case Next => {
      if (!transport.isOpen) {
        transport.open()
      }
      val query = queryQueue.dequeue()
      query.method match {
        case get: Get => {
          asyncClient.get(get.key, get.columnPath, get.consistencyLevel, new AsyncMethodCallback[get_call] {
            def onComplete(response: get_call) {
              get.success(Option(response.getResult))
              next()
            }

            def onError(exception: Exception) = {
              get.failure(exception)
              next()
            }
          })
        }
        case getSlice: GetSlice => {
          asyncClient.get_slice(getSlice.key, getSlice.columnParent, getSlice.predicate, getSlice.consistencyLevel, new AsyncMethodCallback[get_slice_call] {
            def onComplete(response: get_slice_call) {
              getSlice.success(Option(response.getResult))
              next()
            }

            def onError(exception: Exception) = {
              getSlice.failure(exception)
              next()
            }
          })
        }
        case executeCql3Query: ExecuteCql3Query => {
          asyncClient.execute_cql3_query(executeCql3Query.query, executeCql3Query.compression, executeCql3Query.consistencyLevel, new AsyncMethodCallback[execute_cql3_query_call] {
            def onComplete(response: execute_cql3_query_call) {
              executeCql3Query.success(Option(response.getResult))
              next()
            }

            def onError(exception: Exception) = {
              executeCql3Query.failure(exception)
              next()
            }
          })
        }
        case _ => next()
      }
      def next() {
        if (!queryQueue.isEmpty) {
          self ! Next
        } else {
          finish.set(true)
        }
      }
    }
  }
}


sealed trait CassandraTask

case class Query(method: CassandraMethod) extends CassandraTask

private case class Next() extends CassandraTask

private case class Exec() extends CassandraTask

sealed trait CassandraMethod

case class Add() extends CassandraMethod

case class AtomicBatchMutate() extends CassandraMethod

case class BatchMutate() extends CassandraMethod

case class DescribeClusterName() extends CassandraMethod

case class DescribeKeyspace() extends CassandraMethod

case class DescribeKeyspaces() extends CassandraMethod

case class DescribePartitioner() extends CassandraMethod

case class DescribeRing() extends CassandraMethod

case class DescribeSchemaVersions() extends CassandraMethod

case class DescribeSnitch() extends CassandraMethod

case class DescribeSplits() extends CassandraMethod

case class DescribeSplitsEx() extends CassandraMethod

case class DescribeTokenMap() extends CassandraMethod

case class DescribeVersion() extends CassandraMethod

case class ExecuteCql3Query(query: String, compression: Compression, consistencyLevel: ConsistencyLevel, success: Option[CqlResult] => Unit, failure: Throwable => Unit) extends CassandraMethod

case class ExecuteCqlQuery() extends CassandraMethod

case class ExecutePreparedCql3Query() extends CassandraMethod

case class ExecutePreparedCqlQuery() extends CassandraMethod

case class Get(key: String, columnPath: ColumnPath, consistencyLevel: ConsistencyLevel, success: Option[ColumnOrSuperColumn] => Unit, failure: Throwable => Unit) extends CassandraMethod

case class GetCount() extends CassandraMethod

case class GetIndexedSlices() extends CassandraMethod

case class GetPagedSlice() extends CassandraMethod

case class GetRangeSlices() extends CassandraMethod

case class GetSlice(key: String, columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel, success: Option[Seq[ColumnOrSuperColumn]] => Unit, failure: Throwable => Unit) extends CassandraMethod

case class Insert() extends CassandraMethod

case class Login() extends CassandraMethod

case class MultigetCount() extends CassandraMethod

case class MultigetSlice() extends CassandraMethod

case class PrepareCql3Query() extends CassandraMethod

case class PrepareCqlQuery() extends CassandraMethod

case class Remove() extends CassandraMethod

case class RemoveCounter() extends CassandraMethod

case class SetCqlVersion() extends CassandraMethod

case class SetKeyspace() extends CassandraMethod

case class SystemAddColumnFamily() extends CassandraMethod

case class SystemAddKeyspace() extends CassandraMethod

case class SystemDropColumnFamily() extends CassandraMethod

case class SystemDropKeyspace() extends CassandraMethod

case class SystemUpdateColumnFamily() extends CassandraMethod

case class SystemUpdateKeyspace() extends CassandraMethod

case class TraceNextQuery() extends CassandraMethod

case class Truncate() extends CassandraMethod