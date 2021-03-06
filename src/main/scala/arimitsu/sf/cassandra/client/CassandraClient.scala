package arimitsu.sf.cassandra.client

import org.apache.cassandra.thrift._
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.thrift.async.AsyncMethodCallback
import org.apache.cassandra.thrift.Cassandra.AsyncClient._
import org.apache.thrift.transport.TNonblockingSocket
import scala.concurrent.{Promise, Future}
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import java.util

/**
 * User: sxend
 * Date: 13/08/17
 * Time: 0:21
 */
object CassandraClient {
  def apply(asyncClientFactory: (TNonblockingSocket) => AsyncClient, transport: TNonblockingSocket): CassandraClient = new CassandraClient(asyncClientFactory, transport)
}

class CassandraClient(private val asyncClientFactory: (TNonblockingSocket) => AsyncClient, private val transport: TNonblockingSocket) {

  private implicit def convertMutationMap(param: Map[ByteBuffer, Map[String, Seq[Mutation]]]): java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = {
    val resultMap = new util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]()
    param.foreach(f => {
      val childMap: java.util.Map[String, java.util.List[Mutation]] = new util.HashMap[String, java.util.List[Mutation]]()

      f._2.foreach(c => {
        childMap.put(c._1, c._2)
      })
      resultMap.put(f._1, childMap)
    })
    resultMap
  }

  private[this] def asyncClient: AsyncClient = asyncClientFactory(transport)

  def login(authRequest: AuthenticationRequest): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.login(authRequest, new AsyncMethodCallback[login_call]() {
        def onComplete(response: login_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }


  def setKeySpace(keySpace: String): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.set_keyspace(keySpace, new AsyncMethodCallback[set_keyspace_call]() {
        def onComplete(response: set_keyspace_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def get(key: ByteBuffer, columnPath: ColumnPath, consistencyLevel: ConsistencyLevel): Future[Option[ColumnOrSuperColumn]] = {
    val promise = Promise[Option[ColumnOrSuperColumn]]()
    try {
      val client = asyncClient
      //      println(client)

      client.get(key, columnPath, consistencyLevel, new AsyncMethodCallback[get_call]() {
        def onComplete(response: get_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })

    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future

  }

  def getSlice(key: ByteBuffer, columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Seq[ColumnOrSuperColumn]]] = {
    val promise = Promise[Option[Seq[ColumnOrSuperColumn]]]()
    try {
      asyncClient.get_slice(key, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[get_slice_call]() {
        def onComplete(response: get_slice_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def getCount(key: ByteBuffer, columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel) = {
    val promise = Promise[Option[Int]]()
    try {
      asyncClient.get_count(key, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[get_count_call]() {
        def onComplete(response: get_count_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def multiGetSlice(keys: Seq[ByteBuffer], columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Map[ByteBuffer, Seq[ColumnOrSuperColumn]]]] = {
    val promise = Promise[Option[Map[ByteBuffer, Seq[ColumnOrSuperColumn]]]]()
    try {
      asyncClient.multiget_slice(keys, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[multiget_slice_call]() {
        def onComplete(response: multiget_slice_call) = promise.success(Option(response.getResult.map((m) => (m._1, m._2.toList.toSeq)).toMap))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def multiGetCount(keys: Seq[ByteBuffer], columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Map[ByteBuffer, Int]]] = {
    val promise = Promise[Option[Map[ByteBuffer, Int]]]()
    try {
      asyncClient.multiget_count(keys, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[multiget_count_call]() {
        def onComplete(response: multiget_count_call) = promise.success(Option(response.getResult.map(m => (m._1, m._2.toInt)).toMap))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def getRangeSlices(columnParent: ColumnParent, predicate: SlicePredicate, range: KeyRange, consistencyLevel: ConsistencyLevel): Future[Option[Seq[KeySlice]]] = {
    val promise = Promise[Option[Seq[KeySlice]]]()
    try {
      asyncClient.get_range_slices(columnParent, predicate, range, consistencyLevel, new AsyncMethodCallback[get_range_slices_call]() {
        def onComplete(response: get_range_slices_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def getPagedSlice(columnFamily: String, range: KeyRange, startColumn: ByteBuffer, consistencyLevel: ConsistencyLevel): Future[Option[Seq[KeySlice]]] = {
    val promise = Promise[Option[Seq[KeySlice]]]()
    try {
      asyncClient.get_paged_slice(columnFamily, range, startColumn, consistencyLevel, new AsyncMethodCallback[get_paged_slice_call]() {
        def onComplete(response: get_paged_slice_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def getIndexedSlices(columnParent: ColumnParent, indexClause: IndexClause, columnPredicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Seq[KeySlice]]] = {
    val promise = Promise[Option[Seq[KeySlice]]]()
    try {
      asyncClient.get_indexed_slices(columnParent, indexClause, columnPredicate, consistencyLevel, new AsyncMethodCallback[get_indexed_slices_call]() {
        def onComplete(response: get_indexed_slices_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def insert(key: ByteBuffer, columnParent: ColumnParent, column: Column, consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.insert(key, columnParent, column, consistencyLevel, new AsyncMethodCallback[insert_call]() {
        def onComplete(response: insert_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def add(key: ByteBuffer, columnParent: ColumnParent, column: CounterColumn, consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.add(key, columnParent, column, consistencyLevel, new AsyncMethodCallback[add_call]() {
        def onComplete(response: add_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def remove(key: ByteBuffer, columnPath: ColumnPath, timestamp: Long, consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.remove(key, columnPath, timestamp, consistencyLevel, new AsyncMethodCallback[remove_call]() {
        def onComplete(response: remove_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def removeCounter(key: ByteBuffer, columnPath: ColumnPath, consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.remove_counter(key, columnPath, consistencyLevel, new AsyncMethodCallback[remove_counter_call]() {
        def onComplete(response: remove_counter_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  implicit def convert[K, V](map: Map[K, V]): java.util.Map[K, V] = {
    val jMap = new util.HashMap[K, V]()
    map.foreach(m => {
      jMap.put(m._1, m._2)
    })
    jMap
  }

  def batchMutate(mutationMap: Map[ByteBuffer, Map[String, Seq[Mutation]]], consistencyLevel: ConsistencyLevel): Future[Unit] = {

    val promise = Promise[Unit]()
    try {
      asyncClient.batch_mutate(mutationMap, consistencyLevel, new AsyncMethodCallback[batch_mutate_call]() {
        def onComplete(response: batch_mutate_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }


  def atomicBatchMutate(mutationMap: Map[ByteBuffer, Map[String, Seq[Mutation]]], consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.atomic_batch_mutate(mutationMap, consistencyLevel, new AsyncMethodCallback[atomic_batch_mutate_call]() {
        def onComplete(response: atomic_batch_mutate_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def truncate(cfName: String, resultHandler: truncate_call): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.truncate(cfName, new AsyncMethodCallback[truncate_call]() {
        def onComplete(response: truncate_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeSchemaVersions(resultHandler: describe_schema_versions_call): Future[Option[Map[String, Seq[String]]]] = {
    val promise = Promise[Option[Map[String, Seq[String]]]]()
    try {
      asyncClient.describe_schema_versions(new AsyncMethodCallback[describe_schema_versions_call]() {
        def onComplete(response: describe_schema_versions_call) = promise.success(Option(response.getResult.map(m => (m._1, m._2.toList)).toMap))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeKeySpaces(resultHandler: describe_keyspaces_call): Future[Option[Seq[KsDef]]] = {
    val promise = Promise[Option[Seq[KsDef]]]()
    try {
      asyncClient.describe_keyspaces(new AsyncMethodCallback[describe_keyspaces_call]() {
        def onComplete(response: describe_keyspaces_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeClusterName(resultHandler: describe_cluster_name_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.describe_cluster_name(new AsyncMethodCallback[describe_cluster_name_call]() {
        def onComplete(response: describe_cluster_name_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeVersion(resultHandler: describe_version_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.describe_version(new AsyncMethodCallback[describe_version_call]() {
        def onComplete(response: describe_version_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeRing(keySpace: String, resultHandler: describe_ring_call): Future[Option[Seq[TokenRange]]] = {
    val promise = Promise[Option[Seq[TokenRange]]]()
    try {
      asyncClient.describe_ring(keySpace, new AsyncMethodCallback[describe_ring_call]() {
        def onComplete(response: describe_ring_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeTokenMap(resultHandler: describe_token_map_call): Future[Option[Map[String, String]]] = {
    val promise = Promise[Option[Map[String, String]]]()
    try {
      asyncClient.describe_token_map(new AsyncMethodCallback[describe_token_map_call]() {
        def onComplete(response: describe_token_map_call) = promise.success(Option(response.getResult.toMap[String, String]))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describePartitioner(resultHandler: describe_partitioner_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.describe_partitioner(new AsyncMethodCallback[describe_partitioner_call]() {
        def onComplete(response: describe_partitioner_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeSnitch(resultHandler: describe_snitch_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.describe_snitch(new AsyncMethodCallback[describe_snitch_call]() {
        def onComplete(response: describe_snitch_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeKeySpace(keySpace: String, resultHandler: describe_keyspace_call): Future[Option[KsDef]] = {
    val promise = Promise[Option[KsDef]]()
    try {
      asyncClient.describe_keyspace(keySpace, new AsyncMethodCallback[describe_keyspace_call]() {
        def onComplete(response: describe_keyspace_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeSplits(cfName: String, startToken: String, endToken: String, keysPerSplit: Int, resultHandler: describe_splits_call): Future[Option[Seq[String]]] = {
    val promise = Promise[Option[Seq[String]]]()
    try {
      asyncClient.describe_splits(cfName, startToken, endToken, keysPerSplit, new AsyncMethodCallback[describe_splits_call]() {
        def onComplete(response: describe_splits_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def traceNextQuery(resultHandler: trace_next_query_call): Future[Option[ByteBuffer]] = {
    val promise = Promise[Option[ByteBuffer]]()
    try {
      asyncClient.trace_next_query(new AsyncMethodCallback[trace_next_query_call]() {
        def onComplete(response: trace_next_query_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def describeSplitsEx(cfName: String, startToken: String, endToken: String, keysPerSplit: Int, resultHandler: describe_splits_ex_call): Future[Option[Seq[CfSplit]]] = {
    val promise = Promise[Option[Seq[CfSplit]]]()
    try {
      asyncClient.describe_splits_ex(cfName, startToken, endToken, keysPerSplit, new AsyncMethodCallback[describe_splits_ex_call]() {
        def onComplete(response: describe_splits_ex_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def systemAddColumnFamily(cfDef: CfDef, resultHandler: system_add_column_family_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.system_add_column_family(cfDef, new AsyncMethodCallback[system_add_column_family_call]() {
        def onComplete(response: system_add_column_family_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def systemDropColumnFamily(columnFamily: String, resultHandler: system_drop_column_family_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.system_drop_column_family(columnFamily, new AsyncMethodCallback[system_drop_column_family_call]() {
        def onComplete(response: system_drop_column_family_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def systemAddKeySpace(ksDef: KsDef, resultHandler: system_add_keyspace_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.system_add_keyspace(ksDef, new AsyncMethodCallback[system_add_keyspace_call]() {
        def onComplete(response: system_add_keyspace_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def systemDropKeySpace(keySpace: String, resultHandler: system_drop_keyspace_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.system_drop_keyspace(keySpace, new AsyncMethodCallback[system_drop_keyspace_call]() {
        def onComplete(response: system_drop_keyspace_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def systemUpdateKeySpace(ksDef: KsDef, resultHandler: system_update_keyspace_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.system_update_keyspace(ksDef, new AsyncMethodCallback[system_update_keyspace_call]() {
        def onComplete(response: system_update_keyspace_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def systemUpdateColumnFamily(cfDef: CfDef, resultHandler: system_update_column_family_call): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    try {
      asyncClient.system_update_column_family(cfDef, new AsyncMethodCallback[system_update_column_family_call]() {
        def onComplete(response: system_update_column_family_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def executeCqlQuery(query: ByteBuffer, compression: Compression, resultHandler: execute_cql_query_call): Future[Option[CqlResult]] = {
    val promise = Promise[Option[CqlResult]]()
    try {
      asyncClient.execute_cql_query(query, compression, new AsyncMethodCallback[execute_cql_query_call]() {
        def onComplete(response: execute_cql_query_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def executeCql3Query(query: ByteBuffer, compression: Compression, consistency: ConsistencyLevel, resultHandler: execute_cql3_query_call): Future[Option[CqlResult]] = {
    val promise = Promise[Option[CqlResult]]()
    try {
      asyncClient.execute_cql3_query(query, compression, consistency, new AsyncMethodCallback[execute_cql3_query_call]() {
        def onComplete(response: execute_cql3_query_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def prepareCqlQuery(query: ByteBuffer, compression: Compression, resultHandler: prepare_cql_query_call): Future[Option[CqlPreparedResult]] = {
    val promise = Promise[Option[CqlPreparedResult]]()
    try {
      asyncClient.prepare_cql_query(query, compression, new AsyncMethodCallback[prepare_cql_query_call]() {
        def onComplete(response: prepare_cql_query_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def prepareCql3Query(query: ByteBuffer, compression: Compression, resultHandler: prepare_cql3_query_call): Future[Option[CqlPreparedResult]] = {
    val promise = Promise[Option[CqlPreparedResult]]()
    try {
      asyncClient.prepare_cql3_query(query, compression, new AsyncMethodCallback[prepare_cql3_query_call]() {
        def onComplete(response: prepare_cql3_query_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def executePreparedCqlQuery(itemId: Int, values: Seq[ByteBuffer], resultHandler: execute_prepared_cql_query_call): Future[Option[CqlResult]] = {
    val promise = Promise[Option[CqlResult]]()
    try {
      asyncClient.execute_prepared_cql_query(itemId, values, new AsyncMethodCallback[execute_prepared_cql_query_call]() {
        def onComplete(response: execute_prepared_cql_query_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def executePreparedCql3Query(itemId: Int, values: Seq[ByteBuffer], consistency: ConsistencyLevel, resultHandler: execute_prepared_cql3_query_call): Future[Option[CqlResult]] = {
    val promise = Promise[Option[CqlResult]]()
    try {
      asyncClient.execute_prepared_cql3_query(itemId, values, consistency, new AsyncMethodCallback[execute_prepared_cql3_query_call]() {
        def onComplete(response: execute_prepared_cql3_query_call) = promise.success(Option(response.getResult))

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

  def setCqlVersion(version: String, resultHandler: set_cql_version_call): Future[Unit] = {
    val promise = Promise[Unit]()
    try {
      asyncClient.set_cql_version(version, new AsyncMethodCallback[set_cql_version_call]() {
        def onComplete(response: set_cql_version_call) = promise.success()

        def onError(exception: Exception) = promise.failure(exception)
      })
    } catch {
      case t: Throwable => promise.failure(t)
    }
    promise.future
  }

}
