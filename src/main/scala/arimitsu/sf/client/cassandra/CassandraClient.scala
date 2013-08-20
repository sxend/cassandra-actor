package arimitsu.sf.client.cassandra

import org.apache.cassandra.thrift._
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.thrift.async.AsyncMethodCallback
import org.apache.cassandra.thrift.Cassandra.AsyncClient._
import scala.concurrent.{Promise, Future}
import scala.collection.JavaConversions._
import java.nio.ByteBuffer

/**
 * User: sxend
 * Date: 13/08/17
 * Time: 0:21
 */
class CassandraClient(asyncClient: AsyncClient) {

  //  private[this] val transport = new TNonBlockingSocket(host, port)
  //  private[this] val asynchronousClient: AsyncClient = new AsyncClient.Factory(new TAsyncClientManager(), new TBinaryProtocol.Factory()).getAsyncClient(transport)

  def login(authRequest: AuthenticationRequest): Future[Unit] = {
    val promise = Promise[Unit]()
    asyncClient.login(authRequest, new AsyncMethodCallback[login_call]() {
      def onComplete(response: login_call) = promise.success()

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def setKeySpace(keySpace: String): Future[Unit] = {
    val promise = Promise[Unit]()
    asyncClient.set_keyspace(keySpace, new AsyncMethodCallback[set_keyspace_call]() {
      def onComplete(response: set_keyspace_call) = promise.success()

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def get(key: ByteBuffer, columnPath: ColumnPath, consistencyLevel: ConsistencyLevel): Future[Option[ColumnOrSuperColumn]] = {
    val promise = Promise[Option[ColumnOrSuperColumn]]()
    asyncClient.get(key, columnPath, consistencyLevel, new AsyncMethodCallback[get_call]() {
      def onComplete(response: get_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def getSlice(key: ByteBuffer, columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Seq[ColumnOrSuperColumn]]] = {
    val promise = Promise[Option[Seq[ColumnOrSuperColumn]]]()
    asyncClient.get_slice(key, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[get_slice_call]() {
      def onComplete(response: get_slice_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def getCount(key: ByteBuffer, columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel) = {
    val promise = Promise[Option[Int]]()
    asyncClient.get_count(key, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[get_count_call]() {
      def onComplete(response: get_count_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def multiGetSlice(keys: Seq[ByteBuffer], columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Map[ByteBuffer, Seq[ColumnOrSuperColumn]]]] = {
    implicit def convertJavaCollection(result: java.util.Map[ByteBuffer, java.util.List[ColumnOrSuperColumn]]): Map[ByteBuffer, Seq[ColumnOrSuperColumn]] = ??? // FIXME
    val promise = Promise[Option[Map[ByteBuffer, Seq[ColumnOrSuperColumn]]]]()
    asyncClient.multiget_slice(keys, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[multiget_slice_call]() {
      def onComplete(response: multiget_slice_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def multiGetCount(keys: Seq[ByteBuffer], columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Map[ByteBuffer, Int]]] = {
    implicit def convert(result: java.util.Map[ByteBuffer, Integer]): Map[ByteBuffer, Int] = ??? // FIXME
    val promise = Promise[Option[Map[ByteBuffer, Int]]]()
    asyncClient.multiget_count(keys, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[multiget_count_call]() {
      def onComplete(response: multiget_count_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def getRangeSlices(columnParent: ColumnParent, predicate: SlicePredicate, range: KeyRange, consistencyLevel: ConsistencyLevel): Future[Option[Seq[KeySlice]]] = {
    val promise = Promise[Option[Seq[KeySlice]]]()
    asyncClient.get_range_slices(columnParent, predicate, range, consistencyLevel, new AsyncMethodCallback[get_range_slices_call]() {
      def onComplete(response: get_range_slices_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def getPagedSlice(columnFamily: String, range: KeyRange, startColumn: ByteBuffer, consistencyLevel: ConsistencyLevel): Future[Option[Seq[KeySlice]]] = {
    val promise = Promise[Option[Seq[KeySlice]]]()
    asyncClient.get_paged_slice(columnFamily, range, startColumn, consistencyLevel, new AsyncMethodCallback[get_paged_slice_call]() {
      def onComplete(response: get_paged_slice_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def getIndexedSlices(columnParent: ColumnParent, indexClause: IndexClause, columnPredicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Seq[KeySlice]]] = {
    val promise = Promise[Option[Seq[KeySlice]]]()
    asyncClient.get_indexed_slices(columnParent, indexClause, columnPredicate, consistencyLevel, new AsyncMethodCallback[get_indexed_slices_call]() {
      def onComplete(response: get_indexed_slices_call) = promise.success(Option(response.getResult))

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def insert(key: ByteBuffer, columnParent: ColumnParent, column: Column, consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    asyncClient.insert(key, columnParent, column, consistencyLevel, new AsyncMethodCallback[insert_call]() {
      def onComplete(response: insert_call) = promise.success()

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def add(key: ByteBuffer, columnParent: ColumnParent, column: CounterColumn, consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    asyncClient.add(key, columnParent, column, consistencyLevel, new AsyncMethodCallback[add_call]() {
      def onComplete(response: add_call) = promise.success()

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def remove(key: ByteBuffer, columnPath: ColumnPath, timestamp: Long, consistencyLevel: ConsistencyLevel): Future[Unit] = {
    val promise = Promise[Unit]()
    asyncClient.remove(key, columnPath, timestamp, consistencyLevel, new AsyncMethodCallback[remove_call]() {
      def onComplete(response: remove_call) = promise.success()

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def removeCounter(key: ByteBuffer, columnPath: ColumnPath, consistencyLevel: ConsistencyLevel, resultHandler: remove_counter_call): Future[Unit] = {
    val promise = Promise[Unit]()
    asyncClient.remove_counter(key, columnPath, consistencyLevel, new AsyncMethodCallback[remove_counter_call]() {
      def onComplete(response: remove_counter_call) = promise.success()

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }

  def batchMutate(mutationMap: Map[ByteBuffer, Map[String, Seq[Mutation]]], consistencyLevel: ConsistencyLevel, resultHandler: batch_mutate_call): Future[Unit] = {
    implicit def convert(result: Map[ByteBuffer, Map[String, Seq[Mutation]]]): java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = ??? // FIXME
    val promise = Promise[Unit]()
    asyncClient.batch_mutate(mutationMap, consistencyLevel, new AsyncMethodCallback[batch_mutate_call]() {
      def onComplete(response: batch_mutate_call) = promise.success()

      def onError(exception: Exception) = promise.failure(exception)
    })
    promise.future
  }


  def atomicBatchMutate(mutation_map: Map[ByteBuffer, Map[String, Seq[Mutation]]], consistency_level: ConsistencyLevel, resultHandler: atomic_batch_mutate_call) = {

    ???
  }

  def truncate(cfName: String, resultHandler: truncate_call) = {
    ???
  }

  def describeSchemaVersions(resultHandler: describe_schema_versions_call) = {
    ???
  }

  def describeKeySpaces(resultHandler: describe_keyspaces_call) = {
    ???
  }

  def describeClusterName(resultHandler: describe_cluster_name_call) = {
    ???
  }

  def describeVersion(resultHandler: describe_version_call) = {
    ???
  }

  def describeRing(keySpace: String, resultHandler: describe_ring_call) = {
    ???
  }

  def describeTokenMap(resultHandler: describe_token_map_call) = {
    ???
  }

  def describePartitioner(resultHandler: describe_partitioner_call) = {
    ???
  }

  def describeSnitch(resultHandler: describe_snitch_call) = {
    ???
  }

  def describeKeySpace(keySpace: String, resultHandler: describe_keyspace_call) = {
    ???
  }

  def describeSplits(cfName: String, start_token: String, end_token: String, keys_per_split: Int, resultHandler: describe_splits_call) = {
    ???
  }

  def traceNextQuery(resultHandler: trace_next_query_call) = {
    ???
  }

  def describeSplitsEx(cfName: String, start_token: String, end_token: String, keys_per_split: Int, resultHandler: describe_splits_ex_call) = {
    ???
  }

  def systemAddColumnFamily(cf_def: CfDef, resultHandler: system_add_column_family_call) = {
    ???
  }

  def systemDropColumnFamily(column_family: String, resultHandler: system_drop_column_family_call) = {
    ???
  }

  def systemAddKeySpace(ks_def: KsDef, resultHandler: system_add_keyspace_call) = {
    ???
  }

  def systemDropKeySpace(keySpace: String, resultHandler: system_drop_keyspace_call) = {
    ???
  }

  def systemUpdateKeySpace(ksDef: KsDef, resultHandler: system_update_keyspace_call) = {
    ???
  }

  def systemUpdateColumnFamily(cfDef: CfDef, resultHandler: system_update_column_family_call) = {
    ???
  }

  def executeCqlQuery(query: ByteBuffer, compression: Compression, resultHandler: execute_cql_query_call) = {
    ???
  }

  def executeCql3Query(query: ByteBuffer, compression: Compression, consistency: ConsistencyLevel, resultHandler: execute_cql3_query_call) = {
    ???
  }

  def prepareCqlQuery(query: ByteBuffer, compression: Compression, resultHandler: prepare_cql_query_call) = {
    ???
  }

  def prepareCql3Query(query: ByteBuffer, compression: Compression, resultHandler: prepare_cql3_query_call) = {
    ???
  }

  def executePreparedCqlQuery(itemId: Int, values: Seq[ByteBuffer], resultHandler: execute_prepared_cql_query_call) = {
    ???
  }

  def executePreparedCql3Query(itemId: Int, values: Seq[ByteBuffer], consistency: ConsistencyLevel, resultHandler: execute_prepared_cql3_query_call) = {
    ???
  }

  def setCqlVersion(version: String, resultHandler: set_cql_version_call) = {
    ???
  }

  //  def getSlice(rowKey: String, columnParent: ColumnParent, predicate: SlicePredicate, consistencyLevel: ConsistencyLevel): Future[Option[Seq[ColumnOrSuperColumn]]] = {
  //    val promise = Promise[Option[Seq[ColumnOrSuperColumn]]]()
  //    asynchronousClient.get_slice(rowKey, columnParent, predicate, consistencyLevel, new AsyncMethodCallback[get_slice_call] {
  //      def onComplete(response: get_slice_call) = promise.success(Option(response.getResult)) = {???}//
  //      def onError(exception: Exception) = promise.failure(exception) = {???}//    })
  //    promise.future
  //  }

  //  def setKeySpace(keySpace: String): Future[Unit] = {
  //    val promise = Promise[Unit]()
  //    asynchronousClient.set_keyspace(keySpace, new AsyncMethodCallback[set_keyspace_call] {
  //      def onComplete(response: set_keyspace_call) = promise.success(response.getResult()) = {???}//
  //      def onError(exception: Exception) = promise.failure(exception) = {???}//    })
  //    promise.future
  //  }

}
