import akka.actor.{ActorSystem, Actor}
import arimitsu.sf.cassandra.client.CassandraClient
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.cassandra.thrift._
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


/**
 * User: sxend
 * Date: 13/08/15
 * Time: 20:46
 */
object CassandraActorTest {
  val actorSystem = ActorSystem("CassandraActorTest")

  def main(args: Array[String]): Unit = {
    //    actorSystem.actorOf(Props[CassandraActorTest]) ! "start"
    val factory = new AsyncClient.Factory(new TAsyncClientManager(), new TBinaryProtocol.Factory())
    val client = CassandraClient(factory.getAsyncClient, new TNonblockingSocket("localhost", 9160))
    val r = client.setKeySpace("asf")
    Await.result(r, 5 seconds)
    val cp = new ColumnPath("app_config")
    cp.setColumn("api_key".getBytes)
    val nonstop = true
    while (nonstop) {
      client.get(ByteBufferUtil.bytes("facebook"), cp, ConsistencyLevel.ONE) onComplete {
        case Success(result) => println(new String(result.get.getColumn.getValue))
        case Failure(t) => println(t)
      }
    }
    while (!nonstop) {
      val f = client.get(ByteBufferUtil.bytes("facebook"), cp, ConsistencyLevel.ONE)
      val result = Await.result(f, 5 seconds)

      println(new String(result.get.getColumn.getValue))
    }
  }
}

class CassandraActorTest extends Actor {
  //  implicit val timeout = Timeout(5 seconds)

  def receive = {
    case _ => {
      //
      //      println("Test start.")
      //      var cassandraActor = CassandraActorTest.actorSystem.actorOf(Props(CassandraActor("localhost", 9160)))
      //      val setKeySpaceFuture = cassandraActor ? SetKeySpace("asf")
      //      setKeySpaceFuture.onSuccess {
      //        case _ => println("asf success")
      //      }
      //      val getSliceFuture: Future[Any] = cassandraActor ? GetSlice("" ,new ColumnParent(), new SlicePredicate(),ConsistencyLevel.ONE)
      //      getSliceFuture onSuccess {
      //        case option: Option[Seq[ColumnOrSuperColumn]] => {
      //          option match {
      //            case res@Some(Seq(ColumnOrSuperColumn)) => {
      //              res.get
      //            }
      //          }
      //        }
      //      }
      //      println("Test end.")
    }
  }
}
