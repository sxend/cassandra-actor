import akka.actor.{Props, ActorSystem, Actor}
import akka.util.Timeout
import arimitsu.sf.actor.cassandra.{GetSlice, SetKeySpace, CassandraActor}
import akka.pattern.ask
import org.apache.cassandra.thrift.{ColumnOrSuperColumn, SlicePredicate, ColumnParent, ConsistencyLevel}
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * User: sxend
 * Date: 13/08/15
 * Time: 20:46
 */
object CassandraActorTest {
  val actorSystem = ActorSystem("CassandraActorTest")

  def main(args: Array[String]): Unit = {
    actorSystem.actorOf(Props[CassandraActorTest]) ! "start"
  }
}

class CassandraActorTest extends Actor {
  implicit val timeout = Timeout(5 seconds)

  def receive = {
    case _ => {

      println("Test start.")
      var cassandraActor = CassandraActorTest.actorSystem.actorOf(Props(CassandraActor("localhost", 9160)))
      val setKeySpaceFuture = cassandraActor ? SetKeySpace("asf")
      setKeySpaceFuture.onSuccess {
        case _ => println("asf success")
      }
      val getSliceFuture: Future[Any] = cassandraActor ? GetSlice("" ,new ColumnParent(), new SlicePredicate(),ConsistencyLevel.ONE)
      getSliceFuture onSuccess {
        case option: Option[Seq[ColumnOrSuperColumn]] => {
          option match {
            case res@Some(Seq(ColumnOrSuperColumn)) => {
              res.get
            }
          }
        }
      }
      println("Test end.")
    }
  }
}
