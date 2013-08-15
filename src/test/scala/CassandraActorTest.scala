import akka.actor.{ActorRef, Props, ActorSystem, Actor}
import arimitsu.sf.actor.cassandra.{Query, CassandraActor}

/**
 * User: sxend
 * Date: 13/08/15
 * Time: 20:46
 */
object CassandraActorTest {

  def main(args: Array[String]):Unit = {
    println("Test start.")
    println("Test end.")
  }
}
class CassandraActorTest extends Actor {
  val actorSystem = ActorSystem("CassandraActorTest")

  def receive = {
    case _ => {
      var cassandraActor: ActorRef = actorSystem.actorOf(Props[CassandraActor])
      cassandraActor ! Query
    }
  }
}
