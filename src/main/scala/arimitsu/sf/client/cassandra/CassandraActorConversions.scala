package arimitsu.sf.client.cassandra

import org.apache.cassandra.utils.ByteBufferUtil
import java.nio.ByteBuffer

/**
 * User: sxend
 * Date: 13/08/15
 * Time: 20:49
 */
private[cassandra] object CassandraActorConversions {
  private[cassandra] implicit def string2ByteBuffer(str: String): ByteBuffer = ByteBufferUtil.bytes(str)
}
