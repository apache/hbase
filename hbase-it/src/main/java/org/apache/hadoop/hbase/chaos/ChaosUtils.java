package org.apache.hadoop.hbase.chaos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * ChaosUtils holds a bunch of useful functions like getting hostname and getting ZooKeeper quorum.
 */
@InterfaceAudience.Private
public class ChaosUtils {

  public static String getHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }


  public static String getZKQuorum(Configuration conf) {
    String port =
      Integer.toString(conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181));
    String[] serverHosts = conf.getStrings(HConstants.ZOOKEEPER_QUORUM, "localhost");
    for (int i = 0; i < serverHosts.length; i++) {
      serverHosts[i] = serverHosts[i] + ":" + port;
    }
    return String.join(",", serverHosts);
  }

}
