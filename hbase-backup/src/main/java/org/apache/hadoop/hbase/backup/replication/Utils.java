package org.apache.hadoop.hbase.backup.replication;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class Utils {
  private Utils() { }
  public static String logPeerId(String peerId) {
    return "[Source for peer " + peerId + "]:";
  }
}
