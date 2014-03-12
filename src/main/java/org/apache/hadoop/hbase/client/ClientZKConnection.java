package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * This class is responsible to handle connection and reconnection to a
 * zookeeper quorum.
 *
 */
public class ClientZKConnection implements Abortable, Watcher {

  private static String ZK_INSTANCE_NAME = HConnectionManager.class.getSimpleName();
  static final Log LOG = LogFactory.getLog(ClientZKConnection.class);
  private ZooKeeperWrapper zooKeeperWrapper;
  private Configuration conf;
  private boolean aborted = false;
  private int reconnectionTimes = 0;
  private int maxReconnectionTimes = 0;

  /**
   * Create a ClientZKConnection
   *
   * @param conf configuration
   */
  public ClientZKConnection(Configuration conf) {
    this.conf = conf;
    maxReconnectionTimes = conf.getInt(
        "hbase.client.max.zookeeper.reconnection", 3);
  }

  /**
   * Get the zookeeper wrapper for this connection, instantiate it if necessary.
   *
   * @return zooKeeperWrapper
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public synchronized ZooKeeperWrapper getZooKeeperWrapper() throws IOException {
    if (zooKeeperWrapper == null) {
      if (this.reconnectionTimes < this.maxReconnectionTimes) {
        zooKeeperWrapper = ZooKeeperWrapper.createInstance(conf,
            ZK_INSTANCE_NAME, this);
      } else {
        String msg = "HBase client failed to connection to zk after "
            + maxReconnectionTimes + " attempts";
        LOG.fatal(msg);
        throw new IOException(msg);
      }
    }
    return zooKeeperWrapper;
  }

  /**
   * Close this connection to zookeeper.
   */
  synchronized void closeZooKeeperConnection() {
    if (zooKeeperWrapper != null) {
      zooKeeperWrapper.close();
      zooKeeperWrapper = null;
    }
  }

  /**
   * Reset this connection to zookeeper.
   *
   * @throws IOException If there is any exception when reconnect to zookeeper
   */
  private synchronized void resetZooKeeperConnection() throws IOException {
    // close the zookeeper connection first
    closeZooKeeperConnection();
    // reconnect to zookeeper
    zooKeeperWrapper = ZooKeeperWrapper.createInstance(conf, ZK_INSTANCE_NAME,
        this);
  }

  @Override
  public synchronized void process(WatchedEvent event) {
    LOG.debug("Received ZK WatchedEvent: " + "[path=" + event.getPath() + "] "
        + "[state=" + event.getState().toString() + "] " + "[type="
        + event.getType().toString() + "]");
    if (event.getType() == EventType.None
        && event.getState() == KeeperState.SyncConnected) {
      LOG.info("Reconnected to ZooKeeper");
      // reset the reconnection times
      reconnectionTimes = 0;
    }
  }

  @Override
  public void abort(final String msg, Throwable t) {
    if (t != null && t instanceof KeeperException.SessionExpiredException) {
      try {
        reconnectionTimes++;
        LOG.info("This client just lost it's session with ZooKeeper, "
            + "trying the " + reconnectionTimes + " times to reconnect.");
        // reconnect to zookeeper if possible
        resetZooKeeperConnection();

        LOG.info("Reconnected successfully. This disconnect could have been"
            + " caused by a network partition or a long-running GC pause,"
            + " either way it's recommended that you verify your "
            + "environment.");
        this.aborted = false;
        return;
      } catch (IOException e) {
        LOG.error("Could not reconnect to ZooKeeper after session"
            + " expiration, aborting");
        t = e;
      }
    }
    if (t != null)
      LOG.fatal(msg, t);
    else
      LOG.fatal(msg);

    this.aborted = true;
    closeZooKeeperConnection();
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }
}
