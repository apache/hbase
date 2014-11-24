/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.RpcClient.FailedServerException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Tracks the availability of the catalog tables
 * <code>hbase:meta</code>.
 *
 * This class is "read-only" in that the locations of the catalog tables cannot
 * be explicitly set.  Instead, ZooKeeper is used to learn of the availability
 * and location of <code>hbase:meta</code>.
 *
 * <p>Call {@link #start()} to start up operation.  Call {@link #stop()}} to
 * interrupt waits and close up shop.
 */
@InterfaceAudience.Private
public class CatalogTracker {
  // TODO JDC 11/30 We don't even have ROOT anymore, revisit
  // TODO: This class needs a rethink.  The original intent was that it would be
  // the one-stop-shop for meta locations and that it would get this
  // info from reading and watching zk state.  The class was to be used by
  // servers when they needed to know of meta movement but also by
  // client-side (inside in HTable) so rather than figure meta
  // locations on fault, the client would instead get notifications out of zk.
  //
  // But this original intent is frustrated by the fact that this class has to
  // read an hbase table, the -ROOT- table, to figure out the hbase:meta region
  // location which means we depend on an HConnection.  HConnection will do
  // retrying but also, it has its own mechanism for finding root and meta
  // locations (and for 'verifying'; it tries the location and if it fails, does
  // new lookup, etc.).  So, at least for now, HConnection (or HTable) can't
  // have a CT since CT needs a HConnection (Even then, do want HT to have a CT?
  // For HT keep up a session with ZK?  Rather, shouldn't we do like asynchbase
  // where we'd open a connection to zk, read what we need then let the
  // connection go?).  The 'fix' is make it so both root and meta addresses
  // are wholey up in zk -- not in zk (root) -- and in an hbase table (meta).
  //
  // But even then, this class does 'verification' of the location and it does
  // this by making a call over an HConnection (which will do its own root
  // and meta lookups).  Isn't this verification 'useless' since when we
  // return, whatever is dependent on the result of this call then needs to
  // use HConnection; what we have verified may change in meantime (HConnection
  // uses the CT primitives, the root and meta trackers finding root locations).
  //
  // When meta is moved to zk, this class may make more sense.  In the
  // meantime, it does not cohere.  It should just watch meta and root and not
  // NOT do verification -- let that be out in HConnection since its going to
  // be done there ultimately anyways.
  //
  // This class has spread throughout the codebase.  It needs to be reigned in.
  // This class should be used server-side only, even if we move meta location
  // up into zk.  Currently its used over in the client package. Its used in
  // MetaReader and MetaEditor classes usually just to get the Configuration
  // its using (It does this indirectly by asking its HConnection for its
  // Configuration and even then this is just used to get an HConnection out on
  // the other end). I made https://issues.apache.org/jira/browse/HBASE-4495 for
  // doing CT fixup. St.Ack 09/30/2011.
  //

  // TODO: Timeouts have never been as advertised in here and its worse now
  // with retries; i.e. the HConnection retries and pause goes ahead whatever
  // the passed timeout is.  Fix.
  private static final Log LOG = LogFactory.getLog(CatalogTracker.class);
  private final HConnection connection;
  private final ZooKeeperWatcher zookeeper;
  private final MetaRegionTracker metaRegionTracker;
  private boolean instantiatedzkw = false;
  private Abortable abortable;

  private volatile boolean stopped = false;

  static final byte [] META_REGION_NAME =
    HRegionInfo.FIRST_META_REGIONINFO.getRegionName();

  /**
   * Constructs a catalog tracker. Find current state of catalog tables.
   * Begin active tracking by executing {@link #start()} post construction. Does
   * not timeout.
   *
   * @param conf
   *          the {@link Configuration} from which a {@link HConnection} will be
   *          obtained; if problem, this connections
   *          {@link HConnection#abort(String, Throwable)} will be called.
   * @throws IOException
   */
  public CatalogTracker(final Configuration conf) throws IOException {
    this(null, conf, null);
  }

  /**
   * Constructs the catalog tracker.  Find current state of catalog tables.
   * Begin active tracking by executing {@link #start()} post construction.
   * Does not timeout.
   * @param zk If zk is null, we'll create an instance (and shut it down
   * when {@link #stop()} is called) else we'll use what is passed.
   * @param conf
   * @param abortable If fatal exception we'll call abort on this.  May be null.
   * If it is we'll use the Connection associated with the passed
   * {@link Configuration} as our Abortable.
   * @throws IOException
   */
  public CatalogTracker(final ZooKeeperWatcher zk, final Configuration conf,
      Abortable abortable)
  throws IOException {
    this(zk, conf, HConnectionManager.getConnection(conf), abortable);
  }

  public CatalogTracker(final ZooKeeperWatcher zk, final Configuration conf,
      HConnection connection, Abortable abortable)
  throws IOException {
    this.connection = connection;
    if (abortable == null) {
      // A connection is abortable.
      this.abortable = this.connection;
    }
    Abortable throwableAborter = new Abortable() {

      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(why, e);
      }

      @Override
      public boolean isAborted() {
        return true;
      }

    };
    if (zk == null) {
      // Create our own.  Set flag so we tear it down on stop.
      this.zookeeper =
        new ZooKeeperWatcher(conf, "catalogtracker-on-" + connection.toString(),
          abortable);
      instantiatedzkw = true;
    } else {
      this.zookeeper = zk;
    }
    this.metaRegionTracker = new MetaRegionTracker(zookeeper, throwableAborter);
  }

  /**
   * Starts the catalog tracker.
   * Determines current availability of catalog tables and ensures all further
   * transitions of either region are tracked.
   * @throws IOException
   * @throws InterruptedException
   */
  public void start() throws IOException, InterruptedException {
    LOG.debug("Starting catalog tracker " + this);
    try {
      this.metaRegionTracker.start();
    } catch (RuntimeException e) {
      Throwable t = e.getCause();
      this.abortable.abort(e.getMessage(), t);
      throw new IOException("Attempt to start meta tracker failed.", t);
    }
  }

  /**
   * @return True if we are stopped. Call only after start else indeterminate answer.
   */
  @VisibleForTesting
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Stop working.
   * Interrupts any ongoing waits.
   */
  public void stop() {
    if (!this.stopped) {
      LOG.debug("Stopping catalog tracker " + this);
      this.stopped = true;
      this.metaRegionTracker.stop();
      try {
        if (this.connection != null) {
          this.connection.close();
        }
      } catch (IOException e) {
        // Although the {@link Closeable} interface throws an {@link
        // IOException}, in reality, the implementation would never do that.
        LOG.error("Attempt to close catalog tracker's connection failed.", e);
      }
      if (this.instantiatedzkw) {
        this.zookeeper.close();
      }
    }
  }

  /**
   * Gets the current location for <code>hbase:meta</code> or null if location is
   * not currently available.
   * @return {@link ServerName} for server hosting <code>hbase:meta</code> or null
   * if none available
   * @throws InterruptedException
   */
  public ServerName getMetaLocation() throws InterruptedException {
    return this.metaRegionTracker.getMetaRegionLocation();
  }

  /**
   * Checks whether meta regionserver znode has some non null data.
   * @return true if data is not null, false otherwise.
   */
  public boolean isMetaLocationAvailable() {
    return this.metaRegionTracker.isLocationAvailable();
  }
  /**
   * Gets the current location for <code>hbase:meta</code> if available and waits
   * for up to the specified timeout if not immediately available.  Returns null
   * if the timeout elapses before root is available.
   * @param timeout maximum time to wait for root availability, in milliseconds
   * @return {@link ServerName} for server hosting <code>hbase:meta</code> or null
   * if none available
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException if meta not available before
   * timeout
   */
  public ServerName waitForMeta(final long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    ServerName sn = metaRegionTracker.waitMetaRegionLocation(timeout);
    if (sn == null) {
      throw new NotAllMetaRegionsOnlineException("Timed out; " + timeout + "ms");
    }
    return sn;
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @param timeout How long to wait on meta location
   * @see #waitForMeta for additional information
   * @return connection to server hosting meta
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   * @deprecated Use #getMetaServerConnection(long)
   */
  public AdminService.BlockingInterface waitForMetaServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getMetaServerConnection(timeout);
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * <p>WARNING: Does not retry.  Use an {@link HTable} instead.
   * @param timeout How long to wait on meta location
   * @see #waitForMeta for additional information
   * @return connection to server hosting meta
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  AdminService.BlockingInterface getMetaServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForMeta(timeout));
  }

  /**
   * Waits indefinitely for availability of <code>hbase:meta</code>.  Used during
   * cluster startup.  Does not verify meta, just that something has been
   * set up in zk.
   * @see #waitForMeta(long)
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForMeta() throws InterruptedException {
    Stopwatch stopwatch = new Stopwatch().start();
    while (!this.stopped) {
      try {
        if (waitForMeta(100) != null) break;
        long sleepTime = stopwatch.elapsedMillis();
        // +1 in case sleepTime=0
        if ((sleepTime + 1) % 10000 == 0) {
          LOG.warn("Have been waiting for meta to be assigned for " + sleepTime + "ms");
        }
      } catch (NotAllMetaRegionsOnlineException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("hbase:meta still not available, sleeping and retrying." +
          " Reason: " + e.getMessage());
        }
      }
    }
  }

  /**
   * @param sn ServerName to get a connection against.
   * @return The AdminProtocol we got when we connected to <code>sn</code>
   * May have come from cache, may not be good, may have been setup by this
   * invocation, or may be null.
   * @throws IOException
   */
  private AdminService.BlockingInterface getCachedConnection(ServerName sn)
  throws IOException {
    if (sn == null) {
      return null;
    }
    AdminService.BlockingInterface service = null;
    try {
      service = connection.getAdmin(sn);
    } catch (RetriesExhaustedException e) {
      if (e.getCause() != null && e.getCause() instanceof ConnectException) {
        // Catch this; presume it means the cached connection has gone bad.
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      LOG.debug("Timed out connecting to " + sn);
    } catch (NoRouteToHostException e) {
      LOG.debug("Connecting to " + sn, e);
    } catch (SocketException e) {
      LOG.debug("Exception connecting to " + sn);
    } catch (UnknownHostException e) {
      LOG.debug("Unknown host exception connecting to  " + sn);
    } catch (FailedServerException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Server " + sn + " is in failed server list.");
      }
    } catch (IOException ioe) {
      Throwable cause = ioe.getCause();
      if (ioe instanceof ConnectException) {
        // Catch. Connect refused.
      } else if (cause != null && cause instanceof EOFException) {
        // Catch. Other end disconnected us.
      } else if (cause != null && cause.getMessage() != null &&
        cause.getMessage().toLowerCase().contains("connection reset")) {
        // Catch. Connection reset.
      } else {
        throw ioe;
      }

    }
    return service;
  }

  /**
   * Verify we can connect to <code>hostingServer</code> and that its carrying
   * <code>regionName</code>.
   * @param hostingServer Interface to the server hosting <code>regionName</code>
   * @param address The servername that goes with the <code>metaServer</code>
   * Interface.  Used logging.
   * @param regionName The regionname we are interested in.
   * @return True if we were able to verify the region located at other side of
   * the Interface.
   * @throws IOException
   */
  // TODO: We should be able to get the ServerName from the AdminProtocol
  // rather than have to pass it in.  Its made awkward by the fact that the
  // HRI is likely a proxy against remote server so the getServerName needs
  // to be fixed to go to a local method or to a cache before we can do this.
  private boolean verifyRegionLocation(AdminService.BlockingInterface hostingServer,
      final ServerName address, final byte [] regionName)
  throws IOException {
    if (hostingServer == null) {
      LOG.info("Passed hostingServer is null");
      return false;
    }
    Throwable t = null;
    try {
      // Try and get regioninfo from the hosting server.
      return ProtobufUtil.getRegionInfo(hostingServer, regionName) != null;
    } catch (ConnectException e) {
      t = e;
    } catch (RetriesExhaustedException e) {
      t = e;
    } catch (RemoteException e) {
      IOException ioe = e.unwrapRemoteException();
      t = ioe;
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof EOFException) {
        t = cause;
      } else if (cause != null && cause.getMessage() != null
          && cause.getMessage().contains("Connection reset")) {
        t = cause;
      } else {
        t = e;
      }
    }
    LOG.info("Failed verification of " + Bytes.toStringBinary(regionName) +
      " at address=" + address + ", exception=" + t);
    return false;
  }

  /**
   * Verify <code>hbase:meta</code> is deployed and accessible.
   * @param timeout How long to wait on zk for meta address (passed through to
   * the internal call to {@link #waitForMetaServerConnection(long)}.
   * @return True if the <code>hbase:meta</code> location is healthy.
   * @throws IOException
   * @throws InterruptedException
   */
  public boolean verifyMetaRegionLocation(final long timeout)
  throws InterruptedException, IOException {
    AdminService.BlockingInterface service = null;
    try {
      service = waitForMetaServerConnection(timeout);
    } catch (NotAllMetaRegionsOnlineException e) {
      // Pass
    } catch (ServerNotRunningYetException e) {
      // Pass -- remote server is not up so can't be carrying root
    } catch (UnknownHostException e) {
      // Pass -- server name doesn't resolve so it can't be assigned anything.
    } catch (RegionServerStoppedException e) {
      // Pass -- server name sends us to a server that is dying or already dead.
    }
    return (service == null)? false:
      verifyRegionLocation(service,
          this.metaRegionTracker.getMetaRegionLocation(), META_REGION_NAME);
  }

  public HConnection getConnection() {
    return this.connection;
  }
}
