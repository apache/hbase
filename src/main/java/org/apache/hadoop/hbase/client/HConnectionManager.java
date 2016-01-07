/*
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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.ExecRPCInvoker;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.RpcEngine;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.SoftValueSortedMap;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

/**
 * A non-instantiable class that manages creation of {@link HConnection}s.
 * <p>The simplest way to use this class is by using {@link #createConnection(Configuration)}.
 * This creates a new {@link HConnection} that is managed by the caller.
 * From this {@link HConnection} {@link HTableInterface} implementations are retrieved 
 * with {@link HConnection#getTable(byte[])}. Example:
 * <pre>
 * {@code
 * HConnection connection = HConnectionManager.createConnection(config);
 * HTableInterface table = connection.getTable("table1");
 * // use the table as needed, for a single operation and a single thread
 * table.close();
 * connection.close();
 * }
 * </pre>
 * <p>The following logic and API will be removed in the future:
 * <p>This class has a static Map of {@link HConnection} instances keyed by
 * {@link Configuration}; all invocations of {@link #getConnection(Configuration)}
 * that pass the same {@link Configuration} instance will be returned the same
 * {@link  HConnection} instance (Adding properties to a Configuration
 * instance does not change its object identity).  Sharing {@link HConnection}
 * instances is usually what you want; all clients of the {@link HConnection}
 * instances share the HConnections' cache of Region locations rather than each
 * having to discover for itself the location of meta, root, etc.  It makes
 * sense for the likes of the pool of HTables class {@link HTablePool}, for
 * instance (If concerned that a single {@link HConnection} is insufficient
 * for sharing amongst clients in say an heavily-multithreaded environment,
 * in practise its not proven to be an issue.  Besides, {@link HConnection} is
 * implemented atop Hadoop RPC and as of this writing, Hadoop RPC does a
 * connection per cluster-member, exclusively).
 *
 * <p>But sharing connections
 * makes clean up of {@link HConnection} instances a little awkward.  Currently,
 * clients cleanup by calling
 * {@link #deleteConnection(Configuration)}.  This will shutdown the
 * zookeeper connection the HConnection was using and clean up all
 * HConnection resources as well as stopping proxies to servers out on the
 * cluster. Not running the cleanup will not end the world; it'll
 * just stall the closeup some and spew some zookeeper connection failed
 * messages into the log.  Running the cleanup on a {@link HConnection} that is
 * subsequently used by another will cause breakage so be careful running
 * cleanup.
 * <p>To create a {@link HConnection} that is not shared by others, you can
 * create a new {@link Configuration} instance, pass this new instance to
 * {@link #getConnection(Configuration)}, and then when done, close it up by
 * doing something like the following:
 * <pre>
 * {@code
 * Configuration newConfig = new Configuration(originalConf);
 * HConnection connection = HConnectionManager.getConnection(newConfig);
 * // Use the connection to your hearts' delight and then when done...
 * HConnectionManager.deleteConnection(newConfig, true);
 * }
 * </pre>
 * <p>Cleanup used to be done inside in a shutdown hook.  On startup we'd
 * register a shutdown hook that called {@link #deleteAllConnections()}
 * on its way out but the order in which shutdown hooks run is not defined so
 * were problematic for clients of HConnection that wanted to register their
 * own shutdown hooks so we removed ours though this shifts the onus for
 * cleanup to the client.
 */
@SuppressWarnings("serial")
public class HConnectionManager {
  // An LRU Map of HConnectionKey -> HConnection (TableServer).  All
  // access must be synchronized.  This map is not private because tests
  // need to be able to tinker with it.
  static final Map<HConnectionKey, HConnectionImplementation> HBASE_INSTANCES;

  public static final int MAX_CACHED_HBASE_INSTANCES;

  private static Log LOG = LogFactory.getLog(HConnectionManager.class);

  static {
    // We set instances to one more than the value specified for {@link
    // HConstants#ZOOKEEPER_MAX_CLIENT_CNXNS}. By default, the zk default max
    // connections to the ensemble from the one client is 30, so in that case we
    // should run into zk issues before the LRU hit this value of 31.
    MAX_CACHED_HBASE_INSTANCES = HBaseConfiguration.create().getInt(
        HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
        HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS) + 1;
    HBASE_INSTANCES = new LinkedHashMap<HConnectionKey, HConnectionImplementation>(
        (int) (MAX_CACHED_HBASE_INSTANCES / 0.75F) + 1, 0.75F, true) {
       @Override
      protected boolean removeEldestEntry(
          Map.Entry<HConnectionKey, HConnectionImplementation> eldest) {
         return size() > MAX_CACHED_HBASE_INSTANCES;
       }
    };
  }

  /*
   * Non-instantiable.
   */
  protected HConnectionManager() {
    super();
  }

  /**
   * Get the connection that goes with the passed <code>conf</code>
   * configuration instance.
   * If no current connection exists, method creates a new connection for the
   * passed <code>conf</code> instance.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection getConnection(Configuration conf)
  throws ZooKeeperConnectionException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (HBASE_INSTANCES) {
      HConnectionImplementation connection = HBASE_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = new HConnectionImplementation(conf, true, null);
        HBASE_INSTANCES.put(connectionKey, connection);
      } else if (connection.isClosed()) {
        HConnectionManager.deleteConnection(connectionKey, true);
        connection = new HConnectionImplementation(conf, true, null);
        HBASE_INSTANCES.put(connectionKey, connection);
      }
      connection.incCount();
      return connection;
    }
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code>
   * instance.
   * Note: This bypasses the usual HConnection life cycle management.
   * The caller is responsible for calling {@link HConnection#close()}
   * on the returned connection instance.
   *
   * This is the recommended way to create HConnections.
   * {@code
   * HConnection connection = HConnectionManager.createConnection(conf);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf)
  throws ZooKeeperConnectionException {
    return new HConnectionImplementation(conf, false, null);
  }

  public static HConnection createConnection(Configuration conf, ExecutorService pool)
      throws IOException {
    return new HConnectionImplementation(conf, false, pool);
  }

  /**
   * Delete connection information for the instance specified by configuration.
   * If there are no more references to it, this will then close connection to
   * the zookeeper ensemble and let go of all resources.
   *
   * @param conf
   *          configuration whose identity is used to find {@link HConnection}
   *          instance.
   * @param stopProxy
   *          No longer used.  This parameter is ignored.
   * @deprecated use {@link #createConnection(org.apache.hadoop.conf.Configuration)} instead
   */
  @Deprecated
  public static void deleteConnection(Configuration conf, boolean stopProxy) {
    deleteConnection(conf);
  }

  /**
   * Delete connection information for the instance specified by configuration.
   * If there are no more references to it, this will then close connection to
   * the zookeeper ensemble and let go of all resources.
   *
   * @param conf
   *          configuration whose identity is used to find {@link HConnection}
   *          instance.
   */
  @Deprecated
  public static void deleteConnection(Configuration conf) {
    deleteConnection(new HConnectionKey(conf), false);
  }

  /**
   * Delete stale connection information for the instance specified by configuration.
   * This will then close connection to
   * the zookeeper ensemble and let go of all resources.
   *
   * @param connection
   */
  public static void deleteStaleConnection(HConnection connection) {
    deleteConnection(connection, true);
  }

  /**
   * Delete information for all connections.
   * @param stopProxy No longer used.  This parameter is ignored.
   * @deprecated use {@link #deleteAllConnections()} instead
   */
  @Deprecated
  public static void deleteAllConnections(boolean stopProxy) {
    deleteAllConnections();
  }

  /**
   * Delete information for all connections.
   * @throws IOException
   */
  @Deprecated
  public static void deleteAllConnections() {
    synchronized (HBASE_INSTANCES) {
      Set<HConnectionKey> connectionKeys = new HashSet<HConnectionKey>();
      connectionKeys.addAll(HBASE_INSTANCES.keySet());
      for (HConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey, false);
      }
      HBASE_INSTANCES.clear();
    }
  }

  @Deprecated
  private static void deleteConnection(HConnection connection, boolean staleConnection) {
    synchronized (HBASE_INSTANCES) {
      for (Entry<HConnectionKey, HConnectionImplementation> connectionEntry : HBASE_INSTANCES
          .entrySet()) {
        if (connectionEntry.getValue() == connection) {
          deleteConnection(connectionEntry.getKey(), staleConnection);
          break;
        }
      }
    }
  }

  @Deprecated
  private static void deleteConnection(HConnectionKey connectionKey,
      boolean staleConnection) {
    synchronized (HBASE_INSTANCES) {
      HConnectionImplementation connection = HBASE_INSTANCES
          .get(connectionKey);
      if (connection != null) {
        connection.decCount();
        if (connection.isZeroReference() || staleConnection) {
          HBASE_INSTANCES.remove(connectionKey);
          connection.internalClose();
        }
      }else {
        LOG.error("Connection not found in the list, can't delete it "+
          "(connection key="+connectionKey+"). May be the key was modified?");
      }
    }
  }

  /**
   * It is provided for unit test cases which verify the behavior of region
   * location cache prefetch.
   * @return Number of cached regions for the table.
   * @throws ZooKeeperConnectionException
   */
  static int getCachedRegionCount(Configuration conf,
      final byte[] tableName)
  throws IOException {
    return execute(new HConnectable<Integer>(conf) {
      @Override
      public Integer connect(HConnection connection) {
        return ((HConnectionImplementation) connection)
            .getNumberOfCachedRegionLocations(tableName);
      }
    });
  }

  /**
   * It's provided for unit test cases which verify the behavior of region
   * location cache prefetch.
   * @return true if the region where the table and row reside is cached.
   * @throws ZooKeeperConnectionException
   */
  static boolean isRegionCached(Configuration conf,
      final byte[] tableName, final byte[] row) throws IOException {
    return execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) {
        return ((HConnectionImplementation) connection).isRegionCached(tableName, row);
      }
    });
  }

  /**
   * This class makes it convenient for one to execute a command in the context
   * of a {@link HConnection} instance based on the given {@link Configuration}.
   *
   * <p>
   * If you find yourself wanting to use a {@link HConnection} for a relatively
   * short duration of time, and do not want to deal with the hassle of creating
   * and cleaning up that resource, then you should consider using this
   * convenience class.
   *
   * @param <T>
   *          the return type of the {@link HConnectable#connect(HConnection)}
   *          method.
   */
  public static abstract class HConnectable<T> {
    public Configuration conf;

    public HConnectable(Configuration conf) {
      this.conf = conf;
    }

    public abstract T connect(HConnection connection) throws IOException;
  }

  /**
   * This convenience method invokes the given {@link HConnectable#connect}
   * implementation using a {@link HConnection} instance that lasts just for the
   * duration of that invocation.
   *
   * @param <T> the return type of the connect method
   * @param connectable the {@link HConnectable} instance
   * @return the value returned by the connect method
   * @throws IOException
   */
  public static <T> T execute(HConnectable<T> connectable) throws IOException {
    if (connectable == null || connectable.conf == null) {
      return null;
    }
    Configuration conf = connectable.conf;
    HConnection connection = HConnectionManager.getConnection(conf);
    boolean connectSucceeded = false;
    try {
      T returnValue = connectable.connect(connection);
      connectSucceeded = true;
      return returnValue;
    } finally {
      try {
        connection.close();
      } catch (Exception e) {
        if (connectSucceeded) {
          throw new IOException("The connection to " + connection
              + " could not be deleted.", e);
        }
      }
    }
  }

  /**
   * Denotes a unique key to a {@link HConnection} instance.
   *
   * In essence, this class captures the properties in {@link Configuration}
   * that may be used in the process of establishing a connection. In light of
   * that, if any new such properties are introduced into the mix, they must be
   * added to the {@link HConnectionKey#properties} list.
   *
   */
  public static class HConnectionKey {
    public static String[] CONNECTION_PROPERTIES = new String[] {
        HConstants.ZOOKEEPER_QUORUM, HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.ZOOKEEPER_CLIENT_PORT,
        HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
        HConstants.HBASE_CLIENT_PAUSE, HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.HBASE_CLIENT_RPC_MAXATTEMPTS,
        HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.HBASE_CLIENT_PREFETCH_LIMIT,
        HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.HBASE_CLIENT_INSTANCE_ID };

    private Map<String, String> properties;
    private String username;

    public HConnectionKey(Configuration conf) {
      Map<String, String> m = new HashMap<String, String>();
      if (conf != null) {
        for (String property : CONNECTION_PROPERTIES) {
          String value = conf.get(property);
          if (value != null) {
            m.put(property, value);
          }
        }
      }
      this.properties = Collections.unmodifiableMap(m);

      try {
        UserProvider provider = UserProvider.instantiate(conf);
        username = provider.getCurrentUserName();
      } catch (IOException ioe) {
        LOG.warn("Error obtaining current user, skipping username in HConnectionKey",
            ioe);
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      if (username != null) {
        result = username.hashCode();
      }
      for (String property : CONNECTION_PROPERTIES) {
        String value = properties.get(property);
        if (value != null) {
          result = prime * result + value.hashCode();
        }
      }

      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      HConnectionKey that = (HConnectionKey) obj;
      if (this.username != null && !this.username.equals(that.username)) {
        return false;
      } else if (this.username == null && that.username != null) {
        return false;
      }
      if (this.properties == null) {
        if (that.properties != null) {
          return false;
        }
      } else {
        if (that.properties == null) {
          return false;
        }
        for (String property : CONNECTION_PROPERTIES) {
          String thisValue = this.properties.get(property);
          String thatValue = that.properties.get(property);
          if (thisValue == thatValue) {
            continue;
          }
          if (thisValue == null || !thisValue.equals(thatValue)) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public String toString() {
      return "HConnectionKey{" +
        "properties=" + properties +
        ", username='" + username + '\'' +
        '}';
    }
  }

  /* Encapsulates connection to zookeeper and regionservers.*/
  static class HConnectionImplementation implements HConnection, Closeable {
    static final Log LOG = LogFactory.getLog(HConnectionImplementation.class);
    private final Class<? extends HRegionInterface> serverInterfaceClass;
    private final long pause;
    private final int numRetries;
    private final int maxRPCAttempts;
    private final int rpcTimeout;
    private final int prefetchRegionLimit;

    private final Object masterLock = new Object();
    private volatile boolean closed;
    private volatile boolean aborted;
    private volatile boolean resetting;
    private volatile HMasterInterface master;
    // ZooKeeper reference
    private volatile ZooKeeperWatcher zooKeeper;
    // ZooKeeper-based master address tracker
    private volatile MasterAddressTracker masterAddressTracker;
    private volatile RootRegionTracker rootRegionTracker;
    private volatile ClusterId clusterId;

    private final Object metaRegionLock = new Object();

    private final Object userRegionLock = new Object();
	
    private final Object resetLock = new Object();

    // thread executor shared by all HTableInterface instances created
    // by this connection
    private volatile ExecutorService batchPool = null;
    private volatile boolean cleanupPool = false;

    private final Configuration conf;

    private RpcEngine rpcEngine;

    // Known region HServerAddress.toString() -> HRegionInterface

    private final Map<String, HRegionInterface> servers =
      new ConcurrentHashMap<String, HRegionInterface>();
    private final ConcurrentHashMap<String, String> connectionLock =
      new ConcurrentHashMap<String, String>();

    /**
     * Map of table to table {@link HRegionLocation}s.
     */
    private final Map<HashedBytes, SoftValueSortedMap<byte [], HRegionLocation>>
      cachedRegionLocations =
        new HashMap<HashedBytes, SoftValueSortedMap<byte [], HRegionLocation>>();

    // The presence of a server in the map implies it's likely that there is an
    // entry in cachedRegionLocations that map to this server; but the absence
    // of a server in this map guarentees that there is no entry in cache that
    // maps to the absent server.
    private final Set<String> cachedServers =
        new HashSet<String>();

    // region cache prefetch is enabled by default. this set contains all
    // tables whose region cache prefetch are disabled.
    private final Set<HashedBytes> regionCachePrefetchDisabledTables =
      new CopyOnWriteArraySet<HashedBytes>();

    private int refCount;

    // indicates whether this connection's life cycle is managed
    private final boolean managed;
    /**
     * constructor
     * @param conf Configuration object
     */
    @SuppressWarnings("unchecked")
    public HConnectionImplementation(Configuration conf, boolean managed, ExecutorService pool)
    throws ZooKeeperConnectionException {
      this.conf = conf;
      this.batchPool = pool;
      this.managed = managed;
      String serverClassName = conf.get(HConstants.REGION_SERVER_CLASS,
        HConstants.DEFAULT_REGION_SERVER_CLASS);
      this.closed = false;
      try {
        this.serverInterfaceClass =
          (Class<? extends HRegionInterface>) Class.forName(serverClassName);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException(
            "Unable to find region server interface " + serverClassName, e);
      }
      this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
          HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
      this.numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
      this.maxRPCAttempts = conf.getInt(
          HConstants.HBASE_CLIENT_RPC_MAXATTEMPTS,
          HConstants.DEFAULT_HBASE_CLIENT_RPC_MAXATTEMPTS);
      this.rpcTimeout = conf.getInt(
          HConstants.HBASE_RPC_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
      this.prefetchRegionLimit = conf.getInt(
          HConstants.HBASE_CLIENT_PREFETCH_LIMIT,
          HConstants.DEFAULT_HBASE_CLIENT_PREFETCH_LIMIT);

      this.master = null;
      this.resetting = false;
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
      return getTable(Bytes.toBytes(tableName));
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws IOException {
      return getTable(tableName, getBatchPool());
    }

    @Override
    public HTableInterface getTable(String tableName, ExecutorService pool) throws IOException {
      return getTable(Bytes.toBytes(tableName), pool);
    }

    @Override
    public HTableInterface getTable(byte[] tableName, ExecutorService pool) throws IOException {
      if (managed) {
        throw new IOException("The connection has to be unmanaged.");
      }
      return new HTable(tableName, this, pool);
    }

    private ExecutorService getBatchPool() {
      if (batchPool == null) {
        // shared HTable thread executor not yet initialized
        synchronized (this) {
          if (batchPool == null) {
            int maxThreads = conf.getInt("hbase.hconnection.threads.max", Integer.MAX_VALUE);
            if (maxThreads == 0) {
              maxThreads = Runtime.getRuntime().availableProcessors();
            }
            long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
            this.batchPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                maxThreads, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                Threads.newDaemonThreadFactory("hbase-connection-shared-executor"));
            ((ThreadPoolExecutor) this.batchPool).allowCoreThreadTimeOut(true);
          }
          this.cleanupPool = true;
        }
      }
      return this.batchPool;
    }

    protected ExecutorService getCurrentBatchPool() {
      return batchPool;
    }

    private void shutdownBatchPool() {
      if (this.cleanupPool && this.batchPool != null && !this.batchPool.isShutdown()) {
        this.batchPool.shutdown();
        try {
          if (!this.batchPool.awaitTermination(10, TimeUnit.SECONDS)) {
            this.batchPool.shutdownNow();
          }
        } catch (InterruptedException e) {
          this.batchPool.shutdownNow();
        }
      }
    }

    private synchronized void ensureZookeeperTrackers()
        throws ZooKeeperConnectionException {
      // initialize zookeeper and master address manager
      if (zooKeeper == null) {
        zooKeeper = getZooKeeperWatcher();
      }
      if (clusterId == null) {
        clusterId = new ClusterId(zooKeeper, this);
        if (clusterId.hasId()) {
          conf.set(HConstants.CLUSTER_ID, clusterId.getId());
        }
      }
      if (masterAddressTracker == null) {
        masterAddressTracker = new MasterAddressTracker(zooKeeper, this);
        masterAddressTracker.start();
      }
      if (rootRegionTracker == null) {
        rootRegionTracker = new RootRegionTracker(zooKeeper, this);
        rootRegionTracker.start();
      }
      // RpcEngine needs access to zookeeper data, like cluster ID
      if (rpcEngine == null) {
        this.rpcEngine = HBaseRPC.getProtocolEngine(conf);
      }
    }

    private synchronized void resetZooKeeperTrackers() {
      if (masterAddressTracker != null) {
        masterAddressTracker.stop();
        masterAddressTracker = null;
      }
      if (rootRegionTracker != null) {
        rootRegionTracker.stop();
        rootRegionTracker = null;
      }
      clusterId = null;
      if (zooKeeper != null) {
        zooKeeper.close();
        zooKeeper = null;
      }
    }

    public Configuration getConfiguration() {
      return this.conf;
    }

    /**
     * Log failure of getMaster attempt
     * @return true if should retry
     */
    private boolean shouldRetryGetMaster(int tries, Exception e) {
      if (tries == numRetries - 1) {
        // This was our last chance - don't bother sleeping
        LOG.info("getMaster attempt " + tries + " of " + numRetries +
          " failed; no more retrying.", e);
        return false;
      }
      LOG.info("getMaster attempt " + tries + " of " + numRetries +
        " failed; retrying after sleep of " +
        ConnectionUtils.getPauseTime(this.pause, tries), e);
      return true;
    }

    public HMasterInterface getMaster()
    throws MasterNotRunningException, ZooKeeperConnectionException {
      // TODO: REMOVE.  MOVE TO HBaseAdmin and redo as a Callable!!!

      // Check if we already have a good master connection
      try {
        if (master != null && master.isMasterRunning()) {
          return master;
        }
      } catch (UndeclaredThrowableException ute) {
        // log, but ignore, the loop below will attempt to reconnect
        LOG.info("Exception contacting master. Retrying...", ute.getCause());
      }

      ensureZookeeperTrackers();
      checkIfBaseNodeAvailable();
      ServerName sn = null;
      synchronized (this.masterLock) {
        try {
          if (master != null && master.isMasterRunning()) {
            return master;
          }
        } catch (UndeclaredThrowableException ute) {
          // log, but ignore, the loop below will attempt to reconnect
          LOG.info("Exception contacting master. Retrying...", ute.getCause());
        }
        this.master = null;

        for (int tries = 0;
          !this.closed && this.master == null && tries < numRetries;
        tries++) {

          try {
            sn = masterAddressTracker.getMasterAddress();
            if (sn == null) {
              LOG.info("ZooKeeper available but no active master location found");
              throw new MasterNotRunningException();
            }

            InetSocketAddress isa =
              new InetSocketAddress(sn.getHostname(), sn.getPort());
            HMasterInterface tryMaster = rpcEngine.getProxy(
                HMasterInterface.class, HMasterInterface.VERSION, isa, this.conf,
                this.rpcTimeout);

            if (tryMaster.isMasterRunning()) {
              this.master = tryMaster;
              this.masterLock.notifyAll();
              break;
            }

          } catch (IOException e) {
            if (!shouldRetryGetMaster(tries, e)) break;
          } catch (UndeclaredThrowableException ute) {
            if (!shouldRetryGetMaster(tries, ute)) break;
          }

          // Cannot connect to master or it is not running. Sleep & retry
          try {
            this.masterLock.wait(ConnectionUtils.getPauseTime(this.pause, tries));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while trying to connect to master.");
          }
        }

        if (this.master == null) {
          if (sn == null) {
            throw new MasterNotRunningException();
          }
          throw new MasterNotRunningException(sn.toString());
        }
        return this.master;
      }
    }

    private void checkIfBaseNodeAvailable() throws MasterNotRunningException {
      if (false == masterAddressTracker.checkIfBaseNodeAvailable()) {
        String errorMsg = "Check the value configured in 'zookeeper.znode.parent'. "
            + "There could be a mismatch with the one configured in the master.";
        LOG.error(errorMsg);
        throw new MasterNotRunningException(errorMsg);
      }
    }

    public boolean isMasterRunning()
    throws MasterNotRunningException, ZooKeeperConnectionException {
      if (this.master == null) {
        getMaster();
      }
      boolean isRunning = master.isMasterRunning();
      if(isRunning) {
        return true;
      }
      throw new MasterNotRunningException();
    }

    public HRegionLocation getRegionLocation(final byte [] name,
        final byte [] row, boolean reload)
    throws IOException {
      return reload? relocateRegion(name, row): locateRegion(name, row);
    }

    public boolean isTableEnabled(byte[] tableName) throws IOException {
      return testTableOnlineState(tableName, true);
    }

    public boolean isTableDisabled(byte[] tableName) throws IOException {
      return testTableOnlineState(tableName, false);
    }

    public boolean isTableAvailable(final byte[] tableName) throws IOException {
      final AtomicBoolean available = new AtomicBoolean(true);
      final AtomicInteger regionCount = new AtomicInteger(0);
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result row) throws IOException {
          byte[] value = row.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          HRegionInfo info = Writables.getHRegionInfoOrNull(value);
          if (info != null && !info.isSplitParent()) {
            if (Bytes.equals(tableName, info.getTableName())) {
              value = row.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.SERVER_QUALIFIER);
              if (value == null) {
                available.set(false);
                return false;
              }
              regionCount.incrementAndGet();
            }
          }
          return true;
        }
      };
      MetaScanner.metaScan(conf, this, visitor, null);
      return available.get() && (regionCount.get() > 0);
    }

    /*
     * @param True if table is online
     */
    private boolean testTableOnlineState(byte [] tableName, boolean online)
    throws IOException {
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        // The root region is always enabled
        return online;
      }
      ZooKeeperWatcher zkw = getZooKeeperWatcher();
      String tableNameStr = Bytes.toString(tableName);
      try {
        if (online) {
          return ZKTableReadOnly.isEnabledTable(zkw, tableNameStr);
        }
        return ZKTableReadOnly.isDisabledTable(zkw, tableNameStr);
      } catch (KeeperException e) {
        throw new IOException("Enable/Disable failed", e);
      }
    }

    @Override
    public HRegionLocation locateRegion(final byte[] regionName) throws IOException {
      return locateRegion(HRegionInfo.getTableName(regionName),
        HRegionInfo.getStartKey(regionName), false, true);
    }

    @Override
    public List<HRegionLocation> locateRegions(final byte[] tableName)
    throws IOException {
      return locateRegions(tableName, false, true);
    }

    @Override
    public List<HRegionLocation> locateRegions(final byte[] tableName, final boolean useCache,
        final boolean offlined) throws IOException {
      NavigableMap<HRegionInfo, ServerName> regions = MetaScanner.allTableRegions(conf, this,
          tableName, offlined);
      final List<HRegionLocation> locations = new ArrayList<HRegionLocation>();
      for (HRegionInfo regionInfo : regions.keySet()) {
        locations.add(locateRegion(tableName, regionInfo.getStartKey(), useCache, true));
      }
      return locations;
    }

    public HRegionLocation locateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{
      return locateRegion(tableName, row, true, true);
    }

    public HRegionLocation relocateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{

      // Since this is an explicit request not to use any caching, finding
      // disabled tables should not be desirable.  This will ensure that an exception is thrown when
      // the first time a disabled table is interacted with.
      if (isTableDisabled(tableName)) {
        throw new DoNotRetryIOException(Bytes.toString(tableName) + " is disabled.");
      }

      return locateRegion(tableName, row, false, true);
    }

    private HRegionLocation locateRegion(final byte [] tableName,
      final byte [] row, boolean useCache, boolean retry)
    throws IOException {
      if (this.closed) throw new IOException(toString() + " closed");
      if (tableName == null || tableName.length == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }
      ensureZookeeperTrackers();
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        try {
          ServerName servername = this.rootRegionTracker.waitRootRegionLocation(this.rpcTimeout);
          LOG.debug("Looked up root region location, connection=" + this +
            "; serverName=" + ((servername == null)? "": servername.toString()));
          if (servername == null) return null;
          return new HRegionLocation(HRegionInfo.ROOT_REGIONINFO,
            servername.getHostname(), servername.getPort());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      } else if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
        return locateRegionInMeta(HConstants.ROOT_TABLE_NAME, tableName, row,
            useCache, metaRegionLock, retry);
      } else {
        // Region not in the cache - have to go to the meta RS
        return locateRegionInMeta(HConstants.META_TABLE_NAME, tableName, row,
            useCache, userRegionLock, retry);
      }
    }

    /*
     * Search .META. for the HRegionLocation info that contains the table and
     * row we're seeking. It will prefetch certain number of regions info and
     * save them to the global region cache.
     */
    private void prefetchRegionCache(final byte[] tableName,
        final byte[] row) {
      // Implement a new visitor for MetaScanner, and use it to walk through
      // the .META.
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        public boolean processRow(Result result) throws IOException {
          try {
            byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER);
            HRegionInfo regionInfo = null;

            if (value != null) {
              // convert the row result into the HRegionLocation we need!
              regionInfo = Writables.getHRegionInfo(value);

              // possible we got a region of a different table...
              if (!Bytes.equals(regionInfo.getTableName(),
                  tableName)) {
                return false; // stop scanning
              }
              if (regionInfo.isOffline()) {
                // don't cache offline regions
                return true;
              }
              value = result.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.SERVER_QUALIFIER);
              if (value == null) {
                return true;  // don't cache it
              }
              final String hostAndPort = Bytes.toString(value);
              String hostname = Addressing.parseHostname(hostAndPort);
              int port = Addressing.parsePort(hostAndPort);
              value = result.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.STARTCODE_QUALIFIER);
              // instantiate the location
              HRegionLocation loc =
                new HRegionLocation(regionInfo, hostname, port);
              // cache this meta entry
              cacheLocation(tableName, loc);
            }
            return true;
          } catch (RuntimeException e) {
            throw new IOException(e);
          }
        }
      };
      try {
        // pre-fetch certain number of regions info at region cache.
        MetaScanner.metaScan(conf, this, visitor, tableName, row,
            this.prefetchRegionLimit, HConstants.META_TABLE_NAME);
      } catch (IOException e) {
        // ignore during prefetch
      }
    }

    /*
      * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
      * info that contains the table and row we're seeking.
      */
    private HRegionLocation locateRegionInMeta(final byte [] parentTable,
      final byte [] tableName, final byte [] row, boolean useCache,
      Object regionLockObject, boolean retry)
    throws IOException {
      HRegionLocation location;
      // If we are supposed to be using the cache, look in the cache to see if
      // we already have the region.
      if (useCache) {
        location = getCachedLocation(tableName, row);
        if (location != null) {
          return location;
        }
      }

      int localNumRetries = retry ? numRetries : 1;
      // build the key of the meta region we should be looking for.
      // the extra 9's on the end are necessary to allow "exact" matches
      // without knowing the precise region names.
      byte [] metaKey = HRegionInfo.createRegionName(tableName, row,
        HConstants.NINES, false);
      for (int tries = 0; true; tries++) {
        if (tries >= localNumRetries) {
          throw new NoServerForRegionException("Unable to find region for "
            + Bytes.toStringBinary(row) + " after " + numRetries + " tries.");
        }

        HRegionLocation metaLocation = null;
        try {
          // locate the root or meta region
          metaLocation = locateRegion(parentTable, metaKey, true, false);
          // If null still, go around again.
          if (metaLocation == null) continue;
          HRegionInterface server =
            getHRegionConnection(metaLocation.getHostname(), metaLocation.getPort());

          Result regionInfoRow = null;
          if (useCache) {
            if (Bytes.equals(parentTable, HConstants.META_TABLE_NAME)
                && (getRegionCachePrefetch(tableName))) {
              // This block guards against two threads trying to load the meta
              // region at the same time. The first will load the meta region and
              // the second will use the value that the first one found.
              synchronized (regionLockObject) {
                // Check the cache again for a hit in case some other thread made the
                // same query while we were waiting on the lock.
                location = getCachedLocation(tableName, row);
                if (location != null) {
                  return location;
                }
                // If the parent table is META, we may want to pre-fetch some
                // region info into the global region cache for this table.
                prefetchRegionCache(tableName, row);
              }
            }
            location = getCachedLocation(tableName, row);
            if (location != null) {
              return location;
            }
          } else {
            // If we are not supposed to be using the cache, delete any existing cached location
            // so it won't interfere.
            deleteCachedLocation(tableName, row);
          }

          // Query the root or meta region for the location of the meta region
          regionInfoRow = server.getClosestRowBefore(
          metaLocation.getRegionInfo().getRegionName(), metaKey,
          HConstants.CATALOG_FAMILY);
          if (regionInfoRow == null) {
            throw new TableNotFoundException(Bytes.toString(tableName));
          }
          byte [] value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          if (value == null || value.length == 0) {
            throw new IOException("HRegionInfo was null or empty in " +
              Bytes.toString(parentTable) + ", row=" + regionInfoRow);
          }
          // convert the row result into the HRegionLocation we need!
          HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(
              value, new HRegionInfo());
          // possible we got a region of a different table...
          if (!Bytes.equals(regionInfo.getTableName(), tableName)) {
            throw new TableNotFoundException(
                  "Table '" + Bytes.toString(tableName) + "' was not found, got: " +
                  Bytes.toString(regionInfo.getTableName()) + ".");
          }
          if (regionInfo.isSplit()) {
            throw new RegionOfflineException("the only available region for" +
              " the required row is a split parent," +
              " the daughters should be online soon: " +
              regionInfo.getRegionNameAsString());
          }
          if (regionInfo.isOffline()) {
            throw new RegionOfflineException("the region is offline, could" +
              " be caused by a disable table call: " +
              regionInfo.getRegionNameAsString());
          }

          value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          String hostAndPort = "";
          if (value != null) {
            hostAndPort = Bytes.toString(value);
          }
          if (hostAndPort.equals("")) {
            throw new NoServerForRegionException("No server address listed " +
              "in " + Bytes.toString(parentTable) + " for region " +
              regionInfo.getRegionNameAsString() + " containing row " +
              Bytes.toStringBinary(row));
          }

          // Instantiate the location
          String hostname = Addressing.parseHostname(hostAndPort);
          int port = Addressing.parsePort(hostAndPort);
          location = new HRegionLocation(regionInfo, hostname, port);
          cacheLocation(tableName, location);
          return location;
        } catch (TableNotFoundException e) {
          // if we got this error, probably means the table just plain doesn't
          // exist. rethrow the error immediately. this should always be coming
          // from the HTable constructor.
          throw e;
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          if (tries < numRetries - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("locateRegionInMeta parentTable=" +
                Bytes.toString(parentTable) + ", metaLocation=" +
                ((metaLocation == null)? "null": "{" + metaLocation + "}") +
                ", attempt=" + tries + " of " +
                this.numRetries + " failed; retrying after sleep of " +
                ConnectionUtils.getPauseTime(this.pause, tries) + " because: " + e.getMessage());
            }
          } else {
            throw e;
          }
          // Only relocate the parent region if necessary
          if(!(e instanceof RegionOfflineException ||
              e instanceof NoServerForRegionException)) {
            relocateRegion(parentTable, metaKey);
          }
        }
        try{
          Thread.sleep(ConnectionUtils.getPauseTime(this.pause, tries));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Giving up trying to location region in " +
            "meta: thread is interrupted.");
        }
      }
    }

    /*
     * Search the cache for a location that fits our table and row key.
     * Return null if no suitable region is located. TODO: synchronization note
     *
     * <p>TODO: This method during writing consumes 15% of CPU doing lookup
     * into the Soft Reference SortedMap.  Improve.
     *
     * @param tableName
     * @param row
     * @return Null or region location found in cache.
     */
    HRegionLocation getCachedLocation(final byte [] tableName,
        final byte [] row) {
      SoftValueSortedMap<byte [], HRegionLocation> tableLocations =
        getTableLocations(tableName);

      // start to examine the cache. we can only do cache actions
      // if there's something in the cache for this table.
      if (tableLocations.isEmpty()) {
        return null;
      }

      HRegionLocation possibleRegion = tableLocations.get(row);
      if (possibleRegion != null) {
        return possibleRegion;
      }

      possibleRegion = tableLocations.lowerValueByKey(row);
      if (possibleRegion == null) {
        return null;
      }

      // make sure that the end key is greater than the row we're looking
      // for, otherwise the row actually belongs in the next region, not
      // this one. the exception case is when the endkey is
      // HConstants.EMPTY_END_ROW, signifying that the region we're
      // checking is actually the last region in the table.
      byte[] endKey = possibleRegion.getRegionInfo().getEndKey();
      if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ||
          KeyValue.getRowComparator(tableName).compareRows(
              endKey, 0, endKey.length, row, 0, row.length) > 0) {
        return possibleRegion;
      }

      // Passed all the way through, so we got nothin - complete cache miss
      return null;
    }

    /**
     * Delete a cached location
     * @param tableName tableName
     * @param row
     */
    void deleteCachedLocation(final byte [] tableName, final byte [] row) {
      synchronized (this.cachedRegionLocations) {
        Map<byte[], HRegionLocation> tableLocations = getTableLocations(tableName);
        if (!tableLocations.isEmpty()) {
          // start to examine the cache. we can only do cache actions
          // if there's something in the cache for this table.
          HRegionLocation rl = getCachedLocation(tableName, row);
          if (rl != null) {
            tableLocations.remove(rl.getRegionInfo().getStartKey());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Removed " +
                rl.getRegionInfo().getRegionNameAsString() +
                " for tableName=" + Bytes.toString(tableName) +
                " from cache " + "because of " + Bytes.toStringBinary(row));
            }
          }
        }
      }
    }

    @Override
    public void deleteCachedRegionLocation(final HRegionLocation location) {
      if (location == null) {
        return;
      }
      synchronized (this.cachedRegionLocations) {
        byte[] tableName = location.getRegionInfo().getTableName();
        Map<byte[], HRegionLocation> tableLocations = getTableLocations(tableName);
        if (!tableLocations.isEmpty()) {
          // Delete if there's something in the cache for this region.
          HRegionLocation removedLocation =
              tableLocations.remove(location.getRegionInfo().getStartKey());
          if (LOG.isDebugEnabled() && removedLocation != null) {
            LOG.debug("Removed " +
              location.getRegionInfo().getRegionNameAsString() +
              " for tableName=" + Bytes.toString(tableName) +
              " from cache");
          }
        }
      }
    }

    @Override
    public void clearCaches(String sn) {
      clearCachedLocationForServer(sn);
    }

    /*
     * Delete all cached entries of a table that maps to a specific location.
     *
     * @param tablename
     * @param server
     */
    private void clearCachedLocationForServer(final String server) {
      boolean deletedSomething = false;
      synchronized (this.cachedRegionLocations) {
        if (!cachedServers.contains(server)) {
          return;
        }
        for (Map<byte[], HRegionLocation> tableLocations :
          cachedRegionLocations.values()) {
          for (Entry<byte[], HRegionLocation> e : tableLocations.entrySet()) {
            HRegionLocation value = e.getValue();
            if (value != null
                && value.getHostnamePort().equals(server)) {
              tableLocations.remove(e.getKey());
              deletedSomething = true;
            }
          }
        }
        cachedServers.remove(server);
      }
      if (deletedSomething && LOG.isDebugEnabled()) {
        LOG.debug("Removed all cached region locations that map to " + server);
      }
    }

    /*
     * @param tableName
     * @return Map of cached locations for passed <code>tableName</code>
     */
    private SoftValueSortedMap<byte [], HRegionLocation> getTableLocations(
        final byte [] tableName) {
      // find the map of cached locations for this table
      HashedBytes key = new HashedBytes(tableName);
      SoftValueSortedMap<byte [], HRegionLocation> result;
      synchronized (this.cachedRegionLocations) {
        result = this.cachedRegionLocations.get(key);
        // if tableLocations for this table isn't built yet, make one
        if (result == null) {
          result = new SoftValueSortedMap<byte [], HRegionLocation>(
              Bytes.BYTES_COMPARATOR);
          this.cachedRegionLocations.put(key, result);
        }
      }
      return result;
    }

    @Override
    public void clearRegionCache() {
      synchronized(this.cachedRegionLocations) {
        this.cachedRegionLocations.clear();
        this.cachedServers.clear();
      }
    }

    @Override
    public void clearRegionCache(final byte [] tableName) {
      synchronized (this.cachedRegionLocations) {
        this.cachedRegionLocations.remove(new HashedBytes(tableName));
      }
    }

    /*
     * Put a newly discovered HRegionLocation into the cache.
     */
    private void cacheLocation(final byte [] tableName,
        final HRegionLocation location) {
      byte [] startKey = location.getRegionInfo().getStartKey();
      Map<byte [], HRegionLocation> tableLocations =
        getTableLocations(tableName);
      boolean hasNewCache = false;
      synchronized (this.cachedRegionLocations) {
        cachedServers.add(location.getHostnamePort());
        hasNewCache = (tableLocations.put(startKey, location) == null);
      }
      if (hasNewCache) {
        LOG.debug("Cached location for " +
            location.getRegionInfo().getRegionNameAsString() +
            " is " + location.getHostnamePort());
      }
    }

    public HRegionInterface getHRegionConnection(HServerAddress hsa)
    throws IOException {
      return getHRegionConnection(hsa, false);
    }

    @Override
    public HRegionInterface getHRegionConnection(final String hostname,
        final int port)
    throws IOException {
      return getHRegionConnection(hostname, port, false);
    }

    public HRegionInterface getHRegionConnection(HServerAddress hsa,
        boolean master)
    throws IOException {
      return getHRegionConnection(null, -1, hsa.getInetSocketAddress(), master);
    }

    @Override
    public HRegionInterface getHRegionConnection(final String hostname,
        final int port, final boolean master)
    throws IOException {
      return getHRegionConnection(hostname, port, null, master);
    }

    /**
     * Either the passed <code>isa</code> is null or <code>hostname</code>
     * can be but not both.
     * @param hostname
     * @param port
     * @param isa
     * @param master
     * @return Proxy.
     * @throws IOException
     */
    HRegionInterface getHRegionConnection(final String hostname, final int port,
        final InetSocketAddress isa, final boolean master)
    throws IOException {
      if (master) getMaster();
      HRegionInterface server;
      String rsName = null;
      if (isa != null) {
        rsName = Addressing.createHostAndPortStr(isa.getHostName(),
            isa.getPort());
      } else {
        rsName = Addressing.createHostAndPortStr(hostname, port);
      }
      ensureZookeeperTrackers();
      // See if we already have a connection (common case)
      server = this.servers.get(rsName);
      if (server == null) {
        // create a unique lock for this RS (if necessary)
        this.connectionLock.putIfAbsent(rsName, rsName);
        // get the RS lock
        synchronized (this.connectionLock.get(rsName)) {
          // do one more lookup in case we were stalled above
          server = this.servers.get(rsName);
          if (server == null) {
            try {
              // Only create isa when we need to.
              InetSocketAddress address = isa != null? isa:
                new InetSocketAddress(hostname, port);
              // definitely a cache miss. establish an RPC for this RS
              server = HBaseRPC.waitForProxy(this.rpcEngine,
                  serverInterfaceClass, HRegionInterface.VERSION,
                  address, this.conf,
                  this.maxRPCAttempts, this.rpcTimeout, this.rpcTimeout);
              this.servers.put(Addressing.createHostAndPortStr(
                  address.getHostName(), address.getPort()), server);
            } catch (RemoteException e) {
              LOG.warn("RemoteException connecting to RS", e);
              // Throw what the RemoteException was carrying.
              throw e.unwrapRemoteException();
            }
          }
        }
      }
      return server;
    }

    /**
     * Get the ZooKeeper instance for this TableServers instance.
     *
     * If ZK has not been initialized yet, this will connect to ZK.
     * @returns zookeeper reference
     * @throws ZooKeeperConnectionException if there's a problem connecting to zk
     */
    @Deprecated
    public synchronized ZooKeeperWatcher getZooKeeperWatcher()
        throws ZooKeeperConnectionException {
      if(zooKeeper == null) {
        try {
          if (this.closed) {
            throw new IOException(toString() + " closed");
          }
          this.zooKeeper = new ZooKeeperWatcher(conf, "hconnection", this);
        } catch(ZooKeeperConnectionException zce) {
          throw zce;
        } catch (IOException e) {
          throw new ZooKeeperConnectionException("An error is preventing" +
              " HBase from connecting to ZooKeeper", e);
        }
      }
      return zooKeeper;
    }

    public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
    throws IOException, RuntimeException {
      return callable.withRetries();
    }

    public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
    throws IOException, RuntimeException {
      return callable.withoutRetries();
    }

    private <R> Callable<MultiResponse> createCallable(final HRegionLocation loc,
        final MultiAction<R> multi, final byte [] tableName) {
      // TODO: This does not belong in here!!! St.Ack  HConnections should
      // not be dealing in Callables; Callables have HConnections, not other
      // way around.
      final HConnection connection = this;
      return new Callable<MultiResponse>() {
       public MultiResponse call() throws IOException {
         ServerCallable<MultiResponse> callable =
           new ServerCallable<MultiResponse>(connection, tableName, null) {
             public MultiResponse call() throws IOException {
               return server.multi(multi);
             }
             @Override
             public void connect(boolean reload) throws IOException {
               server = connection.getHRegionConnection(loc.getHostname(), loc.getPort());
             }
           };
         return callable.withoutRetries();
       }
     };
   }

    public void processBatch(List<? extends Row> list,
        final byte[] tableName,
        ExecutorService pool,
        Object[] results) throws IOException, InterruptedException {
      // This belongs in HTable!!! Not in here.  St.Ack

      // results must be the same size as list
      if (results.length != list.size()) {
        throw new IllegalArgumentException("argument results must be the same size as argument list");
      }

      processBatchCallback(list, tableName, pool, results, null);
    }

    /**
     * Executes the given
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call}
     * callable for each row in the
     * given list and invokes
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)}
     * for each result returned.
     *
     * @param protocol the protocol interface being called
     * @param rows a list of row keys for which the callable should be invoked
     * @param tableName table name for the coprocessor invoked
     * @param pool ExecutorService used to submit the calls per row
     * @param callable instance on which to invoke
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
     * for each row
     * @param callback instance on which to invoke
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)}
     * for each result
     * @param <T> the protocol interface type
     * @param <R> the callable's return type
     * @throws IOException
     */
    public <T extends CoprocessorProtocol,R> void processExecs(
        final Class<T> protocol,
        List<byte[]> rows,
        final byte[] tableName,
        ExecutorService pool,
        final Batch.Call<T,R> callable,
        final Batch.Callback<R> callback)
      throws IOException, Throwable {

      Map<byte[],Future<R>> futures =
          new TreeMap<byte[],Future<R>>(Bytes.BYTES_COMPARATOR);
      for (final byte[] r : rows) {
        final ExecRPCInvoker invoker =
            new ExecRPCInvoker(conf, this, protocol, tableName, r);
        Future<R> future = pool.submit(
            new Callable<R>() {
              public R call() throws Exception {
                T instance = (T)Proxy.newProxyInstance(conf.getClassLoader(),
                    new Class[]{protocol},
                    invoker);
                R result = callable.call(instance);
                byte[] region = invoker.getRegionName();
                if (callback != null) {
                  callback.update(region, r, result);
                }
                return result;
              }
            });
        futures.put(r, future);
      }
      for (Map.Entry<byte[],Future<R>> e : futures.entrySet()) {
        try {
          e.getValue().get();
        } catch (ExecutionException ee) {
          LOG.warn("Error executing for row "+Bytes.toStringBinary(e.getKey()), ee);
          throw ee.getCause();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted executing for row " +
              Bytes.toStringBinary(e.getKey()), ie);
        }
      }
    }

    /**
     * Parameterized batch processing, allowing varying return types for
     * different {@link Row} implementations.
     */
    public <R> void processBatchCallback(
        List<? extends Row> list,
        byte[] tableName,
        ExecutorService pool,
        Object[] results,
        Batch.Callback<R> callback)
    throws IOException, InterruptedException {
      // This belongs in HTable!!! Not in here.  St.Ack

      // results must be the same size as list
      if (results.length != list.size()) {
        throw new IllegalArgumentException(
            "argument results must be the same size as argument list");
      }
      if (list.isEmpty()) {
        return;
      }

      // Keep track of the most recent servers for any given item for better
      // exceptional reporting.  We keep HRegionLocation to save on parsing.
      // Later below when we use lastServers, we'll pull what we need from
      // lastServers.
      HRegionLocation [] lastServers = new HRegionLocation[results.length];
      List<Row> workingList = new ArrayList<Row>(list);
      boolean retry = true;
      // count that helps presize actions array
      int actionCount = 0;

      for (int tries = 0; tries < numRetries && retry; ++tries) {

        // sleep first, if this is a retry
        if (tries >= 1) {
          long sleepTime = ConnectionUtils.getPauseTime(this.pause, tries);
          LOG.debug("Retry " +tries+ ", sleep for " +sleepTime+ "ms!");
          Thread.sleep(sleepTime);
        }
        // step 1: break up into regionserver-sized chunks and build the data structs
        Map<HRegionLocation, MultiAction<R>> actionsByServer =
          new HashMap<HRegionLocation, MultiAction<R>>();
        for (int i = 0; i < workingList.size(); i++) {
          Row row = workingList.get(i);
          if (row != null) {
            HRegionLocation loc = locateRegion(tableName, row.getRow());
            byte[] regionName = loc.getRegionInfo().getRegionName();

            MultiAction<R> actions = actionsByServer.get(loc);
            if (actions == null) {
              actions = new MultiAction<R>();
              actionsByServer.put(loc, actions);
            }

            Action<R> action = new Action<R>(row, i);
            lastServers[i] = loc;
            actions.add(regionName, action);
          }
        }

        // step 2: make the requests

        Map<HRegionLocation, Future<MultiResponse>> futures =
            new HashMap<HRegionLocation, Future<MultiResponse>>(
                actionsByServer.size());

        for (Entry<HRegionLocation, MultiAction<R>> e: actionsByServer.entrySet()) {
          futures.put(e.getKey(), pool.submit(createCallable(e.getKey(), e.getValue(), tableName)));
        }

        // step 3: collect the failures and successes and prepare for retry

        for (Entry<HRegionLocation, Future<MultiResponse>> responsePerServer
             : futures.entrySet()) {
          HRegionLocation loc = responsePerServer.getKey();

          try {
            Future<MultiResponse> future = responsePerServer.getValue();
            MultiResponse resp = future.get();

            if (resp == null) {
              // Entire server failed
              LOG.debug("Failed all for server: " + loc.getHostnamePort() +
                ", removing from cache");
              continue;
            }

            for (Entry<byte[], List<Pair<Integer,Object>>> e : resp.getResults().entrySet()) {
              byte[] regionName = e.getKey();
              List<Pair<Integer, Object>> regionResults = e.getValue();
              for (Pair<Integer, Object> regionResult : regionResults) {
                if (regionResult == null) {
                  // if the first/only record is 'null' the entire region failed.
                  LOG.debug("Failures for region: " +
                      Bytes.toStringBinary(regionName) +
                      ", removing from cache");
                } else {
                  // Result might be an Exception, including DNRIOE
                  results[regionResult.getFirst()] = regionResult.getSecond();
                  if (callback != null && !(regionResult.getSecond() instanceof Throwable)) {
                    callback.update(e.getKey(),
                        list.get(regionResult.getFirst()).getRow(),
                        (R)regionResult.getSecond());
                  }
                }
              }
            }
          } catch (ExecutionException e) {
            LOG.warn("Failed all from " + loc, e);
          }
        }

        // step 4: identify failures and prep for a retry (if applicable).

        // Find failures (i.e. null Result), and add them to the workingList (in
        // order), so they can be retried.
        retry = false;
        workingList.clear();
        actionCount = 0;
        for (int i = 0; i < results.length; i++) {
          // if null (fail) or instanceof Throwable && not instanceof DNRIOE
          // then retry that row. else dont.
          if (results[i] == null ||
              (results[i] instanceof Throwable &&
                  !(results[i] instanceof DoNotRetryIOException))) {

            retry = true;
            actionCount++;
            Row row = list.get(i);
            workingList.add(row);
            deleteCachedLocation(tableName, row.getRow());
          } else {
            if (results[i] != null && results[i] instanceof Throwable) {
              actionCount++;
            }
            // add null to workingList, so the order remains consistent with the original list argument.
            workingList.add(null);
          }
        }
      }

      List<Throwable> exceptions = new ArrayList<Throwable>(actionCount);
      List<Row> actions = new ArrayList<Row>(actionCount);
      List<String> addresses = new ArrayList<String>(actionCount);

      for (int i = 0 ; i < results.length; i++) {
        if (results[i] == null || results[i] instanceof Throwable) {
          exceptions.add((Throwable)results[i]);
          actions.add(list.get(i));
          addresses.add(lastServers[i].getHostnamePort());
        }
      }

      if (!exceptions.isEmpty()) {
        throw new RetriesExhaustedWithDetailsException(exceptions,
            actions,
            addresses);
      }
    }

    /*
     * Return the number of cached region for a table. It will only be called
     * from a unit test.
     */
    int getNumberOfCachedRegionLocations(final byte[] tableName) {
      synchronized (this.cachedRegionLocations) {
        Map<byte[], HRegionLocation> tableLocs =
          this.cachedRegionLocations.get(new HashedBytes(tableName));

        if (tableLocs == null) {
          return 0;
        }
        return tableLocs.values().size();
      }
    }

    /**
     * Check the region cache to see whether a region is cached yet or not.
     * Called by unit tests.
     * @param tableName tableName
     * @param row row
     * @return Region cached or not.
     */
    boolean isRegionCached(final byte[] tableName, final byte[] row) {
      HRegionLocation location = getCachedLocation(tableName, row);
      return location != null;
    }

    public void setRegionCachePrefetch(final byte[] tableName,
        final boolean enable) {
      if (!enable) {
        regionCachePrefetchDisabledTables.add(new HashedBytes(tableName));
      }
      else {
        regionCachePrefetchDisabledTables.remove(new HashedBytes(tableName));
      }
    }

    public boolean getRegionCachePrefetch(final byte[] tableName) {
      return !regionCachePrefetchDisabledTables.contains(new HashedBytes(tableName));
    }

    @Override
    public void prewarmRegionCache(byte[] tableName,
        Map<HRegionInfo, HServerAddress> regions) {
      for (Map.Entry<HRegionInfo, HServerAddress> e : regions.entrySet()) {
        HServerAddress hsa = e.getValue();
        if (hsa == null || hsa.getInetSocketAddress() == null) continue;
        cacheLocation(tableName,
          new HRegionLocation(e.getKey(), hsa.getHostname(), hsa.getPort()));
      }
    }

    @Override
    public void abort(final String msg, Throwable t) {
      if (t instanceof KeeperException) {
        LOG.info("This client just lost it's session with ZooKeeper, will"
            + " automatically reconnect when needed.");
        if (t instanceof KeeperException.SessionExpiredException) {
          LOG.info("ZK session expired. This disconnect could have been" +
              " caused by a network partition or a long-running GC pause," +
              " either way it's recommended that you verify your environment.");
          synchronized (resetLock) {
            if (resetting) return;
            this.resetting = true;
          }
          resetZooKeeperTrackers();
          this.resetting = false;
        }
        return;
      }
      if (t != null) LOG.fatal(msg, t);
      else LOG.fatal(msg);
      this.aborted = true;
      close();
    }

    @Override
    public boolean isClosed() {
      return this.closed;
    }

    @Override
    public boolean isAborted(){
      return this.aborted;
    }

    public int getCurrentNrHRS() throws IOException {
      try {
        ZooKeeperWatcher zkw = getZooKeeperWatcher();
        // We go to zk rather than to master to get count of regions to avoid
        // HTable having a Master dependency.  See HBase-2828
        return ZKUtil.getNumberOfChildren(zkw,
            zkw.rsZNode);
      } catch (KeeperException ke) {
        throw new IOException("Unexpected ZooKeeper exception", ke);
      }
    }

    /**
     * Increment this client's reference count.
     */
    void incCount() {
      ++refCount;
    }

    /**
     * Decrement this client's reference count.
     */
    void decCount() {
      if (refCount > 0) {
        --refCount;
      }
    }

    /**
     * Return if this client has no reference
     *
     * @return true if this client has no reference; false otherwise
     */
    boolean isZeroReference() {
      return refCount == 0;
    }

    void internalClose() {
      if (this.closed) {
        return;
      }
      shutdownBatchPool();
      master = null;

      this.servers.clear();
      if (this.rpcEngine != null) {
        this.rpcEngine.close();
      }

      synchronized (this) {
        if (this.zooKeeper != null) {
          LOG.info("Closed zookeeper sessionid=0x" +
            Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));
          this.zooKeeper.close();
          this.zooKeeper = null;
        }
        this.closed = true;
      }
    }

    public void close() {
      if (managed) {
        if (aborted) {
          HConnectionManager.deleteStaleConnection(this);
        } else {
          HConnectionManager.deleteConnection(this, false);
        }
      } else {
        internalClose();
      }
      if (LOG.isTraceEnabled()) LOG.debug("" + this.zooKeeper + " closed.");
    }

    /**
     * Close the connection for good, regardless of what the current value of
     * {@link #refCount} is. Ideally, {@link #refCount} should be zero at this
     * point, which would be the case if all of its consumers close the
     * connection. However, on the off chance that someone is unable to close
     * the connection, perhaps because it bailed out prematurely, the method
     * below will ensure that this {@link HConnection} instance is cleaned up.
     * Caveat: The JVM may take an unknown amount of time to call finalize on an
     * unreachable object, so our hope is that every consumer cleans up after
     * itself, like any good citizen.
     */
    @Override
    protected void finalize() throws Throwable {
      // Pretend as if we are about to release the last remaining reference
      refCount = 1;
      close();
      LOG.debug("The connection to " + this.zooKeeper
          + " was closed by the finalize method.");
    }

    public HTableDescriptor[] listTables() throws IOException {
      HTableDescriptor[] htd = getMaster().getHTableDescriptors();
      return htd;
    }

    public HTableDescriptor[] getHTableDescriptors(List<String> tableNames) throws IOException {
      if (tableNames == null || tableNames.isEmpty()) return new HTableDescriptor[0];
      if (tableNames == null || tableNames.size() == 0) return null;
      return getMaster().getHTableDescriptors(tableNames);
    }

    @Override
    public String[] getTableNames() throws IOException {
      return getMaster().getTableNames();
    }

    public HTableDescriptor getHTableDescriptor(final byte[] tableName)
    throws IOException {
      if (tableName == null || tableName.length == 0) return null;
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        return new UnmodifyableHTableDescriptor(HTableDescriptor.ROOT_TABLEDESC);
      }
      if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
        return HTableDescriptor.META_TABLEDESC;
      }
      List<String> tableNameList = new ArrayList<String>(1);
      tableNameList.add(Bytes.toString(tableName));
      HTableDescriptor[] htds = getHTableDescriptors(tableNameList);
      if (htds != null && htds.length > 0) {
        return htds[0];
      }
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
  }

  /**
   * Set the number of retries to use serverside when trying to communicate
   * with another server over {@link HConnection}.  Used updating catalog
   * tables, etc.  Call this method before we create any Connections.
   * @param c The Configuration instance to set the retries into.
   * @param log Used to log what we set in here.
   */
  public static void setServerSideHConnectionRetries(final Configuration c,
      final Log log) {
    int hcRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // Go big.  Multiply by 10.  If we can't get to meta after this many retries
    // then something seriously wrong.
    int serversideMultiplier =
      c.getInt("hbase.client.serverside.retries.multiplier", 10);
    int retries = hcRetries * serversideMultiplier;
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    log.debug("Set serverside HConnection retries=" + retries);
  }
}
