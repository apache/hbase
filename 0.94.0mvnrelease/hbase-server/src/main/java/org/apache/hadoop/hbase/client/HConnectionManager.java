/**
 * Copyright 2010 The Apache Software Foundation
 *
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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionMovedException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.ExecRPCInvoker;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.SoftValueSortedMap;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ServiceException;

/**
 * A non-instantiable class that manages {@link HConnection}s.
 * This class has a static Map of {@link HConnection} instances keyed by
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
 * {@link #deleteConnection(Configuration, boolean)}.  This will shutdown the
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
 * register a shutdown hook that called {@link #deleteAllConnections(boolean)}
 * on its way out but the order in which shutdown hooks run is not defined so
 * were problematic for clients of HConnection that wanted to register their
 * own shutdown hooks so we removed ours though this shifts the onus for
 * cleanup to the client.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HConnectionManager {
  // An LRU Map of HConnectionKey -> HConnection (TableServer).  All
  // access must be synchronized.  This map is not private because tests
  // need to be able to tinker with it.
  static final Map<HConnectionKey, HConnectionImplementation> HBASE_INSTANCES;

  public static final int MAX_CACHED_HBASE_INSTANCES;

  /** Parameter name for what client protocol to use. */
  public static final String CLIENT_PROTOCOL_CLASS = "hbase.clientprotocol.class";

  /** Default client protocol class name. */
  public static final String DEFAULT_CLIENT_PROTOCOL_CLASS = ClientProtocol.class.getName();

  /** Parameter name for what admin protocol to use. */
  public static final String REGION_PROTOCOL_CLASS = "hbase.adminprotocol.class";

  /** Default admin protocol class name. */
  public static final String DEFAULT_ADMIN_PROTOCOL_CLASS = AdminProtocol.class.getName();

  private static final Log LOG = LogFactory.getLog(HConnectionManager.class);

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
        connection = new HConnectionImplementation(conf, true);
        HBASE_INSTANCES.put(connectionKey, connection);
      }
      connection.incCount();
      return connection;
    }
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code>
   * instance.
   * Note: This bypasses the usual HConnection life cycle management!
   * Use this with caution, the caller is responsible for closing the
   * created connection.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf)
  throws ZooKeeperConnectionException {
    return new HConnectionImplementation(conf, false);
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
   *          Shuts down all the proxy's put up to cluster members including to
   *          cluster HMaster. Calls
   *          {@link HBaseRPC#stopProxy(org.apache.hadoop.hbase.ipc.VersionedProtocol)}
   *          .
   */
  public static void deleteConnection(Configuration conf, boolean stopProxy) {
    deleteConnection(new HConnectionKey(conf), stopProxy, false);
  }

  /**
   * Delete stale connection information for the instance specified by configuration.
   * This will then close connection to
   * the zookeeper ensemble and let go of all resources.
   *
   * @param connection
   */
  public static void deleteStaleConnection(HConnection connection) {
    deleteConnection(connection, true, true);
  }

  /**
   * Delete information for all connections.
   * @param stopProxy stop the proxy as well
   * @throws IOException
   */
  public static void deleteAllConnections(boolean stopProxy) {
    synchronized (HBASE_INSTANCES) {
      Set<HConnectionKey> connectionKeys = new HashSet<HConnectionKey>();
      connectionKeys.addAll(HBASE_INSTANCES.keySet());
      for (HConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey, stopProxy, false);
      }
      HBASE_INSTANCES.clear();
    }
  }

  private static void deleteConnection(HConnection connection, boolean stopProxy,
      boolean staleConnection) {
    synchronized (HBASE_INSTANCES) {
      for (Entry<HConnectionKey, HConnectionImplementation> connectionEntry : HBASE_INSTANCES
          .entrySet()) {
        if (connectionEntry.getValue() == connection) {
          deleteConnection(connectionEntry.getKey(), stopProxy, staleConnection);
          break;
        }
      }
    }
  }

  private static void deleteConnection(HConnectionKey connectionKey,
      boolean stopProxy, boolean staleConnection) {
    synchronized (HBASE_INSTANCES) {
      HConnectionImplementation connection = HBASE_INSTANCES
          .get(connectionKey);
      if (connection != null) {
        connection.decCount();
        if (connection.isZeroReference() || staleConnection) {
          HBASE_INSTANCES.remove(connectionKey);
          connection.close(stopProxy);
        } else if (stopProxy) {
          connection.stopProxyOnClose(stopProxy);
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

    protected HConnectable(Configuration conf) {
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
  static class HConnectionKey {
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
        User currentUser = User.getCurrent();
        if (currentUser != null) {
          username = currentUser.getName();
        }
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
    private final Class<? extends AdminProtocol> adminClass;
    private final Class<? extends ClientProtocol> clientClass;
    private final long pause;
    private final int numRetries;
    private final int maxRPCAttempts;
    private final int rpcTimeout;
    private final int prefetchRegionLimit;

    private volatile boolean closed;
    private volatile boolean aborted;

    private final Object metaRegionLock = new Object();
    private final Object userRegionLock = new Object();

    // We have a single lock for master & zk to prevent deadlocks. Having
    //  one lock for ZK and one lock for master is not possible:
    //  When creating a connection to master, we need a connection to ZK to get
    //  its address. But another thread could have taken the ZK lock, and could
    //  be waiting for the master lock => deadlock.
    private final Object masterAndZKLock = new Object();

    private long keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;
    private long keepMasterAliveUntil = Long.MAX_VALUE;
    private final DelayedClosing delayedClosing =
      DelayedClosing.createAndStart(this);


    private final Configuration conf;

    // Known region ServerName.toString() -> RegionClient/Admin
    private final ConcurrentHashMap<String, Map<String, VersionedProtocol>> servers =
      new ConcurrentHashMap<String, Map<String, VersionedProtocol>>();
    private final ConcurrentHashMap<String, String> connectionLock =
      new ConcurrentHashMap<String, String>();

    /**
     * Map of table to table {@link HRegionLocation}s.  The table key is made
     * by doing a {@link Bytes#mapKey(byte[])} of the table's name.
     */
    private final Map<Integer, SoftValueSortedMap<byte [], HRegionLocation>>
      cachedRegionLocations =
        new HashMap<Integer, SoftValueSortedMap<byte [], HRegionLocation>>();

    // The presence of a server in the map implies it's likely that there is an
    // entry in cachedRegionLocations that map to this server; but the absence
    // of a server in this map guarentees that there is no entry in cache that
    // maps to the absent server.
    private final Set<String> cachedServers =
        new HashSet<String>();

    // region cache prefetch is enabled by default. this set contains all
    // tables whose region cache prefetch are disabled.
    private final Set<Integer> regionCachePrefetchDisabledTables =
      new CopyOnWriteArraySet<Integer>();

    private boolean stopProxy;
    private int refCount;

    // indicates whether this connection's life cycle is managed (by us)
    private final boolean managed;
    /**
     * constructor
     * @param conf Configuration object
     */
    @SuppressWarnings("unchecked")
    public HConnectionImplementation(Configuration conf, boolean managed)
    throws ZooKeeperConnectionException {
      this.conf = conf;
      this.managed = managed;
      String adminClassName = conf.get(REGION_PROTOCOL_CLASS,
        DEFAULT_ADMIN_PROTOCOL_CLASS);
      this.closed = false;
      try {
        this.adminClass =
          (Class<? extends AdminProtocol>) Class.forName(adminClassName);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException(
            "Unable to find region server interface " + adminClassName, e);
      }
      String clientClassName = conf.get(CLIENT_PROTOCOL_CLASS,
        DEFAULT_CLIENT_PROTOCOL_CLASS);
      try {
        this.clientClass =
          (Class<? extends ClientProtocol>) Class.forName(clientClassName);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException(
            "Unable to find client protocol " + clientClassName, e);
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

      retrieveClusterId();
    }

    /**
     * An identifier that will remain the same for a given connection.
     * @return
     */
    public String toString(){
      return "hconnection 0x" + Integer.toHexString( hashCode() );
    }

    private String clusterId = null;
    public final void retrieveClusterId(){
      if (conf.get(HConstants.CLUSTER_ID) != null){
        return;
      }

      // No synchronized here, worse case we will retrieve it twice, that's
      //  not an issue.
      if (this.clusterId == null){
        this.clusterId = conf.get(HConstants.CLUSTER_ID);
        if (this.clusterId == null) {
          ZooKeeperKeepAliveConnection zkw = null;
          try {
            zkw = getKeepAliveZooKeeperWatcher();
            this.clusterId = ZKClusterId.readClusterIdZNode(zkw);
            if (clusterId == null) {
              LOG.info("ClusterId read in ZooKeeper is null");
            }
          } catch (KeeperException e) {
            LOG.warn("Can't retrieve clusterId from Zookeeper", e);
          } catch (IOException e) {
            LOG.warn("Can't retrieve clusterId from Zookeeper", e);
          } finally {
            if (zkw != null) {
              zkw.close();
            }
          }
          if (this.clusterId == null) {
            this.clusterId = "default";
          }

          LOG.info("ClusterId is " + clusterId);
        }
      }

      conf.set(HConstants.CLUSTER_ID, clusterId);
    }

    @Override
    public Configuration getConfiguration() {
      return this.conf;
    }


    /**
     * Create a new Master proxy. Try once only.
     */
    private HMasterInterface createMasterInterface()
      throws IOException, KeeperException, ServiceException {

      ZooKeeperKeepAliveConnection zkw;
      try {
        zkw = getKeepAliveZooKeeperWatcher();
      } catch (IOException e) {
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      }

      try {

        checkIfBaseNodeAvailable(zkw);
        ServerName sn = MasterAddressTracker.getMasterAddress(zkw);
        if (sn == null) {
          String msg =
            "ZooKeeper available but no active master location found";
          LOG.info(msg);
          throw new MasterNotRunningException(msg);
        }


        InetSocketAddress isa =
          new InetSocketAddress(sn.getHostname(), sn.getPort());
        HMasterInterface tryMaster = (HMasterInterface) HBaseRPC.getProxy(
          HMasterInterface.class, HMasterInterface.VERSION, isa, this.conf,
          this.rpcTimeout);

        if (tryMaster.isMasterRunning(
            null, RequestConverter.buildIsMasterRunningRequest()).getIsMasterRunning()) {
          return tryMaster;
        } else {
          HBaseRPC.stopProxy(tryMaster);
          String msg = "Can create a proxy to master, but it is not running";
          LOG.info(msg);
          throw new MasterNotRunningException(msg);
        }
      } finally {
        zkw.close();
      }
    }


    /**
     * Get a connection to master. This connection will be reset if master dies.
     * Don't close it, it will be closed with the connection.
     * @deprecated This function is deprecated because it creates a never ending
     * connection to master and  hence wastes resources.
     * In the hbase.client package, use {@link #getKeepAliveMaster()} instead.
     *
     * Internally, we're using the shared master as there is no reason to create
     *  a new connection. However, in this case, we will never close the shared
     *  master.
     */
    @Override
    @Deprecated
    public HMasterInterface getMaster() throws
      MasterNotRunningException, ZooKeeperConnectionException {
      synchronized (this.masterAndZKLock) {
        canCloseMaster = false;
        try {
          return getKeepAliveMaster();
        } catch (MasterNotRunningException e) {
          throw e;
        }
      }
    }

    /**
     * Create a master, retries if necessary.
     */
    private HMasterInterface createMasterWithRetries()
      throws MasterNotRunningException {

      // The lock must be at the beginning to prevent multiple master creation
      //  (and leaks) in a multithread context
      synchronized (this.masterAndZKLock) {
        Exception exceptionCaught = null;
        HMasterInterface master = null;
        int tries = 0;
        while (
          !this.closed && master == null
          ) {
          tries++;
          try {
            master = createMasterInterface();
          } catch (IOException e) {
            exceptionCaught = e;
          } catch (KeeperException e) {
            exceptionCaught = e;
          } catch (ServiceException e) {
            exceptionCaught = e;
          }

          if (exceptionCaught != null)
            // It failed. If it's not the last try, we're going to wait a little
          if (tries < numRetries) {
            long pauseTime = ConnectionUtils.getPauseTime(this.pause, tries);
            LOG.info("getMaster attempt " + tries + " of " + numRetries +
              " failed; retrying after sleep of " +pauseTime, exceptionCaught);

            try {
              Thread.sleep(pauseTime);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(
                "Thread was interrupted while trying to connect to master.", e);
            }

          } else {
            // Enough tries, we stop now
            LOG.info("getMaster attempt " + tries + " of " + numRetries +
              " failed; no more retrying.", exceptionCaught);
            throw new MasterNotRunningException(exceptionCaught);
          }
        }

        if (master == null) {
          // implies this.closed true
          throw new MasterNotRunningException(
            "Connection was closed while trying to get master");
        }

        return master;
      }
    }

    private void checkIfBaseNodeAvailable(ZooKeeperWatcher zkw)
      throws MasterNotRunningException {
      String errorMsg;
      try {
        if (ZKUtil.checkExists(zkw, zkw.baseZNode) == -1) {
          errorMsg = "The node " + zkw.baseZNode+" is not in ZooKeeper. "
            + "It should have been written by the master. "
            + "Check the value configured in 'zookeeper.znode.parent'. "
            + "There could be a mismatch with the one configured in the master.";
          LOG.error(errorMsg);
          throw new MasterNotRunningException(errorMsg);
        }
      } catch (KeeperException e) {
        errorMsg = "Can't get connection to ZooKeeper: " + e.getMessage();
        LOG.error(errorMsg);
        throw new MasterNotRunningException(errorMsg, e);
      }
    }

    /**
     * @return true if the master is running, throws an exception otherwise
     * @throws MasterNotRunningException - if the master is not running
     * @throws ZooKeeperConnectionException
     */
    @Override
    public boolean isMasterRunning()
      throws MasterNotRunningException, ZooKeeperConnectionException {
      // When getting the master proxy connection, we check it's running,
      // so if there is no exception, it means we've been able to get a
      // connection on a running master
      getKeepAliveMaster().close();
      return true;
    }

    @Override
    public HRegionLocation getRegionLocation(final byte [] name,
        final byte [] row, boolean reload)
    throws IOException {
      return reload? relocateRegion(name, row): locateRegion(name, row);
    }

    @Override
    public boolean isTableEnabled(byte[] tableName) throws IOException {
      return testTableOnlineState(tableName, true);
    }

    @Override
    public boolean isTableDisabled(byte[] tableName) throws IOException {
      return testTableOnlineState(tableName, false);
    }

    @Override
    public boolean isTableAvailable(final byte[] tableName) throws IOException {
      final AtomicBoolean available = new AtomicBoolean(true);
      final AtomicInteger regionCount = new AtomicInteger(0);
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result row) throws IOException {
          byte[] value = row.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          HRegionInfo info = Writables.getHRegionInfoOrNull(value);
          if (info != null) {
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
      MetaScanner.metaScan(conf, visitor);
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
      String tableNameStr = Bytes.toString(tableName);
      ZooKeeperKeepAliveConnection zkw = getKeepAliveZooKeeperWatcher();
      try {
        if (online) {
          return ZKTable.isEnabledTable(zkw, tableNameStr);
        }
        return ZKTable.isDisabledTable(zkw, tableNameStr);
      } catch (KeeperException e) {
        throw new IOException("Enable/Disable failed", e);
      }finally {
         zkw.close();
      }
    }

    @Override
    public HRegionLocation locateRegion(final byte [] regionName)
    throws IOException {
      // TODO implement.  use old stuff or new stuff?
      return null;
    }

    @Override
    public List<HRegionLocation> locateRegions(final byte [] tableName)
    throws IOException {
      // TODO implement.  use old stuff or new stuff?
      return null;
    }

    @Override
    public HRegionLocation locateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{
      return locateRegion(tableName, row, true);
    }

    @Override
    public HRegionLocation relocateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{
      return locateRegion(tableName, row, false);
    }

    private HRegionLocation locateRegion(final byte [] tableName,
      final byte [] row, boolean useCache)
    throws IOException {
      if (this.closed) throw new IOException(toString() + " closed");
      if (tableName == null || tableName.length == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }

      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        ZooKeeperKeepAliveConnection zkw = getKeepAliveZooKeeperWatcher();
        try {
          LOG.debug("Looking up root region location in ZK," +
            " connection=" + this);
          ServerName servername =
            RootRegionTracker.blockUntilAvailable(zkw, this.rpcTimeout);

          LOG.debug("Looked up root region location, connection=" + this +
            "; serverName=" + ((servername == null) ? "null" : servername));
          if (servername == null) return null;
          return new HRegionLocation(HRegionInfo.ROOT_REGIONINFO,
            servername.getHostname(), servername.getPort());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        } finally {
          zkw.close();
        }
      } else if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
        return locateRegionInMeta(HConstants.ROOT_TABLE_NAME, tableName, row,
          useCache, metaRegionLock);
      } else {
        // Region not in the cache - have to go to the meta RS
        return locateRegionInMeta(HConstants.META_TABLE_NAME, tableName, row,
          useCache, userRegionLock);
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
        MetaScanner.metaScan(conf, visitor, tableName, row,
            this.prefetchRegionLimit);
      } catch (IOException e) {
        LOG.warn("Encountered problems when prefetch META table: ", e);
      }
    }

    /*
      * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
      * info that contains the table and row we're seeking.
      */
    private HRegionLocation locateRegionInMeta(final byte [] parentTable,
      final byte [] tableName, final byte [] row, boolean useCache,
      Object regionLockObject)
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

      // build the key of the meta region we should be looking for.
      // the extra 9's on the end are necessary to allow "exact" matches
      // without knowing the precise region names.
      byte [] metaKey = HRegionInfo.createRegionName(tableName, row,
        HConstants.NINES, false);
      for (int tries = 0; true; tries++) {
        if (tries >= numRetries) {
          throw new NoServerForRegionException("Unable to find region for "
            + Bytes.toStringBinary(row) + " after " + numRetries + " tries.");
        }

        HRegionLocation metaLocation = null;
        try {
          // locate the root or meta region
          metaLocation = locateRegion(parentTable, metaKey);
          // If null still, go around again.
          if (metaLocation == null) continue;
          ClientProtocol server =
            getClient(metaLocation.getHostname(), metaLocation.getPort());

          Result regionInfoRow = null;
          // This block guards against two threads trying to load the meta
          // region at the same time. The first will load the meta region and
          // the second will use the value that the first one found.
          synchronized (regionLockObject) {
            // If the parent table is META, we may want to pre-fetch some
            // region info into the global region cache for this table.
            if (Bytes.equals(parentTable, HConstants.META_TABLE_NAME) &&
                (getRegionCachePrefetch(tableName)) )  {
              prefetchRegionCache(tableName, row);
            }

            // Check the cache again for a hit in case some other thread made the
            // same query while we were waiting on the lock. If not supposed to
            // be using the cache, delete any existing cached location so it won't
            // interfere.
            if (useCache) {
              location = getCachedLocation(tableName, row);
              if (location != null) {
                return location;
              }
            } else {
              deleteCachedLocation(tableName, row);
            }

            // Query the root or meta region for the location of the meta region
            regionInfoRow = ProtobufUtil.getRowOrBefore(server,
              metaLocation.getRegionInfo().getRegionName(), metaKey,
              HConstants.CATALOG_FAMILY);
          }
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
        Map<byte[], HRegionLocation> tableLocations =
            getTableLocations(tableName);
        // start to examine the cache. we can only do cache actions
        // if there's something in the cache for this table.
        if (!tableLocations.isEmpty()) {
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
            if (e.getValue().getHostnamePort().equals(server)) {
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
      Integer key = Bytes.mapKey(tableName);
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
        this.cachedRegionLocations.remove(Bytes.mapKey(tableName));
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

    @Override
    public AdminProtocol getAdmin(final String hostname,
        final int port) throws IOException {
      return getAdmin(hostname, port, false);
    }

    @Override
    public ClientProtocol getClient(
        final String hostname, final int port) throws IOException {
      return (ClientProtocol)getProtocol(hostname, port,
        clientClass, ClientProtocol.VERSION);
    }

    @Override
    public AdminProtocol getAdmin(final String hostname,
        final int port, final boolean master) throws IOException {
      return (AdminProtocol)getProtocol(hostname, port,
        adminClass, AdminProtocol.VERSION);
    }

    /**
     * Either the passed <code>isa</code> is null or <code>hostname</code>
     * can be but not both.
     * @param hostname
     * @param port
     * @param protocolClass
     * @param version
     * @return Proxy.
     * @throws IOException
     */
    VersionedProtocol getProtocol(final String hostname,
        final int port, final Class <? extends VersionedProtocol> protocolClass,
        final long version) throws IOException {
      String rsName = Addressing.createHostAndPortStr(hostname, port);
      // See if we already have a connection (common case)
      Map<String, VersionedProtocol> protocols = this.servers.get(rsName);
      if (protocols == null) {
        protocols = new HashMap<String, VersionedProtocol>();
        Map<String, VersionedProtocol> existingProtocols =
          this.servers.putIfAbsent(rsName, protocols);
        if (existingProtocols != null) {
          protocols = existingProtocols;
        }
      }
      String protocol = protocolClass.getName();
      VersionedProtocol server = protocols.get(protocol);
      if (server == null) {
        // create a unique lock for this RS + protocol (if necessary)
        String lockKey = protocol + "@" + rsName;
        this.connectionLock.putIfAbsent(lockKey, lockKey);
        // get the RS lock
        synchronized (this.connectionLock.get(lockKey)) {
          // do one more lookup in case we were stalled above
          server = protocols.get(protocol);
          if (server == null) {
            try {
              // Only create isa when we need to.
              InetSocketAddress address = new InetSocketAddress(hostname, port);
              // definitely a cache miss. establish an RPC for this RS
              server = HBaseRPC.waitForProxy(
                  protocolClass, version, address, this.conf,
                  this.maxRPCAttempts, this.rpcTimeout, this.rpcTimeout);
              protocols.put(protocol, server);
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

    @Override
    @Deprecated
    public ZooKeeperWatcher getZooKeeperWatcher()
        throws ZooKeeperConnectionException {
      canCloseZKW = false;

      try {
        return getKeepAliveZooKeeperWatcher();
      } catch (ZooKeeperConnectionException e){
        throw e;
      }catch (IOException e) {
        // Encapsulate exception to keep interface
        throw new ZooKeeperConnectionException(
          "Can't create a zookeeper connection", e);
      }
    }


    private ZooKeeperKeepAliveConnection keepAliveZookeeper;
    private int keepAliveZookeeperUserCount;
    private boolean canCloseZKW = true;

    // keepAlive time, in ms. No reason to make it configurable.
    private static final long keepAlive = 5 * 60 * 1000;

    /**
     * Retrieve a shared ZooKeeperWatcher. You must close it it once you've have
     *  finished with it.
     * @return The shared instance. Never returns null.
     */
    public ZooKeeperKeepAliveConnection getKeepAliveZooKeeperWatcher()
      throws IOException {
      synchronized (masterAndZKLock) {

        if (keepAliveZookeeper == null) {
          // We don't check that our link to ZooKeeper is still valid
          // But there is a retry mechanism in the ZooKeeperWatcher itself
          keepAliveZookeeper = new ZooKeeperKeepAliveConnection(
            conf, this.toString(), this);
        }
        keepAliveZookeeperUserCount++;
        keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;

        return keepAliveZookeeper;
      }
    }

    void releaseZooKeeperWatcher(ZooKeeperWatcher zkw) {
      if (zkw == null){
        return;
      }
      synchronized (masterAndZKLock) {
        --keepAliveZookeeperUserCount;
        if (keepAliveZookeeperUserCount <=0 ){
          keepZooKeeperWatcherAliveUntil =
            System.currentTimeMillis() + keepAlive;
        }
      }
    }


    /**
     * Creates a Chore thread to check the connections to master & zookeeper
     *  and close them when they reach their closing time (
     *  {@link #keepMasterAliveUntil} and
     *  {@link #keepZooKeeperWatcherAliveUntil}). Keep alive time is
     *  managed by the release functions and the variable {@link #keepAlive}
     */
    private static class DelayedClosing extends Chore implements Stoppable {
      private HConnectionImplementation hci;
      Stoppable stoppable;

      private DelayedClosing(
        HConnectionImplementation hci, Stoppable stoppable){
        super(
          "ZooKeeperWatcher and Master delayed closing for connection "+hci,
          60*1000, // We check every minutes
          stoppable);
        this.hci = hci;
        this.stoppable = stoppable;
      }

      static DelayedClosing createAndStart(HConnectionImplementation hci){
        Stoppable stoppable = new Stoppable() {
              private volatile boolean isStopped = false;
              @Override public void stop(String why) { isStopped = true;}
              @Override public boolean isStopped() {return isStopped;}
            };

        return new DelayedClosing(hci, stoppable);
      }

      @Override
      protected void chore() {
        synchronized (hci.masterAndZKLock) {
          if (hci.canCloseZKW) {
            if (System.currentTimeMillis() >
              hci.keepZooKeeperWatcherAliveUntil) {

              hci.closeZooKeeperWatcher();
              hci.keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;
            }
          }
          if (hci.canCloseMaster) {
            if (System.currentTimeMillis() > hci.keepMasterAliveUntil) {
              hci.closeMaster();
              hci.keepMasterAliveUntil = Long.MAX_VALUE;
            }
          }
        }
      }

      @Override
      public void stop(String why) {
        stoppable.stop(why);
      }

      @Override
      public boolean isStopped() {
        return stoppable.isStopped();
      }
    }

    private void closeZooKeeperWatcher() {
      synchronized (masterAndZKLock) {
        if (keepAliveZookeeper != null) {
          LOG.info("Closing zookeeper sessionid=0x" +
            Long.toHexString(
              keepAliveZookeeper.getRecoverableZooKeeper().getSessionId()));
          keepAliveZookeeper.internalClose();
          keepAliveZookeeper = null;
        }
        keepAliveZookeeperUserCount = 0;
      }
    }

    private static class MasterHandler implements InvocationHandler {
      private HConnectionImplementation connection;
      private HMasterInterface master;

      protected MasterHandler(HConnectionImplementation connection,
                                    HMasterInterface master) {
        this.connection = connection;
        this.master = master;
      }

      @Override
      public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
        if (method.getName().equals("close") &&
              method.getParameterTypes().length == 0) {
          release(connection, master);
          return null;
        } else {
          try {
            return method.invoke(master, args);
          }catch  (InvocationTargetException e){
            // We will have this for all the exception, checked on not, sent
            //  by any layer, including the functional exception
            Throwable cause = e.getCause();
            if (cause == null){
              throw new RuntimeException(
                "Proxy invocation failed and getCause is null", e);
            }
            if (cause instanceof UndeclaredThrowableException) {
              cause = cause.getCause();
            }
            if (cause instanceof ServiceException) {
              ServiceException se = (ServiceException)cause;
              cause = ProtobufUtil.getRemoteException(se);
            }
            throw cause;
          }
        }
      }

      private void release(
        HConnectionImplementation connection,
        org.apache.hadoop.hbase.ipc.HMasterInterface target) {
        connection.releaseMaster(target);
      }
    }

    private HMasterInterface keepAliveMaster;
    private int keepAliveMasterUserCount;

    // The old interface {@link #getMaster} allows to get a master that's never
    //  closed. So if someone uses this interface, we never close the shared
    //  master whatever the user count...
    // When closing the connection, we close the master as well, as in the
    //  previous implementation.
    private boolean canCloseMaster = true;

    /**
     * This function allows HBaseAdmin and potentially others
     * to get a shared master connection.
     *
     * @return The shared instance. Never returns null.
     * @throws MasterNotRunningException
     */
    MasterKeepAliveConnection getKeepAliveMaster()
      throws MasterNotRunningException {
      synchronized (masterAndZKLock) {
        if (!isKeepAliveMasterConnectedAndRunning()) {
          if (keepAliveMaster != null) {
            HBaseRPC.stopProxy(keepAliveMaster);
          }
          keepAliveMaster = null;
          keepAliveMaster = createMasterWithRetries();
        }
        keepAliveMasterUserCount++;
        keepMasterAliveUntil = Long.MAX_VALUE;

        return (MasterKeepAliveConnection) Proxy.newProxyInstance(
          MasterKeepAliveConnection.class.getClassLoader(),
          new Class[]{MasterKeepAliveConnection.class},
          new MasterHandler(this, keepAliveMaster)
        );
      }
    }

    private boolean isKeepAliveMasterConnectedAndRunning(){
      if (keepAliveMaster == null){
        return false;
      }
      try {
         return keepAliveMaster.isMasterRunning(
           null, RequestConverter.buildIsMasterRunningRequest()).getIsMasterRunning();
      }catch (UndeclaredThrowableException e){
        // It's somehow messy, but we can receive exceptions such as
        //  java.net.ConnectException but they're not declared. So we catch
        //  it...
        LOG.info("Master connection is not running anymore",
          e.getUndeclaredThrowable());
        return false;
      } catch (ServiceException se) {
        LOG.warn("Checking master connection", se);
        return false;
      }
    }

   private void releaseMaster(HMasterInterface master) {
      if (master == null){
        return;
      }
      synchronized (masterAndZKLock) {
        --keepAliveMasterUserCount;
        if (keepAliveMasterUserCount <= 0) {
          keepMasterAliveUntil =
            System.currentTimeMillis() + keepAlive;
        }
      }
    }

    /**
     * Immediate close of the shared master. Can be by the delayed close or
     *  when closing the connection itself.
     */
    private void closeMaster() {
      synchronized (masterAndZKLock) {
        if (keepAliveMaster != null ){
          LOG.info("Closing master connection");
          HBaseRPC.stopProxy(keepAliveMaster);
          keepAliveMaster = null;
        }
        keepAliveMasterUserCount = 0;
      }
    }


    @Override
    public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
    throws IOException, RuntimeException {
      return callable.withRetries();
    }

    @Override
    public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
    throws IOException, RuntimeException {
      return callable.withoutRetries();
    }

    @Deprecated
    private <R> Callable<MultiResponse> createCallable(
      final HRegionLocation loc, final MultiAction<R> multi,
      final byte [] tableName) {
      // TODO: This does not belong in here!!! St.Ack  HConnections should
      // not be dealing in Callables; Callables have HConnections, not other
      // way around.
      final HConnection connection = this;
      return new Callable<MultiResponse>() {
       public MultiResponse call() throws IOException {
         ServerCallable<MultiResponse> callable =
           new ServerCallable<MultiResponse>(connection, tableName, null) {
             public MultiResponse call() throws IOException {
               return ProtobufUtil.multi(server, multi);
            }
             @Override
             public void connect(boolean reload) throws IOException {
               server = connection.getClient(
                 loc.getHostname(), loc.getPort());
             }
           };
         return callable.withoutRetries();
       }
     };
   }

    void updateCachedLocation(HRegionLocation hrl, String hostname, int port) {
      HRegionLocation newHrl = new HRegionLocation(hrl.getRegionInfo(), hostname, port);
      synchronized (this.cachedRegionLocations) {
        cacheLocation(hrl.getRegionInfo().getTableName(), newHrl);
      }
    }

    void deleteCachedLocation(HRegionLocation rl) {
      synchronized (this.cachedRegionLocations) {
        Map<byte[], HRegionLocation> tableLocations =
          getTableLocations(rl.getRegionInfo().getTableName());
        tableLocations.remove(rl.getRegionInfo().getStartKey());
      }
    }

    private void updateCachedLocations(
      UpdateHistory updateHistory, HRegionLocation hrl, Object t) {
      updateCachedLocations(updateHistory, hrl, null, null, t);
    }

    private void updateCachedLocations(UpdateHistory updateHistory, byte[] tableName, Row row,
      Object t) {
      updateCachedLocations(updateHistory, null, tableName, row, t);
    }

    /**
     * Update the location with the new value (if the exception is a RegionMovedException) or delete
     *  it from the cache.
     * We need to keep an history of the modifications, because we can have first an update then a
     *  delete. The delete would remove the update.
     * @param updateHistory - The set used for the history
     * @param hrl - can be null. If it's the case, tableName and row should not be null
     * @param tableName - can be null if hrl is not null.
     * @param row  - can be null if hrl is not null.
     * @param exception - An object (to simplify user code) on which we will try to find a nested
     *                  or wrapped or both RegionMovedException
     */
    private void updateCachedLocations(
      UpdateHistory updateHistory, final HRegionLocation hrl, final byte[] tableName,
      Row row, final Object exception) {

      if ((row == null || tableName == null) && hrl == null){
        LOG.warn ("Coding error, see method javadoc. row="+row+", tableName="+
          Bytes.toString(tableName)+", hrl="+hrl);
        return;
      }

      // Is it something we have already updated?
      final HRegionLocation myLoc = (hrl != null ?
        hrl : getCachedLocation(tableName, row.getRow()));
      if (myLoc == null) {
        // There is no such location in the cache => it's been removed already => nothing to do
        return;
      }

      final String regionName = myLoc.getRegionInfo().getRegionNameAsString();

      if (updateHistory.contains(regionName)) {
        // Already updated/deleted => nothing to do
        return;
      }

      updateHistory.add(regionName);

      final RegionMovedException rme = RegionMovedException.find(exception);
      if (rme != null) {
        LOG.info("Region " + myLoc.getRegionInfo().getRegionNameAsString() + " moved from " +
          myLoc.getHostnamePort() + ", updating client location cache." +
          " New server: " + rme.getHostname() + ":" + rme.getPort());
        updateCachedLocation(myLoc, rme.getHostname(), rme.getPort());
      } else {
        deleteCachedLocation(myLoc);
      }
    }

    @Override
    @Deprecated
    public void processBatch(List<? extends Row> list,
        final byte[] tableName,
        ExecutorService pool,
        Object[] results) throws IOException, InterruptedException {
      // This belongs in HTable!!! Not in here.  St.Ack

      // results must be the same size as list
      if (results.length != list.size()) {
        throw new IllegalArgumentException(
          "argument results must be the same size as argument list");
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

    private static class UpdateHistory{
      private final Set<String> updateHistory = new HashSet<String>(100); // size: if we're doing a
      // rolling restart we may have 100 regions with a wrong location if we're really unlucky

      public boolean contains(String regionName) {
        return updateHistory.contains(regionName);
      }

      public void add(String regionName) {
        updateHistory.add(regionName);
      }
    }

    /**
     * Parameterized batch processing, allowing varying return types for
     * different {@link Row} implementations.
     */
    @Override
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
        UpdateHistory updateHistory = new UpdateHistory();

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
            HRegionLocation loc = locateRegion(tableName, row.getRow(), true);
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
            LOG.debug("Failed all from " + loc, e);
            updateCachedLocations(updateHistory, loc, e);
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
            updateCachedLocations(updateHistory, tableName, row, results[i]);
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
      Integer key = Bytes.mapKey(tableName);
      synchronized (this.cachedRegionLocations) {
        Map<byte[], HRegionLocation> tableLocs =
          this.cachedRegionLocations.get(key);

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

    @Override
    public void setRegionCachePrefetch(final byte[] tableName,
        final boolean enable) {
      if (!enable) {
        regionCachePrefetchDisabledTables.add(Bytes.mapKey(tableName));
      }
      else {
        regionCachePrefetchDisabledTables.remove(Bytes.mapKey(tableName));
      }
    }

    @Override
    public boolean getRegionCachePrefetch(final byte[] tableName) {
      return !regionCachePrefetchDisabledTables.contains(Bytes.mapKey(tableName));
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
      if (t instanceof KeeperException.SessionExpiredException
        && keepAliveZookeeper != null) {
        synchronized (masterAndZKLock) {
          if (keepAliveZookeeper != null) {
            LOG.warn("This client just lost it's session with ZooKeeper," +
              " closing it." +
              " It will be recreated next time someone needs it", t);
            closeZooKeeperWatcher();
          }
        }
      }else {
        if (t != null) {
          LOG.fatal(msg, t);
        } else {
          LOG.fatal(msg);
        }
        this.aborted = true;
        this.closed = true;
      }
    }

    @Override
    public boolean isClosed() {
      return this.closed;
    }

    @Override
    public boolean isAborted(){
      return this.aborted;
    }

    @Override
    public int getCurrentNrHRS() throws IOException {
      ZooKeeperKeepAliveConnection zkw = getKeepAliveZooKeeperWatcher();

      try {
        // We go to zk rather than to master to get count of regions to avoid
        // HTable having a Master dependency.  See HBase-2828
        return ZKUtil.getNumberOfChildren(zkw, zkw.rsZNode);
      } catch (KeeperException ke) {
        throw new IOException("Unexpected ZooKeeper exception", ke);
      } finally {
          zkw.close();
      }
    }

    public void stopProxyOnClose(boolean stopProxy) {
      this.stopProxy = stopProxy;
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

    void close(boolean stopProxy) {
      if (this.closed) {
        return;
      }
      delayedClosing.stop("Closing connection");
      if (stopProxy) {
        closeMaster();
        for (Map<String, VersionedProtocol> i : servers.values()) {
          for (VersionedProtocol server: i.values()) {
            HBaseRPC.stopProxy(server);
          }
        }
      }
      closeZooKeeperWatcher();
      this.servers.clear();
      this.closed = true;
    }

    @Override
    public void close() {
      if (managed) {
        HConnectionManager.deleteConnection(this, stopProxy, false);
      } else {
        close(true);
      }
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
      super.finalize();
      // Pretend as if we are about to release the last remaining reference
      refCount = 1;
      close();
    }

    @Override
    public HTableDescriptor[] listTables() throws IOException {
      MasterKeepAliveConnection master = getKeepAliveMaster();
      try {
        return master.getHTableDescriptors();
      } finally {
        master.close();
      }
    }

    @Override
    public HTableDescriptor[] getHTableDescriptors(List<String> tableNames) throws IOException {
      if (tableNames == null || tableNames.isEmpty()) return new HTableDescriptor[0];
      MasterKeepAliveConnection master = getKeepAliveMaster();
      try {
        return master.getHTableDescriptors(tableNames);
      }finally {
        master.close();
      }
    }

    /**
     * Connects to the master to get the table descriptor.
     * @param tableName table name
     * @return
     * @throws IOException if the connection to master fails or if the table
     *  is not found.
     */
    @Override
    public HTableDescriptor getHTableDescriptor(final byte[] tableName)
    throws IOException {
      if (tableName == null || tableName.length == 0) return null;
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        return new UnmodifyableHTableDescriptor(HTableDescriptor.ROOT_TABLEDESC);
      }
      if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
        return HTableDescriptor.META_TABLEDESC;
      }
      MasterKeepAliveConnection master = getKeepAliveMaster();
      HTableDescriptor[] htds;
      try {
        htds = master.getHTableDescriptors();
      }finally {
        master.close();
      }
      if (htds != null && htds.length > 0) {
        for (HTableDescriptor htd: htds) {
          if (Bytes.equals(tableName, htd.getName())) {
            return htd;
          }
        }
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
