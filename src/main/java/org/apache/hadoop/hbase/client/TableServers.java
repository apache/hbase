/**
 * Copyright 2014 The Apache Software Foundation
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.SyncFailedException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HConnectionParams;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.HBaseThriftRPC;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionOverloadedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/* Encapsulates finding the servers for an HBase instance */
public class TableServers implements ServerConnection {
  static final Log LOG = LogFactory.getLog(TableServers.class);
  private final Class<? extends HRegionInterface> serverInterfaceClass;
  private final int prefetchRegionLimit;

  private final Object masterLock = new Object();
  private final int maxOutstandingRequestsPerServer;
  private volatile boolean closed;
  private volatile HMasterInterface master;
  private volatile boolean masterChecked;

  private final Object rootRegionLock = new Object();
  private final Object metaRegionLock = new Object();
  private final Object userRegionLock = new Object();

  private volatile Configuration conf;
  private final HConnectionParams params;

  // Used by master and region servers during safe mode only
  private volatile HRegionLocation rootRegionLocation;

  MetaCache metaCache = new MetaCache();

  // amount of time to wait before we consider a server to be in fast fail
  // mode
  protected long fastFailThresholdMilliSec;
  // Keeps track of failures when we cannot talk to a server. Helps in
  // fast failing clients if the server is down for a long time.
  protected final ConcurrentMap<HServerAddress, FailureInfo> repeatedFailuresMap = new ConcurrentHashMap<HServerAddress, FailureInfo>();
  // We populate repeatedFailuresMap every time there is a failure. So, to
  // keep it
  // from growing unbounded, we garbage collect the failure information
  // every cleanupInterval.
  protected final long failureMapCleanupIntervalMilliSec;
  protected volatile long lastFailureMapCleanupTimeMilliSec;
  // Amount of time that has to pass, before we clear region -> regionserver
  // cache
  // again, when in fast fail mode. This is used to clean unused entries.
  protected long cacheClearingTimeoutMilliSec;
  // clear failure Info. Used to clean out all entries.
  // A safety valve, in case the client does not exit the
  // fast fail mode for any reason.
  private long fastFailClearingTimeMilliSec;
  private final boolean recordClientContext;

  private ThreadLocal<List<OperationContext>> operationContextPerThread = new ThreadLocal<>();

  private final ConcurrentHashMap<HServerAddress, AtomicInteger> outstandingRequests = new ConcurrentHashMap<>();

  @Override
  public void resetOperationContext() {
    if (!recordClientContext || this.operationContextPerThread == null) {
      return;
    }

    List<OperationContext> currContext = this.operationContextPerThread.get();

    if (currContext != null) {
      currContext.clear();
    }
  }

  @Override
  public List<OperationContext> getAndResetOperationContext() {
    if (!recordClientContext || this.operationContextPerThread == null) {
      return null;
    }

    List<OperationContext> currContext = this.operationContextPerThread.get();

    if (currContext == null) {
      return null;
    }

    ArrayList<OperationContext> context = new ArrayList<OperationContext>(
        currContext);

    // Made a copy, clear the context
    currContext.clear();

    return context;
  }

  /**
   * Keeps track of repeated failures to any region server.
   *
   * @author amitanand.s
   *
   */
  protected class FailureInfo {
    // The number of consecutive failures.
    private final AtomicLong numConsecutiveFailures = new AtomicLong();
    // The time when the server started to become unresponsive
    // Once set, this would never be updated.
    private long timeOfFirstFailureMilliSec;
    // The time when the client last tried to contact the server.
    // This is only updated by one client at a time
    private volatile long timeOfLatestAttemptMilliSec;
    // The time when the client last cleared cache for regions assigned
    // to the server. Used to ensure we don't clearCache too often.
    private volatile long timeOfLatestCacheClearMilliSec;
    // Used to keep track of concurrent attempts to contact the server.
    // In Fast fail mode, we want just one client thread to try to connect
    // the rest of the client threads will fail fast.
    private final AtomicBoolean exclusivelyRetringInspiteOfFastFail = new AtomicBoolean(
        false);

    @Override
    public String toString() {
      return "FailureInfo: numConsecutiveFailures = "
          + numConsecutiveFailures + " timeOfFirstFailureMilliSec = "
          + timeOfFirstFailureMilliSec + " timeOfLatestAttemptMilliSec = "
          + timeOfLatestAttemptMilliSec
          + " timeOfLatestCacheClearMilliSec = "
          + timeOfLatestCacheClearMilliSec
          + " exclusivelyRetringInspiteOfFastFail  = "
          + exclusivelyRetringInspiteOfFastFail.get();
    }

    FailureInfo(long firstFailureTime) {
      this.timeOfFirstFailureMilliSec = firstFailureTime;
    }
  }

  private final ThreadLocal<MutableBoolean> threadRetryingInFastFailMode = new ThreadLocal<MutableBoolean>();

  // For TESTING purposes only;
  public Map<HServerAddress, FailureInfo> getFailureMap() {
    return repeatedFailuresMap;
  }
  // region cache prefetch is enabled by default. this set contains all
  // tables whose region cache prefetch are disabled.
  private final Set<Integer> regionCachePrefetchDisabledTables =
    new CopyOnWriteArraySet<Integer>();
  // keep track of servers that have been updated for batchedLoad
  // tablename -> Map
  final Map<StringBytes, ConcurrentMap<HRegionInfo, HRegionLocation>>
    batchedUploadUpdatesMap = new ConcurrentHashMap<>();
  private int batchedUploadSoftFlushRetries;
  private long batchedUploadSoftFlushTimeoutMillis;
  private final boolean useThrift;

  private Map<StringBytes, StringBytes> initializedTableSet =
      new ConcurrentHashMap<>();
  /**
   * constructor
   * @param conf Configuration object
   */
  @SuppressWarnings("unchecked")
  public TableServers(Configuration conf) {
    this.conf = conf;
    params = HConnectionParams.getInstance(conf);
    this.useThrift = conf.getBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
      HConstants.CLIENT_TO_RS_USE_THRIFT_DEFAULT);

    String serverClassName =
      conf.get(HConstants.REGION_SERVER_CLASS,
          HConstants.DEFAULT_REGION_SERVER_CLASS);

    this.closed = false;

    try {
      this.serverInterfaceClass =
        (Class<? extends HRegionInterface>) Class.forName(serverClassName);

    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
          "Unable to find region server interface " + serverClassName, e);
    }

    // TODO move parameters below into HConnectionParams
    this.cacheClearingTimeoutMilliSec = conf.getLong(
      "hbase.client.fastfail.cache.clear.interval",
        10000); // 10 sec
    this.fastFailThresholdMilliSec = conf.getLong(
      "hbase.client.fastfail.threshold",
        60000); // 1 min
    this.failureMapCleanupIntervalMilliSec = conf.getLong(
        "hbase.client.fastfail.cleanup.map.interval.millisec", 600000); // 10 min
    this.fastFailClearingTimeMilliSec = conf.getLong(
        "hbase.client.fastfail.cleanup.all.millisec", 900000); // 15 mins

    this.prefetchRegionLimit = conf.getInt("hbase.client.prefetch.limit",
        10);

    this.master = null;
    this.masterChecked = false;
    this.batchedUploadSoftFlushRetries =
        conf.getInt("hbase.client.batched-upload.softflush.retries", 10);
    this.batchedUploadSoftFlushTimeoutMillis =
        conf.getLong("hbase.client.batched-upload.softflush.timeout.ms", 60000L); // 1 min

    this.recordClientContext = conf.getBoolean("hbase.client.record.context", false);

    this.maxOutstandingRequestsPerServer = conf.getInt("hbase.client.max.outstanding.requests.per.server", 50);
  }

  // Used by master and region servers during safe mode only
  @Override
  public void unsetRootRegionLocation() {
    this.rootRegionLocation = null;
  }

  // Used by master and region servers during safe mode only
  @Override
  public void setRootRegionLocation(HRegionLocation rootRegion) {
    if (rootRegion == null) {
      throw new IllegalArgumentException(
          "Cannot set root region location to null.");
    }
    this.rootRegionLocation = rootRegion;
  }

  @Override
  public HMasterInterface getMaster() throws MasterNotRunningException {
    ZooKeeperWrapper zk;
    try {
      zk = getZooKeeperWrapper();
    } catch (IOException e) {
      throw new MasterNotRunningException(e);
    }

    HServerAddress masterLocation = null;
    synchronized (this.masterLock) {
      for (int tries = 0; !this.closed && !this.masterChecked
          && this.master == null && tries < params.getNumRetries(); tries++) {

        try {
          masterLocation = zk.readMasterAddress(zk);

          if (masterLocation != null) {
            HMasterInterface tryMaster = (HMasterInterface) HBaseRPC.getProxy(
                  HMasterInterface.class, HBaseRPCProtocolVersion.versionID,
                  masterLocation.getInetSocketAddress(), this.conf,
                  params.getRpcTimeout(), HBaseRPCOptions.DEFAULT);

            if (tryMaster.isMasterRunning()) {
              this.master = tryMaster;
              this.masterLock.notifyAll();
              break;
            }
          }

        } catch (IOException e) {
          if (tries == params.getNumRetries() - 1) {
            // This was our last chance - don't bother sleeping
            LOG.info(
                "getMaster attempt " + tries + " of "
                    + params.getNumRetries() + " failed; no more retrying.",
                e);
            break;
          }
          LOG.info(
              "getMaster attempt " + tries + " of " + params.getNumRetries()
                  + " failed; retrying after sleep of "
                  + params.getPauseTime(tries), e);
        }

        // Cannot connect to master or it is not running. Sleep & retry
        try {
          this.masterLock.wait(params.getPauseTime(tries));
        } catch (InterruptedException e) {
          // continue
        }
      }
      this.masterChecked = true;
    }
    if (this.master == null) {
      if (masterLocation == null) {
        throw new MasterNotRunningException();
      }
      throw new MasterNotRunningException(masterLocation.toString());
    }
    return this.master;
  }

  @Override
  public boolean isMasterRunning() {
    if (this.master == null) {
      try {
        getMaster();

      } catch (MasterNotRunningException e) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean tableExists(StringBytes tableName)
      throws MasterNotRunningException {
    getMaster();
    if (tableName == null) {
      throw new IllegalArgumentException("Table name cannot be null");
    }
    if (isMetaTableName(tableName)) {
      return true;
    }
    byte[] tname = Bytes.toBytes(tableName + ",,");
    Scan scan = new Scan(tname);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);

    try (HTable table = new HTable(conf, HConstants.META_TABLE_NAME)) {
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result data = scanner.next();
        if (data != null && data.size() > 0) {
          HRegionInfo info = Writables.getHRegionInfo(
            data.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));
          if (info.getTableDesc().getNameAsString().equals(tableName.getString())) {
            // A region for this table already exists. Ergo table exists.
            return true;
          }
        }
      } catch (IOException e) {
        LOG.warn("Testing for table existence threw exception", e);
      }
    } catch (IOException e) {
      LOG.warn("Testing for table existence threw exception", e);
    }
    return false;
  }

  /*
   * @param n
   *
   * @return Truen if passed tablename <code>n</code> is equal to the name of
   * a catalog table.
   */
  private static boolean isMetaTableName(StringBytes n) {
    return MetaUtils.isMetaTableName(n.getBytes());
  }

  @Override
  public HRegionLocation getRegionLocation(StringBytes tableName,
      final byte[] row, boolean reload) throws IOException {
    return reload ? relocateRegion(tableName, row) :
      locateRegion(tableName, row);
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    getMaster();
    final TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          HRegionInfo info = null;
          if (value != null) {
            info = Writables.getHRegionInfo(value);
          }
          // Only examine the rows where the startKey is zero length
          if (info != null && info.getStartKey().length == 0) {
            uniqueTables.add(info.getTableDesc());
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };
    MetaScanner.metaScan(conf, visitor);

    return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
  }

  @Override
  public boolean isTableEnabled(StringBytes tableName) throws IOException {
    return testTableOnlineState(tableName, true);
  }

  @Override
  public boolean isTableDisabled(StringBytes tableName) throws IOException {
    return testTableOnlineState(tableName, false);
  }

  @Override
  public boolean isTableAvailable(final StringBytes tableName)
      throws IOException {
    final AtomicBoolean available = new AtomicBoolean(true);
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result row) throws IOException {
        byte[] value = row.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        HRegionInfo info = Writables.getHRegionInfoOrNull(value);
        if (info != null) {
          if (tableName.equalBytes(info.getTableDesc().getName())) {
            value = row.getValue(HConstants.CATALOG_FAMILY,
                HConstants.SERVER_QUALIFIER);
            if (value == null) {
              available.set(false);
              return false;
            }
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(conf, visitor);
    return available.get();
  }

  /*
   * If online == true Returns true if all regions are online Returns false in
   * any other case If online == false Returns true if all regions are offline
   * Returns false in any other case
   */
  private boolean testTableOnlineState(StringBytes tableName, boolean online)
      throws IOException {
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(tableName.toString());
    }
    if (tableName.equals(HConstants.ROOT_TABLE_NAME_STRINGBYTES)) {
      // The root region is always enabled
      return true;
    }
    int rowsScanned = 0;
    int rowsOffline = 0;
    byte[] startKey =
        HRegionInfo.createRegionName(tableName.getBytes(), null,
        HConstants.ZEROES, false);
    byte[] endKey;
    HRegionInfo currentRegion;
    Scan scan = new Scan(startKey);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    int rows = this.conf.getInt("hbase.meta.scanner.caching", 100);
    scan.setCaching(rows);
    StringBytes parentTable =
        tableName.equals(HConstants.META_TABLE_NAME_STRINGBYTES)
        ? HConstants.ROOT_TABLE_NAME_STRINGBYTES
        : HConstants.META_TABLE_NAME_STRINGBYTES;
    ScannerCallable s = new ScannerCallable(this, parentTable, scan,
        HBaseRPCOptions.DEFAULT);
    try {
      // Open scanner
      getRegionServerWithRetries(s);
      do {
        currentRegion = s.getHRegionInfo();
        Result r;
        Result[] rrs;
        while ((rrs = getRegionServerWithRetries(s)) != null
            && rrs.length > 0) {
          r = rrs[0];
          byte[] value = r.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          if (value != null) {
            HRegionInfo info = Writables.getHRegionInfoOrNull(value);
            if (info != null) {
              if (tableName.equalBytes(info.getTableDesc().getName())) {
                rowsScanned += 1;
                rowsOffline += info.isOffline() ? 1 : 0;
              }
            }
          }
        }
        endKey = currentRegion.getEndKey();
      } while (!(endKey == null || Bytes.equals(endKey,
          HConstants.EMPTY_BYTE_ARRAY)));
    } finally {
      s.setClose();
      // Doing below will call 'next' again and this will close the scanner
      // Without it we leave scanners open.
      getRegionServerWithRetries(s);
    }
    LOG.debug("Rowscanned=" + rowsScanned + ", rowsOffline=" + rowsOffline);
    boolean onOffLine = online ? rowsOffline == 0
        : rowsOffline == rowsScanned;
    return rowsScanned > 0 && onOffLine;
  }

  private static class HTableDescriptorFinder implements
      MetaScanner.MetaScannerVisitor {
    byte[] tableName;
    HTableDescriptor result;

    protected HTableDescriptorFinder(byte[] tableName) {
      this.tableName = tableName;
    }

    @Override
    public boolean processRow(Result rowResult) throws IOException {
      HRegionInfo info = Writables.getHRegionInfo(rowResult.getValue(
          HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
      HTableDescriptor desc = info.getTableDesc();
      if (Bytes.compareTo(desc.getName(), tableName) == 0) {
        result = desc;
        return false;
      }
      return true;
    }

    HTableDescriptor getResult() {
      return result;
    }
  }

  @Override
  public HTableDescriptor getHTableDescriptor(StringBytes tableName)
      throws IOException {
    if (tableName.equals(HConstants.ROOT_TABLE_NAME_STRINGBYTES)) {
      if (HTableDescriptor.isMetaregionSeqidRecordEnabled(conf)) {
        return new UnmodifyableHTableDescriptor(HTableDescriptor.ROOT_TABLEDESC_WITH_HISTORIAN_COLUMN);
      } else {
        return new UnmodifyableHTableDescriptor(HTableDescriptor.ROOT_TABLEDESC);
      }
    }
    if (tableName.equals(HConstants.META_TABLE_NAME_STRINGBYTES)) {
      return HTableDescriptor.META_TABLEDESC;
    }
    TableServers.HTableDescriptorFinder finder =
        new HTableDescriptorFinder(tableName.getBytes());
    MetaScanner.metaScan(conf, finder, tableName);
    HTableDescriptor result = finder.getResult();
    if (result == null) {
      throw new TableNotFoundException(tableName.toString());
    }
    return result;
  }

  @Override
  public HRegionLocation locateRegion(StringBytes tableName, final byte[] row)
      throws IOException {
    return locateRegion(tableName, row, true);
  }

  @Override
  public HRegionLocation relocateRegion(StringBytes tableName, byte[] row)
      throws IOException {
    return locateRegion(tableName, row, false);
  }

  private HRegionLocation locateRegion(StringBytes tableName,
      final byte[] row, boolean useCache) throws IOException {
    if (tableName.isEmpty()) {
      throw new IllegalArgumentException(
          "table name cannot be null or zero length");
    }
    if (tableName.equals(HConstants.META_TABLE_NAME_STRINGBYTES)) {
      return locateMetaInRoot(row, useCache, metaRegionLock);
    } else if (tableName.equals(HConstants.ROOT_TABLE_NAME_STRINGBYTES)) {
      synchronized (rootRegionLock) {
        // This block guards against two threads trying to find the root
        // region at the same time. One will go do the find while the
        // second waits. The second thread will not do find.

        if (!useCache || rootRegionLocation == null
            || inFastFailMode(this.rootRegionLocation.getServerAddress())) {
          this.rootRegionLocation = locateRootRegion();
          LOG.info("Updated rootRegionLocation from ZK to : " + this.rootRegionLocation);
        }
        return this.rootRegionLocation;
      }
    } else {
      // Region not in the cache - have to go to the meta RS
      return locateRegionInMeta(HConstants.META_TABLE_NAME_STRINGBYTES,
          tableName, row, useCache, userRegionLock);
    }
  }

  private HRegionLocation prefetchRegionCache(final StringBytes tableName,
      final byte[] row) {
    return prefetchRegionCache(tableName, row, this.prefetchRegionLimit);
  }

  /*
   * Search .META. for the HRegionLocation info that contains the table and
   * row we're seeking. It will prefetch certain number of regions info and
   * save them to the global region cache.
   *
   * @param the row in the .META. table. If it is null, all cache will be
   *        prefetched and null is returned.
   */
  public HRegionLocation prefetchRegionCache(final StringBytes tableName,
      final byte[] row, int prefetchRegionLimit) {
    // Implement a new visitor for MetaScanner, and use it to walk through
    // the .META.
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          HRegionInfo regionInfo = null;

          if (value != null) {
            // convert the row result into the HRegionLocation we need!
            regionInfo = Writables.getHRegionInfo(value);

            // possible we got a region of a different table...
            if (!tableName.equalBytes(regionInfo.getTableDesc().getName())) {
              return false; // stop scanning
            }
            if (regionInfo.isOffline()) {
              // don't cache offline regions
              return true;
            }
            value = result.getValue(HConstants.CATALOG_FAMILY,
                HConstants.SERVER_QUALIFIER);
            if (value == null) {
              return true; // don't cache it
            }
            final String serverAddress = Bytes.toString(value);

            value = result.getValue(HConstants.CATALOG_FAMILY,
                HConstants.STARTCODE_QUALIFIER);
            long serverStartCode = -1;
            if (value != null) {
              serverStartCode = Bytes.toLong(value);
            }

            // instantiate the location
            HRegionLocation loc = new HRegionLocation(regionInfo,
                new HServerAddress(serverAddress), serverStartCode);
            // cache this meta entry
            metaCache.add(tableName, loc);
          }
          return true;
        } catch (RuntimeException e) {
          throw new IOException(e);
        }
      }
    };
    try {
      // prefetch certain number of regions info at region cache.
      MetaScanner.metaScan(conf, visitor, tableName, row, prefetchRegionLimit);
      if (row == null) {
        return null;
      }
      return metaCache.getForRow(tableName, row);
    } catch (IOException e) {
      LOG.warn("Encounted problems when prefetch META table: ", e);
    }
    return null;
  }

  /**
    * Search the meta table (.META.) for the HRegionLocation info that
    * contains the table and row we're seeking.
    */
  private HRegionLocation locateRegionInMeta(StringBytes parentTable,
      final StringBytes tableName, final byte[] row, boolean useCache,
    Object regionLockObject)
  throws IOException {
    HRegionLocation location;
    if (useCache) {
      location = metaCache.getForRow(tableName, row);
      if (location != null) {
        return location;
      }
    }

    // If we are supposed to be using the cache, look in the cache to see if
    // we already have the region.

    // build the key of the meta region we should be looking for.
    // the extra 9's on the end are necessary to allow "exact" matches
    // without knowing the precise region names.
    byte[] metaKey = HRegionInfo.createRegionName(tableName.getBytes(), row,
      HConstants.NINES, false);
    for (int tries = 0; true; tries++) {
      if (tries >= params.getNumRetries()) {
        throw new NoServerForRegionException("Unable to find region for "
          + Bytes.toStringBinary(row) + " after " + params.getNumRetries() +
          " tries.");
      }

      FailureInfo fInfo = null;
      HServerAddress server = null;
      boolean didTry = false;
      boolean couldNotCommunicateWithServer = false;
      boolean retryDespiteFastFailMode = false;
      try {
        // locate the root or meta region
        HRegionLocation metaLocation = locateRegion(parentTable, metaKey);

        server = metaLocation.getServerAddress();
        fInfo = repeatedFailuresMap.get(server);

        // Handle the case where .META. is on an unresponsive server.
        if (inFastFailMode(server) &&
            !this.currentThreadInFastFailMode()) {
          // In Fast-fail mode, all but one thread will fast fail. Check
          // if we are that one chosen thread.

          retryDespiteFastFailMode = shouldRetryInspiteOfFastFail(fInfo);

          if (retryDespiteFastFailMode == false) { // we don't have to retry
            throw new PreemptiveFastFailException(fInfo.numConsecutiveFailures.get(),
                fInfo.timeOfFirstFailureMilliSec, fInfo.timeOfLatestAttemptMilliSec, server.getHostname());
          }
        }
        didTry = true;

        HBaseThriftRPC.isMeta.get().push(true);
        Result regionInfoRow = null;
        try {
          // This block guards against two threads trying to load the meta
          // region at the same time. The first will load the meta region and
          // the second will use the value that the first one found.
          synchronized (regionLockObject) {
            // Check the cache again for a hit in case some other thread made the
            // same query while we were waiting on the lock. If not supposed to
            // be using the cache, delete any existing cached location so it won't
            // interfere.
            if (useCache) {
              location = metaCache.getForRow(tableName, row);
              if (location != null) {
                return location;
              }
            } else {
              LOG.debug("Deleting the client location cache.");
              deleteCachedLocation(tableName, row, null);
            }

            // If the parent table is META, we may want to pre-fetch some
            // region info into the global region cache for this table.
            if (parentTable.equals(HConstants.META_TABLE_NAME_STRINGBYTES)
                && getRegionCachePrefetch(tableName)) {
              LOG.debug("Prefetching the client location cache.");
              location = prefetchRegionCache(tableName, row);
              if (location != null) {
                return location;
              }
            }

            HRegionInterface serverInterface =
              getHRegionConnection(metaLocation.getServerAddress());

            // Query the root or meta region for the location of the meta region
            regionInfoRow = serverInterface.getClosestRowBefore(
              metaLocation.getRegionInfo().getRegionName(), metaKey,
              HConstants.CATALOG_FAMILY);
        }
      } catch (Exception e) {
        throw e;
      } finally {
        HBaseThriftRPC.isMeta.get().pop();
      }
      location = getLocationFromRow(regionInfoRow, tableName,
        parentTable, row);
      metaCache.add(tableName, location);
      return location;
    } catch (TableNotFoundException e) {
      // if we got this error, probably means the table just plain doesn't
      // exist. rethrow the error immediately. this should always be coming
      // from the HTable constructor.
      throw e;
    } catch (PreemptiveFastFailException e) {
      // already processed this. Don't process this again.
      throw e;
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        e = RemoteExceptionHandler
          .decodeRemoteException((RemoteException) e);
      } else if (isNetworkException(e)) {
        couldNotCommunicateWithServer = true;
        handleFailureToServer(server, e);
      }
      if (tries < params.getNumRetries() - 1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("locateRegionInMeta attempt " + tries + " of "
            + params.getNumRetries()
            + " failed; retrying after sleep of "
            + params.getPauseTime(tries) + " because: " + e.getMessage());
        }
      } else {
        throw e;
      }
      // Only relocate the parent region if necessary
      if (!(e instanceof RegionOfflineException || e instanceof NoServerForRegionException)) {
        relocateRegion(parentTable, row);
      }
    } catch (Exception e) {
      couldNotCommunicateWithServer = true;
      handleFailureToServer(server, e);
      if (tries < params.getNumRetries() - 1) {
        LOG.debug("locateRegionInMeta attempt " + tries + " of "
          + params.getNumRetries() + " failed; retrying after sleep of "
          + params.getPauseTime(tries) + " because: " + e.getMessage());
      } else {
        throw e;
      }
    } finally {
      updateFailureInfoForServer(server, fInfo, didTry,
        couldNotCommunicateWithServer, retryDespiteFastFailMode);
    }
    try {
      Thread.sleep(params.getPauseTime(tries));
    } catch (InterruptedException e) {
      // continue
    }
  }
}

private HRegionLocation getLocationFromRow(Result regionInfoRow,
      StringBytes tableName, StringBytes parentTable, byte[] row)
      throws IOException {
  if (regionInfoRow == null) {
      throw new TableNotFoundException(tableName.toString());
  }
  byte[] value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY,
    HConstants.REGIONINFO_QUALIFIER);
  if (value == null || value.length == 0) {
      throw new IOException("HRegionInfo was null or empty in " + parentTable
          + ", row=" + regionInfoRow);
  }
  // convert the row result into the HRegionLocation we need!
  HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(value,
    new HRegionInfo());
  // possible we got a region of a different table...
    if (!tableName.equalBytes(regionInfo.getTableDesc().getName())) {
      throw new TableNotFoundException("Table '" + tableName
      + "' was not found.");
  }
  if (regionInfo.isOffline()) {
    throw new RegionOfflineException("region offline: "
      + regionInfo.getRegionNameAsString());
  }

  value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY,
    HConstants.SERVER_QUALIFIER);
  String serverAddress = "";
  if (value != null) {
    serverAddress = Bytes.toString(value);
  }
  if (serverAddress.equals("")) {
      throw new NoServerForRegionException("No server address listed in "
          + parentTable + " for region " + regionInfo.getRegionNameAsString()
          + " containing row " + Bytes.toStringBinary(row));
  }

  value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY,
    HConstants.STARTCODE_QUALIFIER);
  long serverStartCode = -1;
  if (value != null) {
    serverStartCode = Bytes.toLong(value);
  }
  // instantiate the location
  HRegionLocation location = new HRegionLocation(regionInfo,
    new HServerAddress(serverAddress), serverStartCode);
  return location;
}

/**
 * TODO:WARNING!!! This looks like a lot of duplicated code with
 * {@link #locateRegionInMeta(byte[], byte[], byte[], boolean, Object)} pls
 * fix this! Search the Root Table for the Meta Region. Retries a fixed
 * number of times and throws if Region is not found.
 */
private HRegionLocation locateMetaInRoot(final byte[] row,
                                         boolean useCache, Object regionLockObject) throws IOException {
  HRegionLocation location;
  final StringBytes parentTable = HConstants.ROOT_TABLE_NAME_STRINGBYTES;
  final StringBytes tableName = HConstants.META_TABLE_NAME_STRINGBYTES;
  if (useCache) {
      location = metaCache.getForRow(tableName, row);
    if (location != null) {
      return location;
    }
  }
  // If we are supposed to be using the cache, look in the cache to see if
  // we already have the region.

  // build the key of the meta region we should be looking for.
  // the extra 9's on the end are necessary to allow "exact" matches
  // without knowing the precise region names.
  byte[] metaKey = HRegionInfo.createRegionName(tableName.getBytes(), row,
    HConstants.NINES, false);
  for (int tries = 0; true; tries++) {
    if (tries >= params.getNumRetries()) {
      throw new NoServerForRegionException("Unable to find region for "
        + Bytes.toStringBinary(row) + " after " + params.getNumRetries()
        + " tries.");
    }

    TableServers.FailureInfo fInfo = null;
    HServerAddress server = null;
    boolean didTry = false;
    boolean couldNotCommunicateWithServer = false;
    boolean retryDespiteFastFailMode = false;
    try {
      // locate the root or meta region
      HRegionLocation metaLocation = null;
      if (useCache && rootRegionLocation != null
        && !inFastFailMode(this.rootRegionLocation.getServerAddress())) {
        metaLocation = rootRegionLocation;
      } else {
        synchronized (rootRegionLock) {
          // This block guards against two threads trying to find the root
          // region at the same time. One will go do the find while the
          // second waits. The second thread will not do find.

          if (!useCache || rootRegionLocation == null
            || inFastFailMode(this.rootRegionLocation.getServerAddress())) {
            HBaseThriftRPC.isMeta.get().push(true);
            try {
              this.rootRegionLocation = locateRootRegion();
            } catch (Exception e) {
              throw e;
            } finally {
              HBaseThriftRPC.isMeta.get().pop();
            }
            LOG.info("Updated rootRegionLocation from ZK to : "
              + this.rootRegionLocation);
          }
          metaLocation = this.rootRegionLocation;
        }
      }
      Preconditions.checkNotNull(metaLocation);
      server = metaLocation.getServerAddress();
      fInfo = repeatedFailuresMap.get(server);

      // Handle the case where .META. is on an unresponsive server.
      if (inFastFailMode(server) && !this.currentThreadInFastFailMode()) {
        // In Fast-fail mode, all but one thread will fast fail. Check
        // if we are that one chosen thread.

        retryDespiteFastFailMode = shouldRetryInspiteOfFastFail(fInfo);

        if (retryDespiteFastFailMode == false) { // we don't have to retry
          throw new PreemptiveFastFailException(
            fInfo.numConsecutiveFailures.get(),
            fInfo.timeOfFirstFailureMilliSec,
            fInfo.timeOfLatestAttemptMilliSec, server.getHostname());
        }
      }
      didTry = true;
      HBaseThriftRPC.isMeta.get().push(true);
      try {
        Result regionInfoRow = null;
        // This block guards against two threads trying to load the meta
        // region at the same time. The first will load the meta region and
        // the second will use the value that the first one found.
        synchronized (metaRegionLock) {
          if (useCache) {
              location = metaCache.getForRow(tableName, row);
            if (location != null) {
              return location;
            }
          } else {
            LOG.debug("Deleting the client location cache.");
            deleteCachedLocation(tableName, row, null);
          }
          HRegionInterface serverInterface = getHRegionConnection(metaLocation
            .getServerAddress());

          // Query the root for the location of the meta region
          regionInfoRow = serverInterface.getClosestRowBefore(metaLocation
            .getRegionInfo().getRegionName(), metaKey,
            HConstants.CATALOG_FAMILY);
          location = getLocationFromRow(regionInfoRow, tableName,
            parentTable, row);
          metaCache.add(tableName, location);
        }
      } catch (Throwable t) {
        throw t;
      } finally {
        HBaseThriftRPC.isMeta.get().pop();
      }
      return location;
    } catch (TableNotFoundException e) {
      // if we got this error, probably means the table just plain doesn't
      // exist. rethrow the error immediately. this should always be coming
      // from the HTable constructor.
      throw e;
    } catch (PreemptiveFastFailException e) {
      // already processed this. Don't process this again.
      throw e;
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        e = RemoteExceptionHandler
          .decodeRemoteException((RemoteException) e);
      } else if (isNetworkException(e)) {
        couldNotCommunicateWithServer = true;
        handleFailureToServer(server, e);
      }
      if (tries < params.getNumRetries() - 1) {
        LOG.debug("IOException locateRegionInMeta attempt " + tries
          + " of " + params.getNumRetries()
          + " failed; retrying after sleep of "
          + params.getPauseTime(tries) + " because: " + e.getMessage(), e);
      } else {
        throw e;
      }
    } catch (Exception e) {
      couldNotCommunicateWithServer = true;
      handleFailureToServer(server, e);
      if (tries < params.getNumRetries() - 1) {
        LOG.debug("Exception locateRegionInMeta attempt " + tries + " of "
          + params.getNumRetries() + " failed; retrying after sleep of "
          + params.getPauseTime(tries) + " because: " + e.getMessage());
      } else {
        throw e;
      }
    } finally {
      updateFailureInfoForServer(server, fInfo, didTry,
        couldNotCommunicateWithServer, retryDespiteFastFailMode);
    }
    try {
      Thread.sleep(params.getPauseTime(tries));
    } catch (InterruptedException e) {
      // continue
    }
  }
}




  /**
   * Check if the exception is something that indicates that we cannot
   * contact/communicate with the server.
   *
   * @param e
   * @return
   */
  private boolean isNetworkException(Throwable e) {
    // This list covers most connectivity exceptions but not all.
    // For example, in SocketOutputStream a plain IOException is thrown
    // at times when the channel is closed.
    return (e instanceof SocketTimeoutException ||
            e instanceof ConnectException ||
            e instanceof ClosedChannelException ||
            e instanceof SyncFailedException ||
            e instanceof EOFException ||
            e instanceof TTransportException);
  }

  @Override
  public Collection<HRegionLocation> getCachedHRegionLocations(
      StringBytes tableName, boolean forceRefresh) {
    if (forceRefresh || !initializedTableSet.containsKey(tableName)) {
      prefetchRegionCache(tableName, null, Integer.MAX_VALUE);
      initializedTableSet.put(tableName, tableName);
    }

    return metaCache.getForTable(tableName).values();
  }

  @Override
  public void deleteCachedLocation(StringBytes tableName, final byte[] row,
      HServerAddress oldServer) {
    metaCache.deleteForRow(tableName, row, oldServer);
  }

  /**
   * Allows flushing the region cache.
   */
  @Override
  public void clearRegionCache() {
    metaCache.clear();
  }

  @SuppressWarnings("resource")
  @Override
  public HRegionInterface getHRegionConnection(
      HServerAddress regionServer, boolean getMaster, HBaseRPCOptions options)
  throws IOException {
    if (getMaster) {
      LOG.debug("Getting master");
      getMaster();
    }
    HRegionInterface server = HRegionServer.getMainRS(regionServer);
    if (server != null && !this.useThrift) {
      return server;
    }

    final boolean thriftPortWrittenToMeta = conf.getBoolean(
       HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META,
       HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META_DEFAULT);
    final boolean hadoopPortWrittenToMeta = !thriftPortWrittenToMeta;

    try {
      // establish an RPC for this RS
      // set hbase.ipc.client.connect.max.retries to retry connection
      // attempts
      if (this.useThrift) {
        Class<? extends ThriftClientInterface> serverInterface =
            ThriftHRegionInterface.Async.class;
        if (thriftPortWrittenToMeta) {
          server = (HRegionInterface) HBaseThriftRPC.getClient(
              regionServer.getInetSocketAddress(), this.conf,
              serverInterface, options);
        } else {
          InetSocketAddress addr = new InetSocketAddress(regionServer
              .getInetSocketAddress().getHostName(), conf.getInt(
                  HConstants.REGIONSERVER_SWIFT_PORT,
                  HConstants.DEFAULT_REGIONSERVER_SWIFT_PORT));
          server = (HRegionInterface) HBaseThriftRPC.getClient(addr,
              this.conf, serverInterface, options);
        }
      } else {
        if (hadoopPortWrittenToMeta) {
          server = (HRegionInterface) HBaseRPC.getProxy(serverInterfaceClass,
              HBaseRPCProtocolVersion.versionID,
              regionServer.getInetSocketAddress(), this.conf,
            params.getRpcTimeout(), options);
        } else {
          InetSocketAddress addr = new InetSocketAddress(regionServer
            .getInetSocketAddress().getHostName(), conf.getInt(
            HConstants.REGIONSERVER_PORT,
            HConstants.DEFAULT_REGIONSERVER_PORT));
          server = (HRegionInterface) HBaseRPC.getProxy(serverInterfaceClass,
            HBaseRPCProtocolVersion.versionID, addr, this.conf,
            params.getRpcTimeout(), options);
        }
      }
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    return server;
  }

  @Override
  public HRegionInterface getHRegionConnection(HServerAddress regionServer,
      HBaseRPCOptions options) throws IOException {
    return getHRegionConnection(regionServer, false, options);
  }

  @Override
  public HRegionInterface getHRegionConnection(HServerAddress regionServer,
      boolean getMaster) throws IOException {
    return getHRegionConnection(regionServer, getMaster,
        HBaseRPCOptions.DEFAULT);
  }

  @Override
  public HRegionInterface getHRegionConnection(HServerAddress regionServer)
      throws IOException {
    return getHRegionConnection(regionServer, false);
  }

  @Override
  public synchronized ZooKeeperWrapper getZooKeeperWrapper()
      throws IOException {
    return HConnectionManager.getClientZKConnection(conf)
        .getZooKeeperWrapper();
  }

  /*
   * Repeatedly try to find the root region in ZK
   *
   * @return HRegionLocation for root region if found
   *
   * @throws NoServerForRegionException - if the root region can not be
   * located after retrying
   *
   * @throws IOException
   */
  private HRegionLocation locateRootRegion() throws IOException {

    // We lazily instantiate the ZooKeeper object because we don't want to
    // make the constructor have to throw IOException or handle it itself.
    ZooKeeperWrapper zk = getZooKeeperWrapper();

    HServerAddress rootRegionAddress = null;
    for (int tries = 0; tries < params.getNumRetries(); tries++) {
      int localTimeouts = 0;
      // ask the master which server has the root region
      while (rootRegionAddress == null
          && localTimeouts < params.getNumRetries()) {
        // Don't read root region until we're out of safe mode so we know
        // that the meta regions have been assigned.
        rootRegionAddress = zk.readRootRegionLocation();
        if (rootRegionAddress == null) {
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Sleeping " + params.getPauseTime(tries)
                  + "ms, waiting for root region.");
            }
            Thread.sleep(params.getPauseTime(tries));
          } catch (InterruptedException iex) {
            // continue
          }
          localTimeouts++;
        }
      }

      if (rootRegionAddress == null) {
        throw new NoServerForRegionException(
            "Timed out trying to locate root region");
      }

      LOG.debug("Trying to get root region at : " + rootRegionAddress);
      try {
        // Get a connection to the region server
        HRegionInterface server = getHRegionConnection(rootRegionAddress);
        // if this works, then we're good, and we have an acceptable address,
        // so we can stop doing retries and return the result.
        // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same getRegionName()
        server.getRegionInfo(HRegionInfo.ROOT_REGIONINFO.getRegionName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found ROOT at " + rootRegionAddress);
        }
        break;
      } catch (Throwable t) {
        t = translateException(t);

        if (tries == params.getNumRetries() - 1) {
          throw new NoServerForRegionException("Timed out trying to locate "+
              "root region because: " + t.getMessage());
        }

        // Sleep and retry finding root region.
        try {
          LOG.debug("Root region location changed. Sleeping.", t);
          Thread.sleep(params.getPauseTime(tries));
          LOG.debug("Wake. Retry finding root region.");
        } catch (InterruptedException iex) {
          // continue
        }
      }

      rootRegionAddress = null;
    }

    // if the address is null by this point, then the retries have failed,
    // and we're sort of sunk
    if (rootRegionAddress == null) {
      throw new NoServerForRegionException(
          "unable to locate root region server");
    }

    // return the region location
    if (HTableDescriptor.isMetaregionSeqidRecordEnabled(conf)) {
      return new HRegionLocation(HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN, rootRegionAddress);
    } else {
      return new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, rootRegionAddress);
    }
  }

  @Override
  public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
      throws IOException {
    List<Throwable> exceptions = new ArrayList<Throwable>();
    RegionOverloadedException roe = null;

    long callStartTime;
    int serverRequestedRetries = 0;

    callStartTime = System.currentTimeMillis();
    long serverRequestedWaitTime = 0;
    // do not retry if region cannot be located. There are enough retries
    // within instantiateRegionLocation.
    callable.instantiateRegionLocation(false /* reload cache? */);

    for (int tries = 0;; tries++) {
      // If server requested wait. We will wait for that time, and start
      // again. Do not count this time/tries against the client retries.
      if (serverRequestedWaitTime > 0) {
        serverRequestedRetries++;

        if (serverRequestedRetries > params.getMaxServerRequestedRetries()) {
          throw RegionOverloadedException.create(roe, exceptions,
              serverRequestedWaitTime);
        }

        long pauseTime = serverRequestedWaitTime + callStartTime
            - System.currentTimeMillis();
        LOG.debug("Got a BlockingWritesRetryLaterException: sleeping for "
            + pauseTime + "ms. serverRequestedRetries = "
            + serverRequestedRetries);
        try {
          Thread.sleep(pauseTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }

        serverRequestedWaitTime = 0;
        tries = 0;
        callStartTime = System.currentTimeMillis();
      }

      try {
        return getRegionServerWithoutRetries(callable, false);
      } catch (DoNotRetryIOException ioe) {
        // clear cache if needed
        if (ioe.getCause() instanceof NotServingRegionException) {
          HRegionLocation prevLoc = callable.location;
          if (prevLoc.getRegionInfo() != null) {
            deleteCachedLocation(callable.tableName, prevLoc.getRegionInfo()
                .getStartKey(), prevLoc.getServerAddress());
          }
        }

        // If we are not supposed to retry; Let it pass through.
        throw ioe;
      } catch (RegionOverloadedException ex) {
        roe = ex;
        serverRequestedWaitTime = roe.getBackoffTimeMillis();
        continue;
      } catch (ClientSideDoNotRetryException exp) {
        // Bail out of the retry loop, immediately
        throw exp;
      } catch (PreemptiveFastFailException pfe) {
        // Bail out of the retry loop, if the host has been consistently
        // unreachable.
        throw pfe;
      } catch (Throwable t) {
        exceptions.add(t);

        if (tries == params.getNumRetries() - 1) {
          throw new RetriesExhaustedException(callable.getServerName(),
              callable.getRegionName(), callable.getRow(), tries, exceptions);
        }

        HRegionLocation prevLoc = callable.location;
        if (prevLoc.getRegionInfo() != null) {
          deleteCachedLocation(callable.tableName, prevLoc.getRegionInfo()
              .getStartKey(), prevLoc.getServerAddress());
        }

        try {
          // do not retry if getting the location throws exception
          callable.instantiateRegionLocation(false /* reload cache ? */);
        } catch (IOException e) {
          exceptions.add(e);
          throw new RetriesExhaustedException(callable.getServerName(),
              callable.getRegionName(), callable.getRow(), tries, exceptions);
        }
        if (prevLoc.getServerAddress().equals(
            callable.location.getServerAddress())) {
          // Bail out of the retry loop if we have to wait too long
          long pauseTime = params.getPauseTime(tries);
          if ((System.currentTimeMillis() - callStartTime + pauseTime) > params
              .getRpcRetryTimeout()) {
            throw new RetriesExhaustedException(callable.getServerName(),
                callable.getRegionName(), callable.getRow(), tries,
                exceptions);
          }
          LOG.debug("getRegionServerWithRetries failed, sleeping for "
              + pauseTime + "ms. tries = " + tries, t);
          try {
            Thread.sleep(pauseTime);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
          }
          // do not reload cache. While we were sleeping hopefully the cache
          // has been re-populated.
          callable.instantiateRegionLocation(false);
        } else {
          LOG.debug("getRegionServerWithRetries failed, "
              + "region moved from " + prevLoc + " to " + callable.location
              + "retrying immediately tries=" + tries, t);
        }
      }
    }
  }

  /**
   * Pass in a ServerCallable with your particular bit of logic defined and
   * this method will pass it to the defined region server.
   *
   * @param <T>
   *          the type of the return value
   * @param callable
   *          callable to run
   * @return an object of type T
   * @throws IOException
   *           if a remote or network exception occurs
   * @throws RuntimeException
   *           other unspecified error
   * @throws PreemptiveFastFailException
   *           if the remote host has been known to be unreachable for more
   *           than this.fastFailThresholdMilliSec.
   */
  @Override
  public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable,
      boolean instantiateRegionLocation) throws IOException,
      RuntimeException, PreemptiveFastFailException {
    FailureInfo fInfo = null;
    HServerAddress server = null;
    boolean didTry = false;
    MutableBoolean couldNotCommunicateWithServer = new MutableBoolean(false);
    boolean retryDespiteFastFailMode = false;
    try {
      if (instantiateRegionLocation) {
        callable.instantiateRegionLocation(false);
      }
      // Logic to fast fail requests to unreachable servers.
      server = callable.getServerAddress();
      fInfo = repeatedFailuresMap.get(server);

      if (inFastFailMode(server) && !currentThreadInFastFailMode()) {
        // In Fast-fail mode, all but one thread will fast fail. Check
        // if we are that one chosen thread.
        retryDespiteFastFailMode = shouldRetryInspiteOfFastFail(fInfo);
        if (retryDespiteFastFailMode == false) { // we don't have to retry
          throw new PreemptiveFastFailException(
              fInfo.numConsecutiveFailures.get(),
              fInfo.timeOfFirstFailureMilliSec,
              fInfo.timeOfLatestAttemptMilliSec, server.getHostname());
        }
      }
      didTry = true;
      callable.instantiateServer();
      return callable.call();
    } catch (PreemptiveFastFailException pfe) {
      throw pfe;
    } catch (ClientSideDoNotRetryException exp) {
      throw exp;
    } catch (ThriftHBaseException e) {
      handleThrowable(e.getServerJavaException(), callable,
          couldNotCommunicateWithServer);
      return null;
    } catch (Throwable t1) {
      handleThrowable(t1, callable, couldNotCommunicateWithServer);
      return null;
    } finally {
      updateFailureInfoForServer(server, fInfo, didTry,
          couldNotCommunicateWithServer.booleanValue(),
          retryDespiteFastFailMode);
    }
  }

  @Override
  public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
      throws IOException, RuntimeException, PreemptiveFastFailException {
    return this.getRegionServerWithoutRetries(callable, true);
  }

  private <T> void updateClientContext(final ServerCallable<T> callable,
      final Throwable t) {
    if (!recordClientContext) {
      return;
    }

    List<OperationContext> currContext = this.operationContextPerThread.get();
    if (currContext == null) {
      currContext = new ArrayList<OperationContext>();
      this.operationContextPerThread.set(currContext);
    }

    currContext.add(new OperationContext(callable.location, t));
  }

  /**
   * Handles failures encountered when communicating with a server.
   *
   * Updates the FailureInfo in repeatedFailuresMap to reflect the failure.
   * Throws RepeatedConnectException if the client is in Fast fail mode.
   *
   * @param server
   * @param t
   *          - the throwable to be handled.
   * @throws PreemptiveFastFailException
   */
  private void handleFailureToServer(HServerAddress server, Throwable t) {
    if (server == null || t == null)
      return;

    long currentTime = System.currentTimeMillis();
    FailureInfo fInfo = repeatedFailuresMap.get(server);
    if (fInfo == null) {
      fInfo = new FailureInfo(currentTime);
      FailureInfo oldfInfo = repeatedFailuresMap.putIfAbsent(server, fInfo);

      if (oldfInfo != null) {
        fInfo = oldfInfo;
      }
    }
    fInfo.timeOfLatestAttemptMilliSec = currentTime;
    fInfo.numConsecutiveFailures.incrementAndGet();

    if (inFastFailMode(server)) {
      // In FastFail mode, do not clear out the cache if it was done recently.
      if (currentTime > fInfo.timeOfLatestCacheClearMilliSec
          + cacheClearingTimeoutMilliSec) {
        fInfo.timeOfLatestCacheClearMilliSec = currentTime;
        metaCache.clearForServer(server.toString());
      }
      LOG.error("Exception in FastFail mode : " + t.toString());
      return;
    }

    // if thrown these exceptions, we clear all the cache entries that
    // map to that slow/dead server; otherwise, let cache miss and ask
    // .META. again to find the new location
    fInfo.timeOfLatestCacheClearMilliSec = currentTime;
    metaCache.clearForServer(server.toString());
  }

  /**
   * Occasionally cleans up unused information in repeatedFailuresMap.
   *
   * repeatedFailuresMap stores the failure information for all remote hosts
   * that had failures. In order to avoid these from growing indefinitely,
   * occassionallyCleanupFailureInformation() will clear these up once every
   * cleanupInterval ms.
   */
  private void occasionallyCleanupFailureInformation() {
    long now = System.currentTimeMillis();
    if (!(now > lastFailureMapCleanupTimeMilliSec
        + failureMapCleanupIntervalMilliSec))
      return;

    // remove entries that haven't been attempted in a while
    // No synchronization needed. It is okay if multiple threads try to
    // remove the entry again and again from a concurrent hash map.
    StringBuilder sb = new StringBuilder();
    for (Entry<HServerAddress, FailureInfo> entry : repeatedFailuresMap
        .entrySet()) {
      if (now > entry.getValue().timeOfLatestAttemptMilliSec
          + failureMapCleanupIntervalMilliSec) { // no recent failures
        repeatedFailuresMap.remove(entry.getKey());
      } else if (now > entry.getValue().timeOfFirstFailureMilliSec
          + this.fastFailClearingTimeMilliSec) { // been failing for a long
                                                 // time
        LOG.error(entry.getKey()
            + " been failing for a long time. clearing out."
            + entry.getValue().toString());
        repeatedFailuresMap.remove(entry.getKey());
      } else {
        sb.append(entry.getKey().toString() + " failing "
            + entry.getValue().toString() + "\n");
      }
    }
    if (sb.length() > 0
    // If there are multiple threads cleaning up, try to see that only one
    // will log the msg.
        && now > this.lastFailureMapCleanupTimeMilliSec
            + this.failureMapCleanupIntervalMilliSec) {
      LOG.warn("Preemptive failure enabled for : " + sb.toString());
    }
    lastFailureMapCleanupTimeMilliSec = now;
  }

  /**
   * Checks to see if we are in the Fast fail mode for requests to the server.
   *
   * If a client is unable to contact a server for more than
   * fastFailThresholdMilliSec the client will get into fast fail mode.
   *
   * @param server
   * @return true if the client is in fast fail mode for the server.
   */
  private boolean inFastFailMode(HServerAddress server) {
    FailureInfo fInfo = repeatedFailuresMap.get(server);
    // if fInfo is null --> The server is considered good.
    // If the server is bad, wait long enough to believe that the server is
    // down.
    return (fInfo != null && System.currentTimeMillis() > fInfo.timeOfFirstFailureMilliSec
        + this.fastFailThresholdMilliSec);
  }

  /**
   * Checks to see if the current thread is already in FastFail mode for
   * *some* server.
   *
   * @return true, if the thread is already in FF mode.
   */
  private boolean currentThreadInFastFailMode() {
    return (this.threadRetryingInFastFailMode.get() != null && this.threadRetryingInFastFailMode
        .get().booleanValue() == true);
  }

  /**
   * Check to see if the client should try to connnect to the server, inspite
   * of knowing that it is in the fast fail mode.
   *
   * The idea here is that we want just one client thread to be actively
   * trying to reconnect, while all the other threads trying to reach the
   * server will short circuit.
   *
   * @param fInfo
   * @return true if the client should try to connect to the server.
   */
  private boolean shouldRetryInspiteOfFastFail(FailureInfo fInfo) {
    // We believe that the server is down, But, we want to have just one
    // client
    // actively trying to connect. If we are the chosen one, we will retry
    // and not throw an exception.
    if (fInfo != null
        && fInfo.exclusivelyRetringInspiteOfFastFail.compareAndSet(false,
            true)) {
      MutableBoolean threadAlreadyInFF = this.threadRetryingInFastFailMode
          .get();
      if (threadAlreadyInFF == null) {
        threadAlreadyInFF = new MutableBoolean();
        this.threadRetryingInFastFailMode.set(threadAlreadyInFF);
      }
      threadAlreadyInFF.setValue(true);

      return true;
    } else {
      return false;
    }
  }

  /**
   * updates the failure information for the server.
   *
   * @param server
   * @param fInfo
   * @param couldNotCommunicate
   * @param retryDespiteFastFailMode
   */
  private void updateFailureInfoForServer(HServerAddress server,
      FailureInfo fInfo, boolean didTry, boolean couldNotCommunicate,
      boolean retryDespiteFastFailMode) {
    if (server == null || fInfo == null || didTry == false)
      return;

    // If we were able to connect to the server, reset the failure
    // information.
    if (couldNotCommunicate == false) {
      LOG.info("Clearing out PFFE for server " + server.getHostname());
      repeatedFailuresMap.remove(server);
    } else {
      // update time of last attempt
      long currentTime = System.currentTimeMillis();
      fInfo.timeOfLatestAttemptMilliSec = currentTime;

      // Release the lock if we were retrying inspite of FastFail
      if (retryDespiteFastFailMode) {
        fInfo.exclusivelyRetringInspiteOfFastFail.set(false);
        threadRetryingInFastFailMode.get().setValue(false);
      }
    }

    occasionallyCleanupFailureInformation();
  }

  public void updateFailureInfoForServer(HServerAddress server,
      boolean didTry, boolean couldNotCommunicate) {
    FailureInfo fInfo = repeatedFailuresMap.get(server);
    boolean retryDespiteFastFailMode = false;
    if (inFastFailMode(server) && !currentThreadInFastFailMode()) {
      // In Fast-fail mode, all but one thread will fast fail. Check
      // if we are that one chosen thread.
      retryDespiteFastFailMode = shouldRetryInspiteOfFastFail(fInfo);
    }

    updateFailureInfoForServer(server, fInfo, didTry, couldNotCommunicate, retryDespiteFastFailMode);
  }

  public void handleThrowable(Throwable t1, ServerCallable<?> callable,
                              MutableBoolean couldNotCommunicateWithServer)
      throws IOException {
    Throwable t2 = translateException(t1);
    boolean isLocalException = !(t2 instanceof RemoteException);
    // translateException throws DoNotRetryException or any
    // non-IOException.
    if (isLocalException && isNetworkException(t2)) {
      couldNotCommunicateWithServer.setValue(true);
      handleFailureToServer(callable.getServerAddress(), t2);
    }

    updateClientContext(callable, t2);
    if (t2 instanceof IOException) {
      throw (IOException) t2;
    } else {
      throw new RuntimeException(t2);
    }
  }

  private Callable<Long> createGetServerStartCodeCallable(
      final HServerAddress address, final HBaseRPCOptions options) {
    final HConnection connection = this;
    return new Callable<Long>() {
      @Override
      public Long call() throws IOException {
        return getRegionServerWithoutRetries(new ServerCallableForBatchOps<Long>(
            connection, address, options) {
          @Override
          public Long call() throws IOException {
            return server.getStartCode();
          }
        });
      }
    };
  }

  private Callable<Long> createCurrentTimeCallable(
      final HServerAddress address, final HBaseRPCOptions options) {
    final HConnection connection = this;
    return new Callable<Long>() {
      @Override
      public Long call() throws IOException {
        return getRegionServerWithoutRetries(new ServerCallableForBatchOps<Long>(
            connection, address, options) {
          @Override
          public Long call() throws IOException {
            return server.getCurrentTimeMillis();
          }
        });
      }
    };
  }

  private Callable<MapWritable> createGetLastFlushTimesCallable(
      final HServerAddress address, final HBaseRPCOptions options) {
    final HConnection connection = this;
    return new Callable<MapWritable>() {
      @Override
      public MapWritable call() throws IOException {
        return getRegionServerWithoutRetries(new ServerCallableForBatchOps<MapWritable>(
            connection, address, options) {
          @Override
          public MapWritable call() throws IOException {
            return server.getLastFlushTimes();
          }
        });
      }
    };
  }

  private Callable<Void> createFlushCallable(final HServerAddress address,
      final HRegionInfo region, final long targetFlushTime,
      final HBaseRPCOptions options) {
    final HConnection connection = this;
    return new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        return getRegionServerWithoutRetries(new ServerCallableForBatchOps<Void>(
            connection, address, options) {
          @Override
          public Void call() throws IOException {
            server.flushRegion(region.getRegionName(), targetFlushTime);
            return null;
          }
        });
      }
    };
  }

  private Callable<MultiResponse> createMultiActionCallable(
      final HServerAddress address, final MultiAction multi,
      final byte[] tableName, final HBaseRPCOptions options) {
    final HConnection connection = this;
    // no need to track mutations here. Done at the caller.
    return new Callable<MultiResponse>() {
      @Override
      public MultiResponse call() throws IOException {
        return getRegionServerWithoutRetries(
            new ServerCallableForBatchOps<MultiResponse>(connection, address,
                options) {
              @Override
              public MultiResponse call() throws IOException,
                  InterruptedException, ExecutionException {
                return server.multiAction(multi);
              }
            }, true);
      }
    };
  }

  private HRegionLocation getRegionLocationForRowWithRetries(
      StringBytes tableName, byte[] rowKey, boolean reload) throws IOException {
    boolean reloadFlag = reload;
    List<Throwable> exceptions = new ArrayList<Throwable>();
    HRegionLocation location = null;
    int tries = 0;
    for (; tries < params.getNumRetries();) {
      try {
        location = getRegionLocation(tableName, rowKey, reloadFlag);
      } catch (Throwable t) {
        exceptions.add(t);
      }
      if (location != null) {
        break;
      }
      reloadFlag = true;
      tries++;
      try {
        Thread.sleep(params.getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }
    if (location == null) {
      throw new RetriesExhaustedException(
          " -- nothing found, no 'location' returned," + " tableName="
              + tableName + ", reload=" + reload + " --",
          HConstants.EMPTY_BYTE_ARRAY, rowKey, tries, exceptions);
    }
    return location;
  }

  private <R extends Row> Map<HServerAddress, MultiAction> splitRequestsByRegionServer(
      List<R> workingList, StringBytes tableName, boolean isGets)
      throws IOException {
    Map<HServerAddress, MultiAction> actionsByServer = new HashMap<HServerAddress, MultiAction>();
    for (int i = 0; i < workingList.size(); i++) {
      Row row = workingList.get(i);
      if (row != null) {
        HRegionLocation loc = locateRegion(tableName, row.getRow(), true);

        byte[] regionName = loc.getRegionInfo().getRegionName();

        MultiAction actions = actionsByServer.get(loc.getServerAddress());
        if (actions == null) {
          actions = new MultiAction();
          actionsByServer.put(loc.getServerAddress(), actions);
        }

        if (isGets) {
          actions.addGet(regionName, (Get) row, i);
        } else {
          trackMutationsToTable(tableName, loc);
          actions.mutate(regionName, (Mutation) row);
        }
      }
    }
    return actionsByServer;
  }

  private Map<HServerAddress, ListenableFuture<MultiResponse>> makeServerRequests(
      Map<HServerAddress, MultiAction> actionsByServer,
      final byte[] tableName, ListeningExecutorService pool, HBaseRPCOptions options) {

    Map<HServerAddress, ListenableFuture<MultiResponse>> futures = new HashMap<>(
        actionsByServer.size());

    boolean singleServer = (actionsByServer.size() == 1);
    for (Entry<HServerAddress, MultiAction> e : actionsByServer.entrySet()) {
      Callable<MultiResponse> callable = createMultiActionCallable(
          e.getKey(), e.getValue(), tableName, options);
      ListenableFuture<MultiResponse> task;

      try {
       validateAndIncrementNumOutstandingPerServer(e.getKey());
        if (singleServer) {
          task =  ListenableFutureTask.create(callable);
          ((FutureTask<MultiResponse>) task).run();
        } else {
          task = pool.submit(callable);
        }
        Futures.addCallback(task, new OutstandingRequestCallback(e.getKey()));
      } catch (IOException e1) {
        task = Futures.immediateFailedFuture(e1);
      }
      futures.put(e.getKey(), task);
    }
    return futures;
  }

  /*
   * Collects responses from each of the RegionServers. If there are failures,
   * a list of failed operations is returned.
   */
  private List<Mutation> collectResponsesForMutateFromAllRS(
      StringBytes tableName,
      Map<HServerAddress, MultiAction> actionsByServer,
      Map<HServerAddress, ListenableFuture<MultiResponse>> futures,
      Map<String, HRegionFailureInfo> failureInfo)
      throws InterruptedException, IOException {

    List<Mutation> newWorkingList = null;
    for (Entry<HServerAddress, ListenableFuture<MultiResponse>> responsePerServer : futures
        .entrySet()) {
      HServerAddress address = responsePerServer.getKey();
      MultiAction request = actionsByServer.get(address);

      Future<MultiResponse> future = responsePerServer.getValue();
      MultiResponse resp = null;

      try {
        resp = future.get();
      } catch (InterruptedException ie) {
        throw ie;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof DoNotRetryIOException)
          throw (DoNotRetryIOException) e.getCause();
      }

      // If we got a response. Let us go through the responses from each
      // region and
      // process the Puts and Deletes.
      // If the response is null, we will add it to newWorkingList here.
      if (request.getDeletes() != null) {
        newWorkingList = processMutationResponseFromOneRegionServer(
            tableName, address, resp, request.getDeletes(), newWorkingList,
            true, failureInfo);
      }
      if (request.getPuts() != null) {
        newWorkingList = processMutationResponseFromOneRegionServer(
            tableName, address, resp, request.getPuts(), newWorkingList,
            false, failureInfo);
      }
    }
    return newWorkingList;
  }

  /*
   * Collects responses from each of the RegionServers and populates the
   * result array. If there are failures, a list of failed operations is
   * returned.
   */
  private List<Get> collectResponsesForGetFromAllRS(StringBytes tableName,
      Map<HServerAddress, MultiAction> actionsByServer,
      Map<HServerAddress, ListenableFuture<MultiResponse>> futures,
      List<Get> orig_list, Result[] results,
      Map<String, HRegionFailureInfo> failureInfo) throws IOException,
      InterruptedException {

    List<Get> newWorkingList = null;
    for (Entry<HServerAddress, ListenableFuture<MultiResponse>> responsePerServer : futures
        .entrySet()) {
      HServerAddress address = responsePerServer.getKey();
      MultiAction request = actionsByServer.get(address);

      Future<MultiResponse> future = responsePerServer.getValue();
      MultiResponse resp = null;

      try {
        resp = future.get();
      } catch (InterruptedException ie) {
        throw ie;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof DoNotRetryIOException) {
          throw (DoNotRetryIOException) e.getCause();
        }
        e.printStackTrace();
      }

      if (resp == null) {
        // Entire server failed
        LOG.info("Failed all for server: " + address
            + ", removing from cache");
      }

      newWorkingList = processGetResponseFromOneRegionServer(tableName,
          address, request, resp, orig_list, newWorkingList, results,
          failureInfo);
    }
    return newWorkingList;
  }

  private <R extends Mutation> List<Mutation> processMutationResponseFromOneRegionServer(
      StringBytes tableName, HServerAddress address, MultiResponse resp,
      Map<byte[], List<R>> map, List<Mutation> newWorkingList,
      boolean isDelete, Map<String, HRegionFailureInfo> failureInfo)
      throws IOException {
    // If we got a response. Let us go through the responses from each region
    // and
    // process the Puts and Deletes.
    for (Map.Entry<byte[], List<R>> e : map.entrySet()) {
      byte[] regionName = e.getKey();
      List<R> regionOps = map.get(regionName);

      long result = 0;
      try {
        if (isDelete)
          result = resp.getDeleteResult(regionName);
        else
          result = resp.getPutResult(regionName);

        if (result != -1) {
          if (newWorkingList == null)
            newWorkingList = new ArrayList<Mutation>();

          newWorkingList.addAll(regionOps.subList((int) result,
              regionOps.size()));
        }
      } catch (Exception ex) {
        String serverName = address.getHostname();
        String regName = Bytes.toStringBinary(regionName);
        // If response is null, we will catch a NPE here.
        translateException(ex);

        if (!failureInfo.containsKey(regName)) {
          failureInfo.put(regName, new HRegionFailureInfo(regName));
        }
        failureInfo.get(regName).addException(ex);
        failureInfo.get(regName).setServerName(serverName);

        if (newWorkingList == null)
          newWorkingList = new ArrayList<Mutation>();

        newWorkingList.addAll(regionOps);
        // enough to remove from cache one of the rows from the region
        deleteCachedLocation(tableName, regionOps.get(0).getRow(), address);
      }
    }

    return newWorkingList;
  }

  private List<Get> processGetResponseFromOneRegionServer(
      StringBytes tableName,
      HServerAddress address, MultiAction request, MultiResponse resp,
      List<Get> orig_list, List<Get> newWorkingList, Result[] results,
      Map<String, HRegionFailureInfo> failureInfo) throws IOException {

    for (Map.Entry<byte[], List<Get>> e : request.getGets().entrySet()) {
      byte[] regionName = e.getKey();
      List<Get> regionGets = request.getGets().get(regionName);
      List<Integer> origIndices = request.getOriginalIndex().get(regionName);

      Result[] getResult;
      try {
        getResult = resp.getGetResult(regionName);

        // fill up the result[] array accordingly
        assert (origIndices.size() == getResult.length);
        for (int i = 0; i < getResult.length; i++) {
          results[origIndices.get(i)] = getResult[i];
        }
      } catch (Exception ex) {
        // If response is null, we will catch a NPE here.
        try {
          translateException(ex);
        } catch (NullPointerException nullPtrex) {
          // This case we don't need to panic and throw an exception. Instead,
          // we should populate the failed requests and return to the caller.
        }

        if (newWorkingList == null)
          newWorkingList = new ArrayList<Get>(orig_list.size());

        String serverName = address.getHostname();
        String regName = Bytes.toStringBinary(regionName);

        if (!failureInfo.containsKey(regName)) {
          failureInfo.put(regName, new HRegionFailureInfo(regName));
        }
        failureInfo.get(regName).addException(ex);
        failureInfo.get(regName).setServerName(serverName);

        // Add the element to the correct position
        for (int i = 0; i < regionGets.size(); i++) {
          newWorkingList.add(origIndices.get(i), regionGets.get(i));
        }

        // enough to clear this once for a region
        deleteCachedLocation(tableName, regionGets.get(0).getRow(), address);
      }
    }

    return newWorkingList;
  }

  @Override
  public void processBatchedMutations(List<Mutation> orig_list,
      StringBytes tableName, ListeningExecutorService pool, List<Mutation> failures,
      HBaseRPCOptions options) throws IOException, InterruptedException {

    // Keep track of the most recent servers for any given item for better
    // exception reporting. We keep HRegionLocation to save on parsing.
    // Later below when we use lastServers, we'll pull what we need from
    // lastServers.
    // Sort the puts based on the row key in order to optimize the row lock
    // acquiring
    // in the server side.

    Map<String, HRegionFailureInfo> failureInfo = new HashMap<String, HRegionFailureInfo>();
    List<Mutation> workingList = orig_list;
    Collections.sort(workingList);

    for (int tries = 0; workingList != null && !workingList.isEmpty()
        && tries < params.getNumRetries(); ++tries) {

      if (tries >= 1) {
        long sleepTime = params.getPauseTime(tries);
        LOG.debug("Retry " + tries + ", sleep for " + sleepTime + "ms!");
        Thread.sleep(sleepTime);
      }

      // step 1: break up into regionserver-sized chunks and build the data
      // structs
      Map<HServerAddress, MultiAction> actionsByServer = splitRequestsByRegionServer(
          workingList, tableName, false);

      // step 2: make the requests
      Map<HServerAddress, ListenableFuture<MultiResponse>> futures = makeServerRequests(
          actionsByServer, tableName.getBytes(), pool, options);

      // step 3: collect the failures and successes and prepare for retry
      workingList = collectResponsesForMutateFromAllRS(tableName,
          actionsByServer, futures, failureInfo);
    }

    if (workingList != null && !workingList.isEmpty()) {
      if (failures != null)
        failures.addAll(workingList);
      throw new RetriesExhaustedException(failureInfo, workingList.size()
          + "mutate operations remaining after " + params.getNumRetries()
          + " retries");
    }
  }

  @Override
  public void processBatchedGets(List<Get> orig_list, StringBytes tableName,
      ListeningExecutorService pool, Result[] results, HBaseRPCOptions options)
      throws IOException, InterruptedException {

    Map<String, HRegionFailureInfo> failureInfo = new HashMap<String, HRegionFailureInfo>();
    // if results is not NULL
    // results must be the same size as list
    if (results != null && (results.length != orig_list.size())) {
      throw new IllegalArgumentException(
          "argument results must be the same size as argument list");
    }

    // Keep track of the most recent servers for any given item for better
    // exception reporting. We keep HRegionLocation to save on parsing.
    // Later below when we use lastServers, we'll pull what we need from
    // lastServers.
    List<Get> workingList = orig_list;

    for (int tries = 0; workingList != null && !workingList.isEmpty()
        && tries < params.getNumRetries(); ++tries) {

      if (tries >= 1) {
        long sleepTime = params.getPauseTime(tries);
        LOG.debug("Retry " + tries + ", sleep for " + sleepTime + "ms!");
        Thread.sleep(sleepTime);
      }

      // step 1: break up into regionserver-sized chunks and build the data
      // structs
      Map<HServerAddress, MultiAction> actionsByServer = splitRequestsByRegionServer(
          workingList, tableName, true);

      // step 2: make the requests
      Map<HServerAddress, ListenableFuture<MultiResponse>> futures = makeServerRequests(
          actionsByServer, tableName.getBytes(), pool, options);

      // step 3: collect the failures and successes and prepare for retry
      workingList = collectResponsesForGetFromAllRS(tableName,
          actionsByServer, futures, workingList, results, failureInfo);
    }

    if (workingList != null && !workingList.isEmpty()) {
      throw new RetriesExhaustedException(failureInfo, workingList.size()
          + " get operations remaining after " + params.getNumRetries()
          + " retries");
    }
  }

  /*
   * Helper class for batch updates. Holds code shared doing batch puts and
   * batch deletes.
   */
  private abstract class Batch<T> {
    final HConnection c;

    private Batch(final HConnection c) {
      this.c = c;
    }

    /**
     * This is the method subclasses must implement.
     *
     * @param currentList
     *          current list of rows
     * @param tableName
     *          table we are processing
     * @param row
     *          row
     * @return Count of items processed or -1 if all.
     * @throws IOException
     *           if a remote or network exception occurs
     * @throws RuntimeException
     *           other undefined exception
     */
    abstract int doCall(final List<? extends Row> currentList,
        final byte[] row, StringBytes tableName, T ret) throws IOException,
        RuntimeException;

    /**
     * Process the passed <code>list</code>.
     *
     * @param list
     *          list of rows to process
     * @param tableName
     *          table we are processing
     * @return Count of how many added or -1 if all added.
     * @throws IOException
     *           if a remote or network exception occurs
     */
    int process(final List<? extends Row> list, StringBytes tableName, T ret)
        throws IOException {
      boolean isLastRow;
      boolean retryOnlyOne = false;
      List<Row> currentList = new ArrayList<Row>();
      int i, tries;
      if (list.size() > 1) {
        Collections.sort(list);
      }
      byte[] region = getRegionName(tableName, list.get(0).getRow(), false);
      byte[] currentRegion = region;

      for (i = 0, tries = 0; i < list.size()
          && tries < params.getNumRetries(); i++) {
        Row row = list.get(i);
        currentList.add(row);
        // If the next record goes to a new region, then we are to clear
        // currentList now during this cycle.
        isLastRow = (i + 1) == list.size();
        if (!isLastRow) {
          region = getRegionName(tableName, list.get(i + 1).getRow(), false);
        }
        if (!Bytes.equals(currentRegion, region) || isLastRow || retryOnlyOne) {
          int index = doCall(currentList, row.getRow(), tableName, ret);
          // index is == -1 if all processed successfully, else its index
          // of last record successfully processed.
          if (index != -1) {
            if (tries == params.getNumRetries() - 1) {
              throw new RetriesExhaustedException(
                  "Some server, retryOnlyOne=" + retryOnlyOne + ", index="
                      + index + ", islastrow=" + isLastRow + ", tries="
                      + tries + ", numtries=" + params.getNumRetries()
                      + ", i=" + i + ", listsize=" + list.size()
                      + ", region=" + Bytes.toStringBinary(region),
                  currentRegion, row.getRow(), tries,
                  new ArrayList<Throwable>());
            }
            tries = doBatchPause(currentRegion, tries);
            i = i - currentList.size() + index;
            retryOnlyOne = true;
            // Reload location.
            region = getRegionName(tableName, list.get(i + 1).getRow(), true);
          } else {
            // Reset these flags/counters on successful batch Put
            retryOnlyOne = false;
            tries = 0;
          }
          currentRegion = region;
          currentList.clear();
        }
      }
      return i;
    }

    /*
     * @param t
     *
     * @param r
     *
     * @param re
     *
     * @return Region name that holds passed row <code>r</code>
     *
     * @throws IOException
     */
    private byte[] getRegionName(StringBytes t, final byte[] r,
        final boolean re) throws IOException {
      HRegionLocation location = getRegionLocationForRowWithRetries(t, r, re);
      return location.getRegionInfo().getRegionName();
    }

    /*
     * Do pause processing before retrying...
     *
     * @param currentRegion
     *
     * @param tries
     *
     * @return New value for tries.
     */
    private int doBatchPause(final byte[] currentRegion, final int tries) {
      int localTries = tries;
      long sleepTime = params.getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reloading region " + Bytes.toStringBinary(currentRegion)
            + " location because regionserver didn't accept updates; tries="
            + tries + " of max=" + params.getNumRetries() + ", waiting="
            + sleepTime + "ms");
      }
      try {
        Thread.sleep(sleepTime);
        localTries++;
      } catch (InterruptedException e) {
        // continue
      }
      return localTries;
    }
  }

  @Override
  public int processBatchOfRows(final ArrayList<Put> list,
      StringBytes tableName, final HBaseRPCOptions options) throws IOException {
    if (list.isEmpty())
      return 0;
    Batch<Object> b = new Batch<Object>(this) {
      @SuppressWarnings("unchecked")
      @Override
      int doCall(final List<? extends Row> currentList, final byte[] row,
          StringBytes tableName, Object whatevs) throws IOException,
          RuntimeException {
        final List<Put> puts = (List<Put>) currentList;
        return getRegionServerWithRetries(new ServerCallable<Integer>(this.c,
            tableName, row, options) {
          @Override
          public Integer call() throws IOException {
            trackMutationsToTable(tableName, location);
            return server.put(location.getRegionInfo().getRegionName(), puts);
          }
        });
      }
    };
    return b.process(list, tableName, new Object());
  }

  @Override
  public Result[] processBatchOfGets(final List<Get> list,
      StringBytes tableName, final HBaseRPCOptions options)
      throws IOException {
    if (list.isEmpty()) {
      return null;
    }

    final List<Get> origList = new ArrayList<Get>(list);
    Batch<Result[]> b = new Batch<Result[]>(this) {
      @SuppressWarnings("unchecked")
      @Override
      int doCall(final List<? extends Row> currentList, final byte[] row,
          StringBytes tableName, Result[] res) throws IOException,
          RuntimeException {
        final List<Get> gets = (List<Get>) currentList;
        Result[] tmp = getRegionServerWithRetries(new ServerCallable<Result[]>(
            this.c, tableName, row, options) {
          @Override
          public Result[] call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), gets);
          }
        });
        for (int i = 0; i < tmp.length; i++) {
          res[origList.indexOf(gets.get(i))] = tmp[i];
        }
        return tmp.length == currentList.size() ? -1 : tmp.length;
      }
    };
    Result[] results = new Result[list.size()];
    b.process(list, tableName, results);
    return results;
  }

  @Override
  public int processBatchOfRowMutations(final List<RowMutations> list,
      StringBytes tableName, final HBaseRPCOptions options) throws IOException {
    if (list.isEmpty()) {
      return 0;
    }
    Batch<Object> b = new Batch<Object>(this) {
      @SuppressWarnings("unchecked")
      @Override
      int doCall(final List<? extends Row> currentList, final byte[] row,
          StringBytes tableName, Object whatevs) throws IOException,
          RuntimeException {
        final List<RowMutations> mutations = (List<RowMutations>) currentList;
        getRegionServerWithRetries(new ServerCallable<Void>(this.c,
            tableName, row, options) {
          @Override
          public Void call() throws IOException {
            trackMutationsToTable(tableName, location);
            server.mutateRow(location.getRegionInfo().getRegionName(),
                mutations);
            return null;
          }
        });
        return -1;
      }
    };
    return b.process(list, tableName, new Object());
  }

  @Override
  public int processBatchOfDeletes(final List<Delete> list,
      StringBytes tableName, final HBaseRPCOptions options) throws IOException {
    if (list.isEmpty()) {
      return 0;
    }
    Batch<Object> b = new Batch<Object>(this) {
      @SuppressWarnings("unchecked")
      @Override
      int doCall(final List<? extends Row> currentList, final byte[] row,
          StringBytes tableName, Object whatevs) throws IOException,
          RuntimeException {
        final List<Delete> deletes = (List<Delete>) currentList;
        return getRegionServerWithRetries(new ServerCallable<Integer>(this.c,
            tableName, row, options) {
          @Override
          public Integer call() throws IOException, InterruptedException,
              ExecutionException {
            trackMutationsToTable(tableName, location);
            return server.delete(location.getRegionInfo().getRegionName(),
                deletes);
          }
        });
      }
    };
    return b.process(list, tableName, new Object());
  }

  @Override
  public void close() {
    if (master != null) {
      master = null;
      masterChecked = false;
    }
    this.clearRegionCache();
    this.rootRegionLocation = null;
  }

  /**
   * Try to make the put path handle errors similar to
   * getRegionServerWithRetries Process a list of puts from a shared HTable
   * thread pool.
   *
   * @param list
   *          The input put request list
   * @param failed
   *          The failed put request list
   * @param tableName
   *          The table name for the put request
   * @throws IOException
   */
  private List<MultiPut> splitPutsIntoMultiPuts(List<Put> list,
      StringBytes tableName, HBaseRPCOptions options) throws IOException {
    Map<HServerAddress, MultiPut> regionPuts = new HashMap<HServerAddress, MultiPut>();

    for (Put put : list) {
      byte[] row = put.getRow();

      HRegionLocation loc = locateRegion(tableName, row, true);
      HServerAddress address = loc.getServerAddress();
      byte[] regionName = loc.getRegionInfo().getRegionName();

      MultiPut mput = regionPuts.get(address);
      if (mput == null) {
        mput = new MultiPut(address);
        regionPuts.put(address, mput);
      }
      mput.add(regionName, put);
      trackMutationsToTable(tableName, loc);
    }

    return new ArrayList<MultiPut>(regionPuts.values());
  }

  @Override
  public List<Put> processListOfMultiPut(List<MultiPut> multiPuts,
      StringBytes givenTableName, HBaseRPCOptions options,
      Map<String, HRegionFailureInfo> failedRegionsInfo) throws IOException {
    List<Put> failed = null;

    List<ListenableFuture<MultiPutResponse>> futures = new ArrayList<>(
        multiPuts.size());
    boolean singleServer = (multiPuts.size() == 1);
    for (MultiPut put : multiPuts) {
      Callable<MultiPutResponse> callable = createPutCallable(put.address,
          put, options);
      ListenableFuture<MultiPutResponse> task;
      try {
        validateAndIncrementNumOutstandingPerServer(put.address);
        if (singleServer) {
          ListenableFutureTask<MultiPutResponse> futureTask = ListenableFutureTask.create(
              callable);
          task = futureTask;
          futureTask.run();
        } else {
          task = HTable.listeningMultiActionPool.submit(callable);
        }
        Futures.addCallback(task, new OutstandingRequestCallback(put.address));
      } catch (IOException e1) {
        task = Futures.immediateFailedFuture(e1);
      }

      futures.add(task);
    }

    RegionOverloadedException toThrow = null;
    long maxWaitTimeRequested = 0;

    for (int i = 0; i < futures.size(); i++) {
      Future<MultiPutResponse> future = futures.get(i);
      MultiPut request = multiPuts.get(i);
      String serverName = request.address.getHostname();
      String regionName = null;
      HRegionFailureInfo regionFailure = null;

      MultiPutResponse resp = null;
      try {
        resp = future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException(e.getMessage());
      } catch (ExecutionException ex) {
        Throwable t = ex.getCause();

        // retry, unless it is not to be retried.
        if (t instanceof DoNotRetryIOException) {
          throw (DoNotRetryIOException)t;
        } else if (t instanceof RegionOverloadedException) {
          RegionOverloadedException roe = (RegionOverloadedException)t;
          if (roe.getBackoffTimeMillis() > maxWaitTimeRequested) {
            maxWaitTimeRequested = roe.getBackoffTimeMillis();
            toThrow = roe;
          }
        }

        if (singleServer && (t instanceof PreemptiveFastFailException)) {
          throw (PreemptiveFastFailException)t;
        }

        if (t instanceof ClientSideDoNotRetryException) {
          throw (ClientSideDoNotRetryException)t;
        }

        // Throw Error directly.
        if (t instanceof Error) {
          LOG.error(t);
          throw (Error)t;
        }

        // Don't retry on non-IOException
        if (t instanceof RuntimeException) {
          LOG.error(t);
          throw (RuntimeException)t;
        } else if (! (t instanceof IOException)) {
          LOG.error(t);
          throw new RuntimeException(t);
        }

        // Add entry for the all the regions involved in this operation.
        for (Map.Entry<byte[], List<Put>> e : request.puts.entrySet()) {
          regionName = Bytes.toStringBinary(e.getKey());
          if (!failedRegionsInfo.containsKey(regionName)) {
            regionFailure = new HRegionFailureInfo(regionName);
            failedRegionsInfo.put(regionName, regionFailure);
          } else {
            regionFailure = failedRegionsInfo.get(regionName);
          }
          regionFailure.setServerName(serverName);
          regionFailure.addException(t);
        }
        LOG.warn("Current exception is not known as fatal, ignoring for retry.", t);
      } catch (CancellationException ce) {
        LOG.debug("Execution cancelled, ignoring for retry.");
      } catch (Throwable unknownThrowable) {
        // Don't eat unknown problem
        LOG.error("Unknown throwable", unknownThrowable);
        throw new RuntimeException(unknownThrowable);
      }

      // For each region
      for (Map.Entry<byte[], List<Put>> e : request.puts.entrySet()) {
        byte[] region = e.getKey();
        List<Put> lst = e.getValue();
        Integer result = null;
        if (resp != null)
          result = resp.getAnswer(region);

        if (result == null) {
          // failed
          LOG.debug("Failed all for region: " + Bytes.toStringBinary(region)
              + ", removing from cache");

          // HTableMultiplexer does not specify the tableName, as it is
          // shared across different tables. In that case, let us try
          // and derive the tableName from the regionName.
          StringBytes tableName =
              (givenTableName != null) ? givenTableName
              : new StringBytes(HRegionInfo.parseRegionName(region)[0]);
          deleteCachedLocation(tableName, lst.get(0).getRow(),
              request.address);

          if (failed == null) {
            failed = new ArrayList<Put>();
          }
          failed.addAll(e.getValue());
        } else if (result != HConstants.MULTIPUT_SUCCESS) {
          // some failures
          if (failed == null) {
            failed = new ArrayList<Put>();
          }
          failed.addAll(lst.subList(result, lst.size()));
          LOG.debug("Failed past " + result + " for region: "
              + Bytes.toStringBinary(region) + ", removing from cache");
        }
      }
    }
    if (toThrow != null) {
      throw toThrow;
    }

    return failed;
  }

  /**
   * If there are multiple puts in the list, process them from a a thread
   * pool, which is shared across all the HTable instance.
   *
   * However, if there is only a single put in the list, process this request
   * within the current thread.
   *
   * @param list
   *          the puts to make - successful puts will be removed.
   *
   *          In the case of an exception, we take different actions depending
   *          on the situation: - If the exception is a DoNotRetryException,
   *          we rethrow it and leave the 'list' parameter in an indeterminate
   *          state. - If the 'list' parameter is a singleton, we directly
   *          throw the specific exception for that put. - Otherwise, we throw
   *          a generic exception indicating that an error occurred. The
   *          'list' parameter is mutated to contain those puts that did not
   *          succeed.
   */
  @Override
  public void processBatchOfPuts(List<Put> list, StringBytes tableName,
      HBaseRPCOptions options) throws IOException {
    RegionOverloadedException roe = null;
    long callStartTime;
    callStartTime = System.currentTimeMillis();

    int tries;
    long serverRequestedWaitTime = 0;
    int serverRequestedRetries = 0;
    Map<String, HRegionFailureInfo> failedServersInfo = new HashMap<String, HRegionFailureInfo>();
    // Sort the puts based on the row key in order to optimize the row lock
    // acquiring
    // in the server side.
    Collections.sort(list);

    for (tries = 0; tries < params.getNumRetries() && !list.isEmpty(); ++tries) {
      // If server requested wait. We will wait for that time, and start
      // again. Do not count this time/tries against the client retries.
      if (serverRequestedWaitTime > 0) {
        serverRequestedRetries++;

        // Only do this for a configurable number of times.
        if (serverRequestedRetries > params.getMaxServerRequestedRetries()) {
          throw roe;
        }

        long sleepTimePending = callStartTime + serverRequestedWaitTime
            - System.currentTimeMillis();
        try {
          Thread.sleep(sleepTimePending);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }
        tries = 0;
        callStartTime = System.currentTimeMillis();
        serverRequestedWaitTime = 0;
      }

      List<Put> failed = null;
      List<MultiPut> multiPuts = this.splitPutsIntoMultiPuts(list, tableName,
          options);

      try {
        failed = this.processListOfMultiPut(multiPuts, tableName, options,
            failedServersInfo);
      } catch (RegionOverloadedException ex) {
        roe = ex;
        serverRequestedWaitTime = roe.getBackoffTimeMillis();
        continue;
      } catch (ClientSideDoNotRetryException exp) {
        // Bail out of the retry loop, immediately
        throw exp;
      } catch (PreemptiveFastFailException pfe) {
        throw pfe;
      }

      list.clear();
      if (failed != null && !failed.isEmpty()) {
        // retry the failed ones after sleep
        list.addAll(failed);

        // Do not sleep the first time. The region might have moved.
        if (tries <= 1)
          continue;

        long pauseTime = params.getPauseTime(tries);
        if ((System.currentTimeMillis() - callStartTime + pauseTime) > params
            .getRpcRetryTimeout()) {
          break; // we will throw a RetriesExhaustedException
        }
        LOG.debug("processBatchOfPuts had some failures, sleeping for "
            + pauseTime + " ms!");
        try {
          Thread.sleep(pauseTime);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }
      }
    }

    // Get exhausted after the retries
    if (!list.isEmpty()) {

      // ran out of retries and didnt succeed everything!
      throw new RetriesExhaustedException(failedServersInfo, "Still had "
          + list.size() + " puts left after retrying " + tries
          + " times, in " + (System.currentTimeMillis() - callStartTime)
          + " ms.");
    }
  }

  private Callable<MultiPutResponse> createPutCallable(
      final HServerAddress address, final MultiPut puts,
      final HBaseRPCOptions options) {
    final HConnection connection = this;
    return new Callable<MultiPutResponse>() {
      @Override
      public MultiPutResponse call() throws IOException {
        return getRegionServerWithoutRetries(
            new ServerCallableForBatchOps<MultiPutResponse>(connection,
                address, options) {
              @Override
              public MultiPutResponse call() throws IOException,
                  InterruptedException, ExecutionException {
                // caller of createPutCallable is responsible to track
                // mutations.
                MultiPutResponse resp = server.multiPut(puts);
                resp.request = puts;
                return resp;
              }
            }, true);
      }
    };
  }

  private Throwable translateException(Throwable t) throws IOException {
    if (t instanceof NoSuchMethodError) {
      // We probably can't recover from this exception by retrying.
      LOG.error(t);
      throw (NoSuchMethodError)t;
    }

    if (t instanceof NullPointerException) {
      // The same here. This is probably a bug.
      LOG.error(t.getMessage(), t);
      throw (NullPointerException)t;
    }

    if (t instanceof UndeclaredThrowableException) {
      t = t.getCause();
    }
    if (t instanceof RemoteException) {
      t = RemoteExceptionHandler.decodeRemoteException((RemoteException)t);
    }
    if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException)t;
    }
    if (t instanceof Error) {
      throw (Error)t;
    }
    return t;
  }

  @Override
  public void setRegionCachePrefetch(StringBytes tableName,
      final boolean enable) {
    if (!enable) {
      regionCachePrefetchDisabledTables.add(Bytes.mapKey(tableName.getBytes()));
    } else {
      regionCachePrefetchDisabledTables.remove(Bytes.mapKey(tableName
          .getBytes()));
    }
  }

  @Override
  public boolean getRegionCachePrefetch(StringBytes tableName) {
    return !regionCachePrefetchDisabledTables.contains(
        Bytes.mapKey(tableName.getBytes()));
  }

  @Override
  public void prewarmRegionCache(StringBytes tableName,
      final Map<HRegionInfo, HServerAddress> regions) {
    for (Map.Entry<HRegionInfo, HServerAddress> e : regions.entrySet()) {
      metaCache.add(tableName, new HRegionLocation(e.getKey(), e.getValue()));
    }
  }

  @Override
  public void startBatchedLoad(StringBytes tableName) {
    batchedUploadUpdatesMap.put(tableName,
        new ConcurrentHashMap<HRegionInfo, HRegionLocation>());
  }

  @Override
  public void endBatchedLoad(StringBytes tableName, HBaseRPCOptions options)
      throws IOException {
    Map<HRegionInfo, HRegionLocation> regionsUpdated = getRegionsUpdated(tableName);
    try {
      // get the current TS from the RegionServer
      Map<HRegionInfo, Long> targetTSMap = getCurrentTimeForRegions(
          regionsUpdated, options);

      // loop to ensure that we have flushed beyond the corresponding TS.
      int tries = 0;
      long now = EnvironmentEdgeManager.currentTimeMillis();
      long waitUntilForHardFlush = now + batchedUploadSoftFlushTimeoutMillis;

      while (tries++ < this.batchedUploadSoftFlushRetries
          && now < waitUntilForHardFlush) {
        // get the lastFlushedTS from the RegionServer. throws Exception if
        // the region has
        // moved elsewhere
        Map<HRegionInfo, Long> flushedTSMap = getRegionFlushTimes(
            regionsUpdated, options);

        for (Iterator<Entry<HRegionInfo, Long>> iter = targetTSMap.entrySet()
            .iterator(); iter.hasNext();) {
          Entry<HRegionInfo, Long> entry = iter.next();
          HRegionInfo region = entry.getKey();
          long targetTime = entry.getValue().longValue();
          long flushedTime = flushedTSMap.get(region).longValue();
          if (flushedTime > targetTime) {
            iter.remove();
          }
          LOG.debug("Region "
              + region.getEncodedName()
              + " was flushed at "
              + flushedTime
              + (flushedTime > targetTime ? ". All updates we made are already on disk."
                  : ". Still waiting for updates to go to the disk.")
              + " Last update was made "
              + (flushedTime > targetTime ? ((flushedTime - targetTime) + " ms before flush.")
                  : ((targetTime - flushedTime) + " ms after flush.")));
        }

        if (targetTSMap.isEmpty()) {
          LOG.info("All regions have been flushed.");
          break;
        }
        LOG.info("Try #" + tries + ". Still waiting to flush "
            + targetTSMap.size() + " regions.");
        long sleepTime = params.getPauseTime(tries);
        Threads.sleep(sleepTime);
        now = EnvironmentEdgeManager.currentTimeMillis();
      }

      // if we have not succeded in flushing all. Force flush.
      if (!targetTSMap.isEmpty()) {
        LOG.info("Forcing regions to flush.");
        flushRegionsAtServers(targetTSMap, regionsUpdated, options);
      }

      // check to see that the servers' current startcode is the same as the
      // one
      // that we have in the map. Having a different start code means that the
      // regionserver may have restarted. To avoid data loss, in case the
      // restart
      // were unclean, we will throw an exception and expect the client to
      // retry.
      //
      // Note that we should be doing this at the very end. After flushing.
      // A successful region flush does not guarantee that the data was
      // persisted correctly. A regionserver could have crashed/restarted,
      // opened the same region again, and then flushed it.
      checkServersAreAlive(regionsUpdated.values(), options);

      clearMutationsToTable(tableName);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new ClientSideDoNotRetryException(
          "One or more regionservers have restarted"
              + ". Please retry the job");
    }
  }

  @Override
  public void flushRegionAndWait(final HRegionInfo regionInfo,
      final HServerAddress addr, long acceptableWindowForLastFlush,
      long maximumWaitTime) throws IOException {
    HBaseRPCOptions options = new HBaseRPCOptions();
    try {
      long serverTime =
          (long)this.createCurrentTimeCallable(addr, options).call();
      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      long leastAcceptableFlushTime =
          serverTime - acceptableWindowForLastFlush;
      boolean forceFlush = false;
      boolean waitForFlushToHappen = true;
      do {
        long lastFlushTime = this.getRegionServerWithoutRetries(
          new ServerCallableForBatchOps<Long>(this, addr, options) {
            @Override
            public Long call() {
              return server.getLastFlushTime(regionInfo.getRegionName());
            }
        }, true);
        if (lastFlushTime >= leastAcceptableFlushTime) break; // flush happened
        long elapsedTime =
            EnvironmentEdgeManager.currentTimeMillis() - startTime;
        if (elapsedTime >= maximumWaitTime) {
          forceFlush = true;
          break;
        }
        // Sleep and hope for flush to happen
        Threads.sleep(
          this.conf.getLong(HConstants.WAIT_TIME_FOR_FLUSH_MS,
          HConstants.DEFAULT_WAIT_TIME_FOR_FLUSH_MS));
      } while (waitForFlushToHappen);
      if (forceFlush) {
        Callable<Void> flushCallable =
            this.createFlushCallable(addr, regionInfo, serverTime, options);
        flushCallable.call();
      }
    } catch (Exception e) {
      // Throwing IOException since the error is recoverable
      // and the function can be safely retried.
      throw new IOException(e);
    }
  }

  private HServerAddress getRandomServerAddress() throws IOException {
    final ArrayList<String> addrs = new ArrayList<String>();

    if (metaCache.getServers().isEmpty()) {
      getMaster();
      // If we have no cached servers
      // Use a visitor to collect a list of all the region server addresses
      // from the meta table
      MetaScannerVisitor visitor = new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result result) throws IOException {
          try {
            HRegionInfo info = Writables.getHRegionInfo(
                result.getValue(HConstants.CATALOG_FAMILY,
                    HConstants.REGIONINFO_QUALIFIER));
            if (!(info.isOffline() || info.isSplit())) {
              byte [] value = result.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.SERVER_QUALIFIER);
              if (value != null && value.length > 0) {
                addrs.add(Bytes.toString(value));
              }
            }
            return true;
          } catch (RuntimeException e) {
            LOG.error("Result=" + result);
            throw e;
          }
        }
      };
      MetaScanner.metaScan(conf, visitor);
    } else {
      // If we have any cached servers use them
      addrs.addAll(metaCache.getServers());
    }
    // return a random region server address
    int numServers = addrs.size();
    if(numServers == 0){
      throw new NoServerForRegionException("No Region Servers Found");
    }
    String serverAddrStr = addrs.get(new Random().nextInt(numServers));
    return new HServerAddress(serverAddrStr);
  }

  @Override
  public String getServerConfProperty(String prop) throws IOException{
    HServerAddress addr = getRandomServerAddress();
    HRegionInterface server = getHRegionConnection(addr);
    return server.getConfProperty(prop);
  }

  private void trackMutationsToTable(StringBytes tableName,
      HRegionLocation location) throws IOException {
    HRegionInfo regionInfo = location.getRegionInfo();
    HServerAddress serverAddress = location.getServerAddress();
    HRegionLocation oldLocation =
        !batchedUploadUpdatesMap.containsKey(tableName) ? null
            : batchedUploadUpdatesMap.get(tableName).putIfAbsent(regionInfo,
                location);
    if (oldLocation != null && !oldLocation.equals(location)) {
      // check if the old server is alive and update the map with the new
      // location
      try {
        checkIfAlive(oldLocation);
        batchedUploadUpdatesMap.get(tableName).put(regionInfo, location);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw new ClientSideDoNotRetryException("Region "
            + regionInfo.getRegionNameAsString() + " moved from "
            + oldLocation + " to." + serverAddress
            + ". Old location not reachable");
      }
    }
  }

  /**
   * Return a map of regions (and the servers they were on) to which mutations
   * were made since {@link startBatchedLoad(tableName)} was called.
   *
   * @param tableName
   * @return Map containing regionInfo, and the servers they were on.
   */
  private Map<HRegionInfo, HRegionLocation> getRegionsUpdated(
      StringBytes tableName) {
    return batchedUploadUpdatesMap.get(tableName);
  }

  /**
   * Clear the map of regions (and the servers) contacted for the specified
   * table.
   *
   * @param tableName
   */
  private void clearMutationsToTable(StringBytes tableName) {
    if (batchedUploadUpdatesMap.containsKey(tableName)) {
      batchedUploadUpdatesMap.get(tableName).clear();
    }
  }

  /**
   * This method is called to check if the remote server is alive, and has the
   * same invocation id/start code, as before.
   *
   * @param location
   * @throws RuntimeException
   * @throws IOException
   */
  private void checkIfAlive(HRegionLocation location) throws IOException {
    checkServersAreAlive(Collections.singletonList(location),
        HBaseRPCOptions.DEFAULT);
  }

  /**
   * This method is called to check if the servers that hosted the regions are
   * alive, and have the same invocation id/start code, as before.
   *
   * @param regionsContacted
   *          -- collection of region locations
   * @param options
   *          -- rpc options to use
   * @throws IOException
   *           if (a) could not talk to the server, or (b) any of the regions
   *           have moved from the location indicated in regionsContacted.
   */
  private void checkServersAreAlive(
      Collection<HRegionLocation> regionsContacted, HBaseRPCOptions options)
      throws IOException {

    HashMap<HServerAddress, Future<Long>> futures = new HashMap<HServerAddress, Future<Long>>();
    Map<HServerAddress, Long> currentServerStartCode = new HashMap<HServerAddress, Long>();

    // get start codes from that server
    for (HRegionLocation location : regionsContacted) {
      HServerAddress server = location.getServerAddress();
      if (!futures.containsKey(server)) {
        futures.put(server, HTable.multiActionThreadPool
            .submit(createGetServerStartCodeCallable(server, options)));
      }
    }

    // populate server start codes;
    for (HServerAddress server : futures.keySet()) {
      Future<Long> future = futures.get(server);
      try {
        currentServerStartCode.put(server, future.get());
      } catch (InterruptedException e) {
        throw new IOException(
            "Interrupted: Could not get current time from server" + server);
      } catch (ExecutionException e) {
        throw new IOException("Could not get current time from " + server,
            e.getCause());
      }
    }

    // check startcodes for each region
    for (HRegionLocation location : regionsContacted) {
      HServerAddress server = location.getServerAddress();
      long expectedStartCode = location.getServerStartCode();
      Long startCode = currentServerStartCode.get(server);
      if (startCode.longValue() != expectedStartCode) {
        LOG.debug("Current startcode for server " + server + " is "
            + startCode.longValue() + " looking for " + location.toString());
        throw new IOException("RegionServer restarted.");
      }
    }
  }

  /**
   * Get the current time in milliseconds at the server for each of the
   * regions in the map.
   *
   * @param regionsContacted
   *          -- map of regions to server address
   * @param options
   *          -- rpc options to use
   * @return Map of regions to Long, representing current time in mills at the
   *         server
   * @throws IOException
   *           if (a) could not talk to the server, or (b) any of the regions
   *           have moved from the location indicated in regionsContacted.
   */
  private Map<HRegionInfo, Long> getCurrentTimeForRegions(
      Map<HRegionInfo, HRegionLocation> regionsContacted,
      HBaseRPCOptions options) throws IOException {

    Map<HRegionInfo, Long> currentTimeForRegions = new HashMap<HRegionInfo, Long>();

    Map<HServerAddress, Long> currentTimeAtServers = new HashMap<HServerAddress, Long>();
    HashMap<HServerAddress, Future<Long>> futures = new HashMap<HServerAddress, Future<Long>>();

    // get flush times from that server
    for (HRegionLocation location : regionsContacted.values()) {
      HServerAddress server = location.getServerAddress();
      if (!futures.containsKey(server)) {
        futures.put(server, HTable.multiActionThreadPool
            .submit(createCurrentTimeCallable(server, options)));
      }
    }

    // populate serverTimes;
    for (HServerAddress server : futures.keySet()) {
      Future<Long> future = futures.get(server);
      try {
        currentTimeAtServers.put(server, future.get());
      } catch (InterruptedException e) {
        throw new IOException(
            "Interrupted: Could not get current time from server" + server,
            e.getCause());
      } catch (ExecutionException e) {
        throw new IOException("Could not get current time from " + server,
            e.getCause());
      }
    }

    for (HRegionInfo region : regionsContacted.keySet()) {
      HServerAddress serverToLookFor = regionsContacted.get(region)
          .getServerAddress();
      Long currentTime = currentTimeAtServers.get(serverToLookFor);
      currentTimeForRegions.put(region, currentTime);
    }
    return currentTimeForRegions;
  }

  /**
   * Ask the regionservers to flush the given regions, if they have not
   * flushed past the desired time.
   *
   * @param targetTSMap
   *          -- map of regions to the desired flush time
   * @param regionsUpdated
   *          -- map of regions to server address
   * @param options
   *          -- rpc options to use
   * @throws IOException
   *           if (a) could not talk to the server, or (b) any of the regions
   *           have moved from the location indicated in regionsContacted.
   */
  private void flushRegionsAtServers(Map<HRegionInfo, Long> targetTSMap,
      Map<HRegionInfo, HRegionLocation> regionsUpdated,
      HBaseRPCOptions options) throws IOException {

    Map<HRegionInfo, Future<Void>> futures = new HashMap<HRegionInfo, Future<Void>>();

    // get flush times from that server
    for (HRegionInfo region : targetTSMap.keySet()) {
      HServerAddress server = regionsUpdated.get(region).getServerAddress();
      long targetFlushTime = targetTSMap.get(region).longValue();

      LOG.debug("forcing a flush at " + server.getHostname());
      futures.put(region, HTable.multiActionThreadPool.submit(
      // use targetFlushTime + 1 to force a flush even if they are equal.
          createFlushCallable(server, region, targetFlushTime + 1, options)));
    }

    boolean toCancel = false;
    IOException toThrow = null;
    // populate regionTimes;
    for (HRegionInfo region : futures.keySet()) {
      Future<Void> future = futures.get(region);
      try {
        if (!toCancel) {
          future.get();
        } else {
          future.cancel(true);
        }
      } catch (InterruptedException e) {
        toCancel = true;
        toThrow = new IOException("Got Interrupted: Could not flush region "
            + region);
      } catch (ExecutionException e) {
        toThrow = new IOException("Could not flush region " + region,
            e.getCause());
      }
    }
    if (toThrow != null) {
      throw toThrow;
    }
  }

  /**
   * Ask the regionservers for the last flushed time for each regions.
   *
   * @param regionsUpdated
   *          -- map of regions to server address
   * @param options
   *          -- rpc options to use
   * @return Map from the region to the region's last flushed time.
   * @throws IOException
   *           if (a) could not talk to the server, or (b) any of the regions
   *           have moved from the location indicated in regionsContacted.
   */
  private Map<HRegionInfo, Long> getRegionFlushTimes(
      Map<HRegionInfo, HRegionLocation> regionsUpdated,
      HBaseRPCOptions options) throws IOException {

    Map<HRegionInfo, Long> regionFlushTimesMap = new HashMap<HRegionInfo, Long>();

    Map<HServerAddress, MapWritable> rsRegionTimes = new HashMap<HServerAddress, MapWritable>();
    Map<HServerAddress, Future<MapWritable>> futures = new HashMap<HServerAddress, Future<MapWritable>>();

    // get flush times from that server
    for (HRegionLocation location : regionsUpdated.values()) {
      HServerAddress server = location.getServerAddress();
      if (!futures.containsKey(server)) {
        futures.put(server, HTable.multiActionThreadPool
            .submit(createGetLastFlushTimesCallable(server, options)));
      }
    }

    boolean toCancel = false;
    IOException toThrow = null;
    // populate flushTimes;
    for (HServerAddress server : futures.keySet()) {
      Future<MapWritable> future = futures.get(server);
      try {
        if (!toCancel) {
          rsRegionTimes.put(server, future.get());
        } else {
          future.cancel(true);
        }
      } catch (InterruptedException e) {
        toThrow = new IOException(
            "Got Interrupted: Could not get last flushed times");
      } catch (ExecutionException e) {
        toThrow = new IOException("Could not get last flushed times from "
            + server, e.getCause());
      }
    }
    if (toThrow != null) {
      throw toThrow;
    }

    for (HRegionInfo region : regionsUpdated.keySet()) {
      HServerAddress serverToLookFor = regionsUpdated.get(region)
          .getServerAddress();

      MapWritable serverMap = rsRegionTimes.get(serverToLookFor);
      LongWritable lastFlushedTime = (LongWritable) serverMap
          .get(new BytesWritable(region.getRegionName()));

      if (lastFlushedTime == null) { // The region is no longer on the server
        throw new ClientSideDoNotRetryException("Region "
            + region.getRegionNameAsString() + " was on " + serverToLookFor
            + " but no longer there.");
      }

      regionFlushTimesMap.put(region, new Long(lastFlushedTime.get()));
    }
    return regionFlushTimesMap;
  }

  void validateAndIncrementNumOutstandingPerServer(HServerAddress address)
      throws TooManyOutstandingRequestsException {
    AtomicInteger atomicOutstanding = getOutstandingCount(address);

    int outstanding = atomicOutstanding.get();
    if (outstanding > maxOutstandingRequestsPerServer) {
      throw new TooManyOutstandingRequestsException(address, outstanding);
    }
    atomicOutstanding.incrementAndGet();
  }

  void decrementNumOutstandingPerServer(HServerAddress address) {
    AtomicInteger outstanding = getOutstandingCount(address);
    outstanding.decrementAndGet();
  }

  private AtomicInteger getOutstandingCount(HServerAddress address) {
    AtomicInteger out = outstandingRequests.get(address);
    if (out == null) {
      out = new AtomicInteger(0);
      AtomicInteger prev = outstandingRequests.putIfAbsent(address, out);
      if (prev != null) {
        out = prev;
      }
    }
    return out;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  private class OutstandingRequestCallback<V> implements FutureCallback<V> {

    private final HServerAddress server;

    public OutstandingRequestCallback(HServerAddress server) {
      this.server = server;
    }

    @Override public void onSuccess(@Nullable V result) {
      decrementNumOutstandingPerServer(server);
    }

    @Override public void onFailure(Throwable t) {
      decrementNumOutstandingPerServer(server);
    }
  }
}
