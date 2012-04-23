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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.MutateType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.regionserver.HRegionServer.QosPriority;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRootHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRootHandler;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * RegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many RegionServers in a single HBase deployment.
 *
 * This will be a replacement for the HRegionServer. It has protobuf protocols
 * implementations. All the HRegionInterface implementations stay in HRegionServer
 * for possible backward compatibility requests.  This also makes it easier to
 * rip of HRegionInterface later on.
 */
@InterfaceAudience.Private
public abstract class RegionServer implements
    ClientProtocol, AdminProtocol, Runnable, RegionServerServices {

  private static final Log LOG = LogFactory.getLog(RegionServer.class);

  private final Random rand = new Random();

  /*
   * Strings to be used in forming the exception message for
   * RegionsAlreadyInTransitionException.
   */
  protected static final String OPEN = "OPEN";
  protected static final String CLOSE = "CLOSE";

  //RegionName vs current action in progress
  //true - if open region action in progress
  //false - if close region action in progress
  protected final ConcurrentSkipListMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);

  protected long maxScannerResultSize;

  // Cache flushing
  protected MemStoreFlusher cacheFlusher;

  // catalog tracker
  protected CatalogTracker catalogTracker;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  protected ReplicationSourceService replicationSourceHandler;
  protected ReplicationSinkService replicationSinkHandler;

  // Compactions
  public CompactSplitThread compactSplitThread;

  final Map<String, RegionScanner> scanners =
      new ConcurrentHashMap<String, RegionScanner>();

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  protected final Map<String, HRegion> onlineRegions =
    new ConcurrentHashMap<String, HRegion>();

  // Leases
  protected Leases leases;

  // Instance of the hbase executor service.
  protected ExecutorService service;

  // Request counter.
  // Do we need this?  Can't we just sum region counters?  St.Ack 20110412
  protected AtomicInteger requestCount = new AtomicInteger();

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;
  protected HFileSystem fs;

  protected static final int NORMAL_QOS = 0;
  protected static final int QOS_THRESHOLD = 10;  // the line between low and high qos
  protected static final int HIGH_QOS = 100;

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  protected volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;

  Map<String, Integer> rowlocks = new ConcurrentHashMap<String, Integer>();

  /**
   * Instantiated as a row lock lease. If the lease times out, the row lock is
   * released
   */
  private class RowLockListener implements LeaseListener {
    private final String lockName;
    private final HRegion region;

    RowLockListener(final String lockName, final HRegion region) {
      this.lockName = lockName;
      this.region = region;
    }

    public void leaseExpired() {
      LOG.info("Row Lock " + this.lockName + " lease expired");
      Integer r = rowlocks.remove(this.lockName);
      if (r != null) {
        region.releaseRowLock(r);
      }
    }
  }

  /**
   * Instantiated as a scanner lease. If the lease times out, the scanner is
   * closed
   */
  private class ScannerListener implements LeaseListener {
    private final String scannerName;

    ScannerListener(final String n) {
      this.scannerName = n;
    }

    public void leaseExpired() {
      RegionScanner s = scanners.remove(this.scannerName);
      if (s != null) {
        LOG.info("Scanner " + this.scannerName + " lease expired on region "
            + s.getRegionInfo().getRegionNameAsString());
        try {
          HRegion region = getRegion(s.getRegionInfo().getRegionName());
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().preScannerClose(s);
          }

          s.close();
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().postScannerClose(s);
          }
        } catch (IOException e) {
          LOG.error("Closing scanner for "
              + s.getRegionInfo().getRegionNameAsString(), e);
        }
      } else {
        LOG.info("Scanner " + this.scannerName + " lease expired");
      }
    }
  }

  /**
   * Method to get the Integer lock identifier used internally from the long
   * lock identifier used by the client.
   *
   * @param lockId
   *          long row lock identifier from client
   * @return intId Integer row lock used internally in HRegion
   * @throws IOException
   *           Thrown if this is not a valid client lock id.
   */
  Integer getLockFromId(long lockId) throws IOException {
    if (lockId == -1L) {
      return null;
    }
    String lockName = String.valueOf(lockId);
    Integer rl = rowlocks.get(lockName);
    if (rl == null) {
      throw new UnknownRowLockException("Invalid row lock");
    }
    this.leases.renewLease(lockName);
    return rl;
  }

  /**
   * Called to verify that this server is up and running.
   *
   * @throws IOException
   */
  protected void checkOpen() throws IOException {
    if (this.stopped || this.abortRequested) {
      throw new RegionServerStoppedException("Server " + getServerName() +
        " not running" + (this.abortRequested ? ", aborting" : ""));
    }
    if (!fsOk) {
      throw new RegionServerStoppedException("File system not available");
    }
  }

  protected void checkIfRegionInTransition(HRegionInfo region,
      String currentAction) throws RegionAlreadyInTransitionException {
    byte[] encodedName = region.getEncodedNameAsBytes();
    if (this.regionsInTransitionInRS.containsKey(encodedName)) {
      boolean openAction = this.regionsInTransitionInRS.get(encodedName);
      // The below exception message will be used in master.
      throw new RegionAlreadyInTransitionException("Received:" + currentAction +
        " for the region:" + region.getRegionNameAsString() +
        " ,which we are already trying to " +
        (openAction ? OPEN : CLOSE)+ ".");
    }
  }

  /**
   * @param region Region to close
   * @param abort True if we are aborting
   * @param zk True if we are to update zk about the region close; if the close
   * was orchestrated by master, then update zk.  If the close is being run by
   * the regionserver because its going down, don't update zk.
   * @return True if closed a region.
   */
  protected boolean closeRegion(HRegionInfo region, final boolean abort,
      final boolean zk) {
    return closeRegion(region, abort, zk, -1);
  }


    /**
   * @param region Region to close
   * @param abort True if we are aborting
   * @param zk True if we are to update zk about the region close; if the close
   * was orchestrated by master, then update zk.  If the close is being run by
   * the regionserver because its going down, don't update zk.
   * @param versionOfClosingNode
   *   the version of znode to compare when RS transitions the znode from
   *   CLOSING state.
   * @return True if closed a region.
   */
  protected boolean closeRegion(HRegionInfo region, final boolean abort,
      final boolean zk, final int versionOfClosingNode) {
    if (this.regionsInTransitionInRS.containsKey(region.getEncodedNameAsBytes())) {
      LOG.warn("Received close for region we are already opening or closing; " +
        region.getEncodedName());
      return false;
    }
    this.regionsInTransitionInRS.putIfAbsent(region.getEncodedNameAsBytes(), false);
    CloseRegionHandler crh = null;
    if (region.isRootRegion()) {
      crh = new CloseRootHandler(this, this, region, abort, zk,
        versionOfClosingNode);
    } else if (region.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, region, abort, zk,
        versionOfClosingNode);
    } else {
      crh = new CloseRegionHandler(this, this, region, abort, zk,
        versionOfClosingNode);
    }
    this.service.submit(crh);
    return true;
  }

   /**
   * @param regionName
   * @return HRegion for the passed binary <code>regionName</code> or null if
   *         named region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    return this.onlineRegions.get(encodedRegionName);
  }

  @Override
  public HRegion getFromOnlineRegions(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }

  /**
   * Protected utility method for safely obtaining an HRegion handle.
   *
   * @param regionName
   *          Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final byte[] regionName)
      throws NotServingRegionException {
    HRegion region = null;
    region = getOnlineRegion(regionName);
    if (region == null) {
      throw new NotServingRegionException("Region is not online: " +
        Bytes.toStringBinary(regionName));
    }
    return region;
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  protected Throwable cleanup(final Throwable t) {
    return cleanup(t, null);
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @param msg Message to log in error. Can be null.
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  protected Throwable cleanup(final Throwable t, final String msg) {
    // Don't log as error if NSRE; NSRE is 'normal' operation.
    if (t instanceof NotServingRegionException) {
      LOG.debug("NotServingRegionException; " +  t.getMessage());
      return t;
    }
    if (msg == null) {
      LOG.error("", RemoteExceptionHandler.checkThrowable(t));
    } else {
      LOG.error(msg, RemoteExceptionHandler.checkThrowable(t));
    }
    if (!checkOOME(t)) {
      checkFileSystem();
    }
    return t;
  }

  /*
   * @param t
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  protected IOException convertThrowableToIOE(final Throwable t) {
    return convertThrowableToIOE(t, null);
  }

  /*
   * @param t
   *
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  protected IOException convertThrowableToIOE(final Throwable t, final String msg) {
    return (t instanceof IOException ? (IOException) t : msg == null
        || msg.length() == 0 ? new IOException(t) : new IOException(msg, t));
  }

  /*
   * Check if an OOME and, if so, abort immediately to avoid creating more objects.
   *
   * @param e
   *
   * @return True if we OOME'd and are aborting.
   */
  public boolean checkOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (e instanceof OutOfMemoryError
          || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
          || (e.getMessage() != null && e.getMessage().contains(
              "java.lang.OutOfMemoryError"))) {
        stop = true;
        LOG.fatal(
          "Run out of memory; HRegionServer will abort itself immediately", e);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
  }

  /**
   * Checks to see if the file system is still accessible. If not, sets
   * abortRequested and stopRequested
   *
   * @return false if file system is not available
   */
  public boolean checkFileSystem() {
    if (this.fsOk && this.fs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
      } catch (IOException e) {
        abort("File System not available", e);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  protected long addRowLock(Integer r, HRegion region)
      throws LeaseStillHeldException {
    long lockId = nextLong();
    String lockName = String.valueOf(lockId);
    rowlocks.put(lockName, r);
    this.leases.createLease(lockName, new RowLockListener(lockName, region));
    return lockId;
  }

  protected long addScanner(RegionScanner s) throws LeaseStillHeldException {
    long scannerId = nextLong();
    String scannerName = String.valueOf(scannerId);
    scanners.put(scannerName, s);
    this.leases.createLease(scannerName, new ScannerListener(scannerName));
    return scannerId;
  }

  /**
   * Generate a random positive long number
   *
   * @return a random positive long number
   */
  protected long nextLong() {
    long n = rand.nextLong();
    if (n == 0) {
      return nextLong();
    }
    if (n < 0) {
      n = -n;
    }
    return n;
  }

  // Start Client methods

  /**
   * Get data from a table.
   *
   * @param controller the RPC controller
   * @param request the get request
   * @throws ServiceException
   */
  @Override
  public GetResponse get(final RpcController controller,
      final GetRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      GetResponse.Builder builder = GetResponse.newBuilder();
      ClientProtos.Get get = request.getGet();
      Boolean existence = null;
      Result r = null;
      if (request.getClosestRowBefore()) {
        if (get.getColumnCount() != 1) {
          throw new DoNotRetryIOException(
            "get ClosestRowBefore supports one and only one family now, not "
              + get.getColumnCount() + " families");
        }
        byte[] row = get.getRow().toByteArray();
        byte[] family = get.getColumn(0).getFamily().toByteArray();
        r = region.getClosestRowBefore(row, family);
      } else {
        Get clientGet = ProtobufUtil.toGet(get);
        if (request.getExistenceOnly() && region.getCoprocessorHost() != null) {
          existence = region.getCoprocessorHost().preExists(clientGet);
        }
        if (existence == null) {
          Integer lock = getLockFromId(clientGet.getLockId());
          r = region.get(clientGet, lock);
          if (request.getExistenceOnly()) {
            boolean exists = r != null && !r.isEmpty();
            if (region.getCoprocessorHost() != null) {
              exists = region.getCoprocessorHost().postExists(clientGet, exists);
            }
            existence = Boolean.valueOf(exists);
          }
        }
      }
      if (existence != null) {
        builder.setExists(existence.booleanValue());
      } else if (r != null) {
        builder.setResult(ProtobufUtil.toResult(r));
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Mutate data in a table.
   *
   * @param controller the RPC controller
   * @param request the mutate request
   * @throws ServiceException
   */
  @Override
  public MutateResponse mutate(final RpcController controller,
      final MutateRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      MutateResponse.Builder builder = MutateResponse.newBuilder();
      Mutate mutate = request.getMutate();
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }
      Integer lock = null;
      Result r = null;
      Boolean processed = null;
      MutateType type = mutate.getMutateType();
      switch (type) {
      case APPEND:
        r = append(region, mutate);
        break;
      case INCREMENT:
        r = increment(region, mutate);
        break;
      case PUT:
        Put put = ProtobufUtil.toPut(mutate);
        lock = getLockFromId(put.getLockId());
        if (request.hasCondition()) {
          Condition condition = request.getCondition();
          byte[] row = condition.getRow().toByteArray();
          byte[] family = condition.getFamily().toByteArray();
          byte[] qualifier = condition.getQualifier().toByteArray();
          CompareOp compareOp = CompareOp.valueOf(condition.getCompareType().name());
          WritableByteArrayComparable comparator =
            (WritableByteArrayComparable)ProtobufUtil.toObject(condition.getComparator());
          if (region.getCoprocessorHost() != null) {
            processed = region.getCoprocessorHost().preCheckAndPut(
              row, family, qualifier, compareOp, comparator, put);
          }
          if (processed == null) {
            boolean result = region.checkAndMutate(row, family,
              qualifier, compareOp, comparator, put, lock, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndPut(row, family,
                qualifier, compareOp, comparator, put, result);
            }
            processed = Boolean.valueOf(result);
          }
        } else {
          region.put(put, lock);
          processed = Boolean.TRUE;
        }
        break;
      case DELETE:
        Delete delete = ProtobufUtil.toDelete(mutate);
        lock = getLockFromId(delete.getLockId());
        if (request.hasCondition()) {
          Condition condition = request.getCondition();
          byte[] row = condition.getRow().toByteArray();
          byte[] family = condition.getFamily().toByteArray();
          byte[] qualifier = condition.getQualifier().toByteArray();
          CompareOp compareOp = CompareOp.valueOf(condition.getCompareType().name());
          WritableByteArrayComparable comparator =
            (WritableByteArrayComparable)ProtobufUtil.toObject(condition.getComparator());
          if (region.getCoprocessorHost() != null) {
            processed = region.getCoprocessorHost().preCheckAndDelete(
              row, family, qualifier, compareOp, comparator, delete);
          }
          if (processed == null) {
            boolean result = region.checkAndMutate(row, family,
              qualifier, compareOp, comparator, delete, lock, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndDelete(row, family,
                qualifier, compareOp, comparator, delete, result);
            }
            processed = Boolean.valueOf(result);
          }
        } else {
          region.delete(delete, lock, delete.getWriteToWAL());
          processed = Boolean.TRUE;
        }
        break;
        default:
          throw new DoNotRetryIOException(
            "Unsupported mutate type: " + type.name());
      }
      if (processed != null) {
        builder.setProcessed(processed.booleanValue());
      } else if (r != null) {
        builder.setResult(ProtobufUtil.toResult(r));
      }
      return builder.build();
    } catch (IOException ie) {
      checkFileSystem();
      throw new ServiceException(ie);
    }
  }

  //
  // remote scanner interface
  //

  /**
   * Scan data in a table.
   *
   * @param controller the RPC controller
   * @param request the scan request
   * @throws ServiceException
   */
  @Override
  public ScanResponse scan(final RpcController controller,
      final ScanRequest request) throws ServiceException {
    Leases.Lease lease = null;
    String scannerName = null;
    try {
      if (!request.hasScannerId() && !request.hasScan()) {
        throw new DoNotRetryIOException(
          "Missing required input: scannerId or scan");
      }
      long scannerId = -1;
      if (request.hasScannerId()) {
        scannerId = request.getScannerId();
        scannerName = String.valueOf(scannerId);
      }
      try {
        checkOpen();
      } catch (IOException e) {
        // If checkOpen failed, server not running or filesystem gone,
        // cancel this lease; filesystem is gone or we're closing or something.
        if (scannerName != null) {
          try {
            leases.cancelLease(scannerName);
          } catch (LeaseException le) {
            LOG.info("Server shutting down and client tried to access missing scanner " +
              scannerName);
          }
        }
        throw e;
      }
      requestCount.incrementAndGet();

      try {
        int ttl = 0;
        HRegion region = null;
        RegionScanner scanner = null;
        boolean moreResults = true;
        boolean closeScanner = false;
        ScanResponse.Builder builder = ScanResponse.newBuilder();
        if (request.hasCloseScanner()) {
          closeScanner = request.getCloseScanner();
        }
        int rows = 1;
        if (request.hasNumberOfRows()) {
          rows = request.getNumberOfRows();
        }
        if (request.hasScannerId()) {
          scanner = scanners.get(scannerName);
          if (scanner == null) {
            throw new UnknownScannerException(
              "Name: " + scannerName + ", already closed?");
          }
          region = getRegion(scanner.getRegionInfo().getRegionName());
        } else {
          region = getRegion(request.getRegion());
          ClientProtos.Scan protoScan = request.getScan();
          Scan scan = ProtobufUtil.toScan(protoScan);
          region.prepareScanner(scan);
          if (region.getCoprocessorHost() != null) {
            scanner = region.getCoprocessorHost().preScannerOpen(scan);
          }
          if (scanner == null) {
            scanner = region.getScanner(scan);
          }
          if (region.getCoprocessorHost() != null) {
            scanner = region.getCoprocessorHost().postScannerOpen(scan, scanner);
          }
          scannerId = addScanner(scanner);
          scannerName = String.valueOf(scannerId);
          ttl = leases.leasePeriod;
        }

        if (rows > 0) {
          try {
            // Remove lease while its being processed in server; protects against case
            // where processing of request takes > lease expiration time.
            lease = leases.removeLease(scannerName);
            List<Result> results = new ArrayList<Result>(rows);
            long currentScanResultSize = 0;

            boolean done = false;
            // Call coprocessor. Get region info from scanner.
            if (region != null && region.getCoprocessorHost() != null) {
              Boolean bypass = region.getCoprocessorHost().preScannerNext(
                scanner, results, rows);
              if (!results.isEmpty()) {
                for (Result r : results) {
                  for (KeyValue kv : r.raw()) {
                    currentScanResultSize += kv.heapSize();
                  }
                }
              }
              if (bypass != null && bypass.booleanValue()) {
                done = true;
              }
            }

            if (!done) {
              List<KeyValue> values = new ArrayList<KeyValue>();
              for (int i = 0; i < rows
                  && currentScanResultSize < maxScannerResultSize; i++) {
                // Collect values to be returned here
                boolean moreRows = scanner.next(values, SchemaMetrics.METRIC_NEXTSIZE);
                if (!values.isEmpty()) {
                  for (KeyValue kv : values) {
                    currentScanResultSize += kv.heapSize();
                  }
                  results.add(new Result(values));
                }
                if (!moreRows) {
                  break;
                }
                values.clear();
              }

              // coprocessor postNext hook
              if (region != null && region.getCoprocessorHost() != null) {
                region.getCoprocessorHost().postScannerNext(scanner, results, rows, true);
              }
            }

            // If the scanner's filter - if any - is done with the scan
            // and wants to tell the client to stop the scan. This is done by passing
            // a null result, and setting moreResults to false.
            if (scanner.isFilterDone() && results.isEmpty()) {
              moreResults = false;
              results = null;
            } else {
              for (Result result: results) {
                if (result != null) {
                  builder.addResult(ProtobufUtil.toResult(result));
                }
              }
            }
          } finally {
            // We're done. On way out re-add the above removed lease.
            // Adding resets expiration time on lease.
            if (scanners.containsKey(scannerName)) {
              if (lease != null) leases.addLease(lease);
              ttl = leases.leasePeriod;
            }
          }
        }

        if (!moreResults || closeScanner) {
          ttl = 0;
          moreResults = false;
          if (region != null && region.getCoprocessorHost() != null) {
            if (region.getCoprocessorHost().preScannerClose(scanner)) {
              return builder.build(); // bypass
            }
          }
          scanner = scanners.remove(scannerName);
          if (scanner != null) {
            scanner.close();
            leases.cancelLease(scannerName);
            if (region != null && region.getCoprocessorHost() != null) {
              region.getCoprocessorHost().postScannerClose(scanner);
            }
          }
        }

        if (ttl > 0) {
          builder.setTtl(ttl);
        }
        builder.setScannerId(scannerId);
        builder.setMoreResults(moreResults);
        return builder.build();
      } catch (Throwable t) {
        if (scannerName != null &&
            t instanceof NotServingRegionException) {
          scanners.remove(scannerName);
        }
        throw convertThrowableToIOE(cleanup(t));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Lock a row in a table.
   *
   * @param controller the RPC controller
   * @param request the lock row request
   * @throws ServiceException
   */
  @Override
  public LockRowResponse lockRow(final RpcController controller,
      final LockRowRequest request) throws ServiceException {
    try {
      if (request.getRowCount() != 1) {
        throw new DoNotRetryIOException(
          "lockRow supports only one row now, not " + request.getRowCount() + " rows");
      }
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      byte[] row = request.getRow(0).toByteArray();
      try {
        Integer r = region.obtainRowLock(row);
        long lockId = addRowLock(r, region);
        LOG.debug("Row lock " + lockId + " explicitly acquired by client");
        LockRowResponse.Builder builder = LockRowResponse.newBuilder();
        builder.setLockId(lockId);
        return builder.build();
      } catch (Throwable t) {
        throw convertThrowableToIOE(cleanup(t,
          "Error obtaining row lock (fsOk: " + this.fsOk + ")"));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Unlock a locked row in a table.
   *
   * @param controller the RPC controller
   * @param request the unlock row request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HIGH_QOS)
  public UnlockRowResponse unlockRow(final RpcController controller,
      final UnlockRowRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      if (!request.hasLockId()) {
        throw new DoNotRetryIOException(
          "Invalid unlock rowrequest, missing lock id");
      }
      long lockId = request.getLockId();
      String lockName = String.valueOf(lockId);
      try {
        Integer r = rowlocks.remove(lockName);
        if (r == null) {
          throw new UnknownRowLockException(lockName);
        }
        region.releaseRowLock(r);
        this.leases.cancelLease(lockName);
        LOG.debug("Row lock " + lockId
          + " has been explicitly released by client");
        return UnlockRowResponse.newBuilder().build();
      } catch (Throwable t) {
        throw convertThrowableToIOE(cleanup(t));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Atomically bulk load several HFiles into an open region
   * @return true if successful, false is failed but recoverably (no action)
   * @throws IOException if failed unrecoverably
   */
  @Override
  public BulkLoadHFileResponse bulkLoadHFile(final RpcController controller,
      final BulkLoadHFileRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      List<Pair<byte[], String>> familyPaths = new ArrayList<Pair<byte[], String>>();
      for (FamilyPath familyPath: request.getFamilyPathList()) {
        familyPaths.add(new Pair<byte[], String>(
          familyPath.getFamily().toByteArray(), familyPath.getPath()));
      }
      boolean loaded = region.bulkLoadHFiles(familyPaths);
      BulkLoadHFileResponse.Builder builder = BulkLoadHFileResponse.newBuilder();
      builder.setLoaded(loaded);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Executes a single {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol}
   * method using the registered protocol handlers.
   * {@link CoprocessorProtocol} implementations must be registered per-region
   * via the
   * {@link org.apache.hadoop.hbase.regionserver.HRegion#registerProtocol(Class, org.apache.hadoop.hbase.ipc.CoprocessorProtocol)}
   * method before they are available.
   *
   * @param regionName name of the region against which the invocation is executed
   * @param call an {@code Exec} instance identifying the protocol, method name,
   *     and parameters for the method invocation
   * @return an {@code ExecResult} instance containing the region name of the
   *     invocation and the return value
   * @throws IOException if no registered protocol handler is found or an error
   *     occurs during the invocation
   * @see org.apache.hadoop.hbase.regionserver.HRegion#registerProtocol(Class, org.apache.hadoop.hbase.ipc.CoprocessorProtocol)
   */
  @Override
  public ExecCoprocessorResponse execCoprocessor(final RpcController controller,
      final ExecCoprocessorRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      ExecCoprocessorResponse.Builder
        builder = ExecCoprocessorResponse.newBuilder();
      ClientProtos.Exec call = request.getCall();
      Exec clientCall = ProtobufUtil.toExec(call);
      ExecResult result = region.exec(clientCall);
      builder.setValue(ProtobufUtil.toParameter(result.getValue()));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Execute multiple actions on a table: get, mutate, and/or execCoprocessor
   *
   * @param controller the RPC controller
   * @param request the multi request
   * @throws ServiceException
   */
  @Override
  public MultiResponse multi(final RpcController controller,
      final MultiRequest request) throws ServiceException {
    try {
      HRegion region = getRegion(request.getRegion());
      MultiResponse.Builder builder = MultiResponse.newBuilder();
      if (request.hasAtomic() && request.getAtomic()) {
        List<Mutate> mutates = new ArrayList<Mutate>();
        for (NameBytesPair parameter: request.getActionList()) {
          Object action = ProtobufUtil.toObject(parameter);
          if (action instanceof Mutate) {
            mutates.add((Mutate)action);
          } else {
            throw new DoNotRetryIOException(
              "Unsupported atomic atction type: "
                + action.getClass().getName());
          }
        }
        mutateRows(region, mutates);
      } else {
        ActionResult.Builder resultBuilder = null;
        List<Mutate> puts = new ArrayList<Mutate>();
        for (NameBytesPair parameter: request.getActionList()) {
          requestCount.incrementAndGet();
          try {
            Object result = null;
            Object action = ProtobufUtil.toObject(parameter);
            if (action instanceof ClientProtos.Get) {
              Get get = ProtobufUtil.toGet((ClientProtos.Get)action);
              Integer lock = getLockFromId(get.getLockId());
              Result r = region.get(get, lock);
              if (r != null) {
                result = ProtobufUtil.toResult(r);
              }
            } else if (action instanceof Mutate) {
              Mutate mutate = (Mutate)action;
              MutateType type = mutate.getMutateType();
              if (type != MutateType.PUT) {
                if (!puts.isEmpty()) {
                  put(builder, region, puts);
                  puts.clear();
                } else if (!region.getRegionInfo().isMetaTable()) {
                  cacheFlusher.reclaimMemStoreMemory();
                }
              }
              Result r = null;
              switch (type) {
              case APPEND:
                r = append(region, mutate);
                break;
              case INCREMENT:
                r = increment(region, mutate);
                break;
              case PUT:
                puts.add(mutate);
                break;
              case DELETE:
                Delete delete = ProtobufUtil.toDelete(mutate);
                Integer lock = getLockFromId(delete.getLockId());
                region.delete(delete, lock, delete.getWriteToWAL());
                r = new Result();
                break;
                default:
                  throw new DoNotRetryIOException(
                    "Unsupported mutate type: " + type.name());
              }
              if (r != null) {
                result = ProtobufUtil.toResult(r);
              }
            } else if (action instanceof ClientProtos.Exec) {
              Exec call = ProtobufUtil.toExec((ClientProtos.Exec)action);
              result = region.exec(call).getValue();
            } else {
              LOG.debug("Error: invalid action, "
                + "it must be a Get, Mutate, or Exec.");
              throw new DoNotRetryIOException("Invalid action, "
                + "it must be a Get, Mutate, or Exec.");
            }
            if (result != null) {
              if (resultBuilder == null) {
                resultBuilder = ActionResult.newBuilder();
              } else {
                resultBuilder.clear();
              }
              NameBytesPair value = ProtobufUtil.toParameter(result);
              resultBuilder.setValue(value);
              builder.addResult(resultBuilder.build());
            }
          } catch (IOException ie) {
            builder.addResult(ResponseConverter.buildActionResult(ie));
          }
        }
        if (!puts.isEmpty()) {
          put(builder, region, puts);
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

// End Client methods
// Start Admin methods

  @Override
  @QosPriority(priority=HIGH_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
      final GetRegionInfoRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      HRegionInfo info = region.getRegionInfo();
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(ProtobufUtil.toRegionInfo(info));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetStoreFileResponse getStoreFile(final RpcController controller,
      final GetStoreFileRequest request) throws ServiceException {
    try {
      HRegion region = getRegion(request.getRegion());
      requestCount.incrementAndGet();
      Set<byte[]> columnFamilies = null;
      if (request.getFamilyCount() == 0) {
        columnFamilies = region.getStores().keySet();
      } else {
        columnFamilies = new HashSet<byte[]>();
        for (ByteString cf: request.getFamilyList()) {
          columnFamilies.add(cf.toByteArray());
        }
      }
      int nCF = columnFamilies.size();
      List<String>  fileList = region.getStoreFileList(
        columnFamilies.toArray(new byte[nCF][]));
      GetStoreFileResponse.Builder builder = GetStoreFileResponse.newBuilder();
      builder.addAllStoreFile(fileList);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority=HIGH_QOS)
  public GetOnlineRegionResponse getOnlineRegion(final RpcController controller,
      final GetOnlineRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      List<HRegionInfo> list = new ArrayList<HRegionInfo>(onlineRegions.size());
      for (Map.Entry<String,HRegion> e: this.onlineRegions.entrySet()) {
        list.add(e.getValue().getRegionInfo());
      }
      Collections.sort(list);
      GetOnlineRegionResponse.Builder builder = GetOnlineRegionResponse.newBuilder();
      for (HRegionInfo region: list) {
        builder.addRegionInfo(ProtobufUtil.toRegionInfo(region));
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }


  // Region open/close direct RPCs

  /**
   * Open a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HIGH_QOS)
  public OpenRegionResponse openRegion(final RpcController controller,
      final OpenRegionRequest request) throws ServiceException {
    int versionOfOfflineNode = -1;
    if (request.hasVersionOfOfflineNode()) {
      versionOfOfflineNode = request.getVersionOfOfflineNode();
    }
    try {
      checkOpen();
      requestCount.incrementAndGet();
      OpenRegionResponse.Builder
        builder = OpenRegionResponse.newBuilder();
      for (RegionInfo regionInfo: request.getRegionList()) {
        HRegionInfo region = ProtobufUtil.toRegionInfo(regionInfo);
        checkIfRegionInTransition(region, OPEN);

        HRegion onlineRegion = getFromOnlineRegions(region.getEncodedName());
        if (null != onlineRegion) {
          // See HBASE-5094. Cross check with META if still this RS is owning the
          // region.
          Pair<HRegionInfo, ServerName> p = MetaReader.getRegion(
            this.catalogTracker, region.getRegionName());
          if (this.getServerName().equals(p.getSecond())) {
            LOG.warn("Attempted open of " + region.getEncodedName()
              + " but already online on this server");
            builder.addOpeningState(RegionOpeningState.ALREADY_OPENED);
            continue;
          } else {
            LOG.warn("The region " + region.getEncodedName()
              + " is online on this server but META does not have this server.");
            removeFromOnlineRegions(region.getEncodedName());
          }
        }
        LOG.info("Received request to open region: " + region.getEncodedName());
        this.regionsInTransitionInRS.putIfAbsent(region.getEncodedNameAsBytes(), true);
        HTableDescriptor htd = this.tableDescriptors.get(region.getTableName());
        // Need to pass the expected version in the constructor.
        if (region.isRootRegion()) {
          this.service.submit(new OpenRootHandler(this, this, region, htd,
            versionOfOfflineNode));
        } else if (region.isMetaRegion()) {
          this.service.submit(new OpenMetaHandler(this, this, region, htd,
            versionOfOfflineNode));
        } else {
          this.service.submit(new OpenRegionHandler(this, this, region, htd,
            versionOfOfflineNode));
        }
        builder.addOpeningState(RegionOpeningState.OPENED);
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Close a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HIGH_QOS)
  public CloseRegionResponse closeRegion(final RpcController controller,
      final CloseRegionRequest request) throws ServiceException {
    int versionOfClosingNode = -1;
    if (request.hasVersionOfClosingNode()) {
      versionOfClosingNode = request.getVersionOfClosingNode();
    }
    boolean zk = request.getTransitionInZK();
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      CloseRegionResponse.Builder
        builder = CloseRegionResponse.newBuilder();
      LOG.info("Received close region: " + region.getRegionNameAsString() +
        ". Version of ZK closing node:" + versionOfClosingNode);
      HRegionInfo regionInfo = region.getRegionInfo();
      checkIfRegionInTransition(regionInfo, CLOSE);
      boolean closed = closeRegion(
        regionInfo, false, zk, versionOfClosingNode);
      builder.setClosed(closed);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Flush a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HIGH_QOS)
  public FlushRegionResponse flushRegion(final RpcController controller,
      final FlushRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Flushing " + region.getRegionNameAsString());
      boolean shouldFlush = true;
      if (request.hasIfOlderThanTs()) {
        shouldFlush = region.getLastFlushTime() < request.getIfOlderThanTs();
      }
      FlushRegionResponse.Builder builder = FlushRegionResponse.newBuilder();
      if (shouldFlush) {
        builder.setFlushed(region.flushcache());
      }
      builder.setLastFlushTime(region.getLastFlushTime());
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Split a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HIGH_QOS)
  public SplitRegionResponse splitRegion(final RpcController controller,
      final SplitRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Splitting " + region.getRegionNameAsString());
      region.flushcache();
      byte[] splitPoint = null;
      if (request.hasSplitPoint()) {
        splitPoint = request.getSplitPoint().toByteArray();
      }
      region.forceSplit(splitPoint);
      compactSplitThread.requestSplit(region, region.checkSplit());
      return SplitRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Compact a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HIGH_QOS)
  public CompactRegionResponse compactRegion(final RpcController controller,
      final CompactRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Compacting " + region.getRegionNameAsString());
      boolean major = false;
      if (request.hasMajor()) {
        major = request.getMajor();
      }
      if (major) {
        region.triggerMajorCompaction();
      }
      compactSplitThread.requestCompaction(region,
        "User-triggered " + (major ? "major " : "") + "compaction",
          CompactSplitThread.PRIORITY_USER);
      return CompactRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Replicate WAL entries on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HIGH_QOS)
  public ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
      final ReplicateWALEntryRequest request) throws ServiceException {
    try {
      if (replicationSinkHandler != null) {
        checkOpen();
        requestCount.incrementAndGet();
        HLog.Entry[] entries = ProtobufUtil.toHLogEntries(request.getEntryList());
        if (entries != null && entries.length > 0) {
          replicationSinkHandler.replicateLogEntries(entries);
        }
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Roll the WAL writer of the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public RollWALWriterResponse rollWALWriter(final RpcController controller,
      final RollWALWriterRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HLog wal = this.getWAL();
      byte[][] regionsToFlush = wal.rollWriter(true);
      RollWALWriterResponse.Builder builder = RollWALWriterResponse.newBuilder();
      if (regionsToFlush != null) {
        for (byte[] region: regionsToFlush) {
          builder.addRegionToFlush(ByteString.copyFrom(region));
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Stop the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public StopServerResponse stopServer(final RpcController controller,
      final StopServerRequest request) throws ServiceException {
    requestCount.incrementAndGet();
    String reason = request.getReason();
    stop(reason);
    return StopServerResponse.newBuilder().build();
  }

  /**
   * Get some information of the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public GetServerInfoResponse getServerInfo(final RpcController controller,
      final GetServerInfoRequest request) throws ServiceException {
    ServerName serverName = getServerName();
    requestCount.incrementAndGet();
    GetServerInfoResponse.Builder builder = GetServerInfoResponse.newBuilder();
    builder.setServerName(ProtobufUtil.toServerName(serverName));
    return builder.build();
  }

// End Admin methods

  /**
   * Find the HRegion based on a region specifier
   *
   * @param regionSpecifier the region specifier
   * @return the corresponding region
   * @throws IOException if the specifier is not null,
   *    but failed to find the region
   */
  protected HRegion getRegion(
      final RegionSpecifier regionSpecifier) throws IOException {
    byte[] value = regionSpecifier.getValue().toByteArray();
    RegionSpecifierType type = regionSpecifier.getType();
    checkOpen();
    switch (type) {
    case REGION_NAME:
      return getRegion(value);
    case ENCODED_REGION_NAME:
      String encodedRegionName = Bytes.toString(value);
      HRegion region = this.onlineRegions.get(encodedRegionName);
      if (region == null) {
        throw new NotServingRegionException(
          "Region is not online: " + encodedRegionName);
      }
      return region;
      default:
        throw new DoNotRetryIOException(
          "Unsupported region specifier type: " + type);
    }
  }

  /**
   * Execute an append mutation.
   *
   * @param region
   * @param mutate
   * @return
   * @throws IOException
   */
  protected Result append(final HRegion region,
      final Mutate mutate) throws IOException {
    Append append = ProtobufUtil.toAppend(mutate);
    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preAppend(append);
    }
    if (r == null) {
      Integer lock = getLockFromId(append.getLockId());
      r = region.append(append, lock, append.getWriteToWAL());
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postAppend(append, r);
      }
    }
    return r;
  }

  /**
   * Execute an increment mutation.
   *
   * @param region
   * @param mutate
   * @return
   * @throws IOException
   */
  protected Result increment(final HRegion region,
      final Mutate mutate) throws IOException {
    Increment increment = ProtobufUtil.toIncrement(mutate);
    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preIncrement(increment);
    }
    if (r == null) {
      Integer lock = getLockFromId(increment.getLockId());
      r = region.increment(increment, lock, increment.getWriteToWAL());
      if (region.getCoprocessorHost() != null) {
        r = region.getCoprocessorHost().postIncrement(increment, r);
      }
    }
    return r;
  }

  /**
   * Execute a list of put mutations.
   *
   * @param builder
   * @param region
   * @param puts
   */
  protected void put(final MultiResponse.Builder builder,
      final HRegion region, final List<Mutate> puts) {
    @SuppressWarnings("unchecked")
    Pair<Put, Integer>[] putsWithLocks = new Pair[puts.size()];

    try {
      ActionResult.Builder resultBuilder = ActionResult.newBuilder();
      NameBytesPair value = ProtobufUtil.toParameter(new Result());
      resultBuilder.setValue(value);
      ActionResult result = resultBuilder.build();

      int i = 0;
      for (Mutate put : puts) {
        Put p = ProtobufUtil.toPut(put);
        Integer lock = getLockFromId(p.getLockId());
        putsWithLocks[i++] = new Pair<Put, Integer>(p, lock);
        builder.addResult(result);
      }

      requestCount.addAndGet(puts.size());
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }

      OperationStatus codes[] = region.put(putsWithLocks);
      for (i = 0; i < codes.length; i++) {
        if (codes[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
          result = ResponseConverter.buildActionResult(
            new DoNotRetryIOException(codes[i].getExceptionMsg()));
          builder.setResult(i, result);
        }
      }
    } catch (IOException ie) {
      ActionResult result = ResponseConverter.buildActionResult(ie);
      for (int i = 0, n = puts.size(); i < n; i++) {
        builder.setResult(i, result);
      }
    }
  }

  /**
   * Mutate a list of rows atomically.
   *
   * @param region
   * @param mutates
   * @throws IOException
   */
  protected void mutateRows(final HRegion region,
      final List<Mutate> mutates) throws IOException {
    Mutate firstMutate = mutates.get(0);
    if (!region.getRegionInfo().isMetaTable()) {
      cacheFlusher.reclaimMemStoreMemory();
    }
    byte[] row = firstMutate.getRow().toByteArray();
    RowMutations rm = new RowMutations(row);
    for (Mutate mutate: mutates) {
      MutateType type = mutate.getMutateType();
      switch (mutate.getMutateType()) {
      case PUT:
        rm.add(ProtobufUtil.toPut(mutate));
        break;
      case DELETE:
        rm.add(ProtobufUtil.toDelete(mutate));
        break;
        default:
          throw new DoNotRetryIOException(
            "mutate supports atomic put and/or delete, not "
              + type.name());
      }
    }
    region.mutateRow(rm);
  }
}
