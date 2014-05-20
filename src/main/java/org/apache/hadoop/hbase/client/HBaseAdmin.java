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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.base.Preconditions;


/**
 * Provides an interface to manage HBase database table metadata + general
 * administrative functions.  Use HBaseAdmin to create, drop, list, enable and
 * disable tables. Use it also to add and drop table column families.
 *
 * See {@link HTable} to add, update, and delete data from an individual table.
 */
public class HBaseAdmin {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  final HConnection connection;
  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  private volatile HMasterInterface master;

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   */
  public HBaseAdmin(Configuration conf) throws MasterNotRunningException {
    this.connection = HConnectionManager.getConnection(conf);
    this.conf = conf;
    // use 30s pause time between retries instead of the small default
    // pause time. Shouldn't the RPC read timeout be also increased?
    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE, 30 * 1000);
    this.numRetries = conf.getInt("hbase.client.retries.number", 5);
    this.master = connection.getMaster();
  }

  public void enableLoadBalancer() throws MasterNotRunningException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    master.enableLoadBalancer();
  }

  public void disableLoadBalancer() throws MasterNotRunningException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    this.master.disableLoadBalancer();
  }

  public boolean isLoadBalancerDisabled() throws MasterNotRunningException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    return this.master.isLoadBalancerDisabled();
  }

  /** @return HConnection used by this object. */
  public HConnection getConnection() {
    return connection;
  }

  /**
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException if the master is not running
   */
  public HMasterInterface getMaster() throws MasterNotRunningException{
    return this.connection.getMaster();
  }

  /** @return - true if the master server is running */
  public boolean isMasterRunning() {
    return this.connection.isMasterRunning();
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws MasterNotRunningException if the master is not running
   */
  public boolean tableExists(final String tableName)
  throws MasterNotRunningException {
    return tableExists(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws MasterNotRunningException if the master is not running
   */
  public boolean tableExists(final byte [] tableName)
  throws MasterNotRunningException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    return connection.tableExists(new StringBytes(tableName));
  }

  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   *
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor[] listTables() throws IOException {
    return this.connection.listTables();
  }


  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final byte [] tableName)
  throws IOException {
    return this.connection.getHTableDescriptor(new StringBytes(tableName));
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length)
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  /**
   * Creates a new table.
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    createTable(desc, null);
  }

  /**
   * Creates a new table with the specified number of regions.  The start key
   * specified will become the end key of the first region of the table, and
   * the end key specified will become the start key of the last region of the
   * table (the first region has a null start key and the last region has a
   * null end key).
   *
   * BigInteger math will be used to divide the key range specified into
   * enough segments to make the required number of total regions.
   *
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [] startKey,
      byte [] endKey, int numRegions)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    if(numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if(Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if(splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys);
  }

  /**
   * Creates a new table with an initial set of empty regions defined by the
   * specified split keys.  The total number of regions created will be the
   * number of split keys plus one (the first region has a null start key and
   * the last region has a null end key).
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(final HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    checkSplitKeys(splitKeys);
    createTableAsync(desc, splitKeys);
    checkTableOnline(desc, splitKeys);
  }

  private void checkTableOnline(final HTableDescriptor desc, byte[][] splitKeys)
      throws IOException, RegionOfflineException, InterruptedIOException {
    long startTime = System.currentTimeMillis();
    int numRegs = splitKeys == null ? 1 : splitKeys.length + 1;
    int prevRegCount = 0;
    for (int tries = 0; tries < numRetries; ++tries) {
      // Wait for new table to come on-line
      final AtomicInteger actualRegCount = new AtomicInteger(0);
      MetaScannerVisitor visitor = new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result rowResult) throws IOException {
          HRegionInfo info = Writables.getHRegionInfo(
              rowResult.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.REGIONINFO_QUALIFIER));
          if (!(Bytes.equals(info.getTableDesc().getName(), desc.getName()))) {
            return false;
          }
          String hostAndPort = null;
          byte [] value = rowResult.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          // Make sure that regions are assigned to server
          if (value != null && value.length > 0) {
            hostAndPort = Bytes.toString(value);
          }
          if (!(info.isOffline() || info.isSplit()) && hostAndPort != null) {
            actualRegCount.incrementAndGet();
          }
          return true;
        }
      };
      MetaScanner.metaScan(conf, visitor, new StringBytes(desc.getName()));
      if (actualRegCount.get() == numRegs) {
        LOG.debug("Table " + desc.getNameAsString() + " is created after "
            + "waiting for " + (System.currentTimeMillis() - startTime) + "ms");
        return;
      }

      if (tries == numRetries - 1) {
        throw new RegionOfflineException("Only " + actualRegCount.get()
            + " of " + numRegs + " regions are online; retries exhausted "
            + "after waiting for " + (System.currentTimeMillis() - startTime)
            + "ms.");
      }
      try { // Sleep
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted when opening"
            + " regions; " + actualRegCount.get() + " of " + numRegs
            + " regions processed so far");
      }
      if (actualRegCount.get() > prevRegCount) { // Making progress
        prevRegCount = actualRegCount.get();
        tries = -1;
      }
    }
    Preconditions.checkArgument(tableExists(desc.getName()));
  }

  /**
   * Sorts and checks the splitKeys.
   * @param splitKeys
   */
  public void checkSplitKeys(byte[][] splitKeys) {
    if (splitKeys != null && splitKeys.length > 1) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (Bytes.equals(splitKey, HConstants.EMPTY_BYTE_ARRAY)) {
          throw new IllegalArgumentException(
            "Split keys cannot be empty"
          );
        }
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException(
              "All split keys must be unique, found duplicate");
        }
        lastKey = splitKey;
      }
    }
  }

  /**
   * Creates a new table but does not block and wait for it to come online.
   * Asynchronous operation.
   *
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException Bad table name.
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTableAsync(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(desc.getName());
    try {
      this.master.createTable(desc, splitKeys);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    } catch (SocketTimeoutException ste) {
      LOG.warn("Creating " + desc.getNameAsString() + " took too long", ste);
    }
  }


  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(tableName);
    HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
    try {
      this.master.deleteTable(tableName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
    final int batchCount = this.conf.getInt("hbase.admin.scanner.caching", 10);
    // Wait until first region is deleted
    HRegionInterface server =
      connection.getHRegionConnection(firstMetaServer.getServerAddress());
    HRegionInfo info = new HRegionInfo();
    for (int tries = 0; tries < numRetries; tries++) {
      long scannerId = -1L;
      try {
        Scan scan = new Scan().addColumn(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
        scannerId = server.openScanner(
          firstMetaServer.getRegionInfo().getRegionName(), scan);
        // Get a batch at a time.
        Result[] values = server.next(scannerId, batchCount);
        if (values == null || values.length == 0) {
          break;
        }
        boolean found = false;
        for (Result r : values) {
          NavigableMap<byte[], byte[]> infoValues =
              r.getFamilyMap(HConstants.CATALOG_FAMILY);
          for (Map.Entry<byte[], byte[]> e : infoValues.entrySet()) {
            if (Bytes.equals(e.getKey(), HConstants.REGIONINFO_QUALIFIER)) {
              info = (HRegionInfo) Writables.getWritable(e.getValue(), info);
              if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
                found = true;
              } else {
                found = false;
                break;
              }
            }
          }
        }
        if (!found) {
          break;
        }
      } catch (IOException ex) {
        if(tries == numRetries - 1) {           // no more tries left
          if (ex instanceof RemoteException) {
            ex = RemoteExceptionHandler.decodeRemoteException((RemoteException) ex);
          }
          throw ex;
        }
      } finally {
        if (scannerId != -1L) {
          try {
            server.close(scannerId);
          } catch (Exception ex) {
            LOG.warn(ex);
          }
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }
    // Delete cached information to prevent clients from using old locations
    HConnectionManager.deleteConnectionInfo(conf, false);
    LOG.info("Deleted " + Bytes.toStringBinary(tableName));
  }



  /**
   * Brings a table on-line (enables it).
   * Synchronous operation.
   *
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void enableTable(final String tableName) throws IOException {
    enableTable(Bytes.toBytes(tableName));
  }

  /**
   * Brings a table on-line (enables it).
   * Synchronous operation.
   *
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void enableTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }

    // Wait until all regions are enabled
    boolean enabled = false;
    for (int tries = 0; tries < this.numRetries; tries++) {

      try {
        this.master.enableTable(tableName);
      } catch (RemoteException e) {
        throw RemoteExceptionHandler.decodeRemoteException(e);
      }
      enabled = isTableEnabled(tableName);
      if (enabled) break;
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
          "enabled in " + Bytes.toStringBinary(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // continue
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for all regions to be enabled from " +
          Bytes.toStringBinary(tableName));
      }
    }
    if (!enabled)
      throw new IOException("Unable to enable table " +
        Bytes.toStringBinary(tableName));
    LOG.info("Enabled table " + Bytes.toStringBinary(tableName));
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * Synchronous operation.
   *
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   */
  public void disableTable(final String tableName) throws IOException {
    disableTable(Bytes.toBytes(tableName));
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * Synchronous operation.
   *
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   */
  public void disableTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }

    // Wait until all regions are disabled
    boolean disabled = false;
    for (int tries = 0; tries < this.numRetries; tries++) {
      try {
        this.master.disableTable(tableName);
      } catch (RemoteException e) {
        throw RemoteExceptionHandler.decodeRemoteException(e);
      }
      disabled = isTableDisabled(tableName);
      if (disabled) break;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleep. Waiting for all regions to be disabled from " +
          Bytes.toStringBinary(tableName));
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for all regions to be disabled from " +
          Bytes.toStringBinary(tableName));
      }
    }
    if (!disabled) {
      throw new RegionException("Retries exhausted, it took too long to wait"+
        " for the table " + Bytes.toStringBinary(tableName) + " to be disabled.");
    }
    LOG.info("Disabled " + Bytes.toStringBinary(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return connection.isTableEnabled(new StringBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(StringBytes tableName) throws IOException {
    return isTableEnabled(tableName.getBytes());
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return connection.isTableDisabled(new StringBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return connection.isTableAvailable(new StringBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return connection.isTableAvailable(new StringBytes(tableName));
  }

  /**
   * Batch alter a table. Only takes regions offline once and performs a single
   * update to .META.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param columnAdditions column descriptors to add to the table
   * @param columnModifications pairs of column names with new descriptors
   * @param columnDeletions column names to delete from the table
   * @throws IOException if a remote or network exception occurs
   */
  public void alterTable(final String tableName,
      List<HColumnDescriptor> columnAdditions,
      List<Pair<String, HColumnDescriptor>> columnModifications,
      List<String> columnDeletions) throws IOException {

    //Use default values since none were specified
    int waitInterval = conf.getInt(HConstants.MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS,
        HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS);

    int maxClosedRegions = conf.getInt(HConstants.MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE,
        HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE);

    alterTable(tableName, columnAdditions, columnModifications,
        columnDeletions, waitInterval, maxClosedRegions);
  }

  /**
   * Batch alter a table. Only takes regions offline once and performs a single
   * update to .META.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param columnAdditions column descriptors to add to the table
   * @param columnModifications pairs of column names with new descriptors
   * @param columnDeletions column names to delete from the table
   * @param waitInterval the interval of time to spread the close of
   *                     numConcurrentRegionsClosed over
   * @param maxConcurrentRegionsClosed the max number of regions to have closed
   *                                   at a time.
   * @throws IOException if a remote or network exception occurs
   */
  public void alterTable(final String tableName,
                         List<HColumnDescriptor> columnAdditions,
                         List<Pair<String, HColumnDescriptor>> columnModifications,
                         List<String> columnDeletions,
                         int waitInterval,
                         int maxConcurrentRegionsClosed) throws IOException {
    // convert all of the strings to bytes and pass to the bytes method
    List<Pair<byte [], HColumnDescriptor>> modificationsBytes =
        new ArrayList<Pair<byte [], HColumnDescriptor>>(
            columnModifications.size());
    List<byte []> deletionsBytes =
        new ArrayList<byte []>(columnDeletions.size());

    for(Pair<String, HColumnDescriptor> c : columnModifications) {
      modificationsBytes.add(new Pair<byte [], HColumnDescriptor>(
          Bytes.toBytes(c.getFirst()), c.getSecond()));
    }
    for(String c : columnDeletions) {
      deletionsBytes.add(Bytes.toBytes(c));
    }

    alterTable(Bytes.toBytes(tableName), columnAdditions, modificationsBytes,
        deletionsBytes, waitInterval, maxConcurrentRegionsClosed);
  }

  /**
   * Batch alter a table. Only takes regions offline once and performs a single
   * update to .META.
   * Any of the three lists can be null, in which case those types of
   * alterations will be ignored.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param columnAdditions column descriptors to add to the table
   * @param columnModifications pairs of column names with new descriptors
   * @param columnDeletions column names to delete from the table
   * @throws IOException if a remote or network exception occurs
   */
  public void alterTable(final byte [] tableName,
      List<HColumnDescriptor> columnAdditions,
      List<Pair<byte[], HColumnDescriptor>> columnModifications,
      List<byte[]> columnDeletions) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    int waitInterval = conf.getInt(HConstants.MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS,
        HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS);

    int maxClosedRegions = conf.getInt(HConstants.MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE,
        HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE);

    alterTable(tableName, columnAdditions, columnModifications, columnDeletions, waitInterval, maxClosedRegions);
  }

  /**
   * Batch alter a table. Only takes regions offline once and performs a single
   * update to .META.
   * Any of the three lists can be null, in which case those types of
   * alterations will be ignored.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param columnAdditions column descriptors to add to the table
   * @param columnModifications pairs of column names with new descriptors
   * @param columnDeletions column names to delete from the table
   * @param waitInterval the interval of time to spread the close of
   *                     numConcurrentRegionsClosed over
   * @param maxConcurrentRegionsClosed the max number of regions to have closed
   *                                   at a time.
   * @throws IOException if a remote or network exception occurs
   */
  public void alterTable(final byte [] tableName,
                         List<HColumnDescriptor> columnAdditions,
                         List<Pair<byte[], HColumnDescriptor>> columnModifications,
                         List<byte[]> columnDeletions,
                         int waitInterval,
                         int maxConcurrentRegionsClosed) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(tableName);
    try {
      this.master.alterTable(tableName, columnAdditions, columnModifications,
          columnDeletions, waitInterval, maxConcurrentRegionsClosed);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Get the status of alter command - indicates how many regions have received
   * the updated schema Asynchronous operation.
   *
   * @param tableName
   *          name of the table to get the status of
   * @return List indicating the number of regions updated List.get(0) is the
   *         regions that are yet to be updated List.get(1) is the total number
   *         of regions of the table
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName)
      throws IOException {
    HTableDescriptor.isLegalTableName(tableName);
    try {
      return this.master.getAlterStatus(tableName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final String tableName, HColumnDescriptor column)
  throws IOException {
    alterTable(Bytes.toBytes(tableName), Arrays.asList(column), null, null);
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException {
    alterTable(tableName, Arrays.asList(column), null, null);
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final String tableName, final String columnName)
  throws IOException {
    alterTable(Bytes.toBytes(tableName), null, null,
        Arrays.asList(Bytes.toBytes(columnName)));
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final byte [] tableName,
      final byte [] columnName)
  throws IOException {
    alterTable(tableName, null, null, Arrays.asList(columnName));
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final String tableName, final String columnName,
      HColumnDescriptor descriptor)
  throws IOException {
    alterTable(Bytes.toBytes(tableName), null, Arrays.asList(
          new Pair<byte [], HColumnDescriptor>(Bytes.toBytes(columnName),
            descriptor)), null);
  }

  public void modifyColumn(final String tableName,
      HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(tableName, descriptor.getNameAsString(), descriptor);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final byte [] tableName, final byte [] columnName,
    HColumnDescriptor descriptor)
  throws IOException {
    alterTable(tableName, null, Arrays.asList(
          new Pair<byte [], HColumnDescriptor>(columnName, descriptor)), null);
  }

  /**
   * Close a region. For expert-admins.
   * Asynchronous operation.
   *
   * @param regionname region name to close
   * @param args Optional server name.  Otherwise, we'll send close to the
   * server registered in .META.
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final String regionname, final Object... args)
  throws IOException {
    closeRegion(Bytes.toBytes(regionname), args);
  }

  /**
   * Close a region.  For expert-admins.
   * Asynchronous operation.
   *
   * @param regionname region name to close
   * @param args Optional server name.  Otherwise, we'll send close to the
   * server registered in .META.
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final byte [] regionname, final Object... args)
  throws IOException {
    // Be careful. Must match the handler over in HMaster at MODIFY_CLOSE_REGION
    int len = (args == null)? 0: args.length;
    int xtraArgsCount = 1;
    Object [] newargs = new Object[len + xtraArgsCount];
    newargs[0] = regionname;
    if(args != null) {
      System.arraycopy(args, 0, newargs, xtraArgsCount, len);
    }
    modifyTable(HConstants.META_TABLE_NAME, HConstants.Modify.CLOSE_REGION,
      newargs);
  }

  /**
   * Move a region. For expert-admins.
   * Asynchronous operation. Region will be be assigned to the specified
   * preferred host, then closed.
   *
   * @param regionName region name to move
   * @param regionServer the "hostname:port" of the region server to which to
   * move the region
   * @throws IOException if a remote or network exception occurs
   */
  public void moveRegion(final String regionName, final String regionServer)
  throws IOException {
    moveRegion(Bytes.toBytes(regionName), regionServer);
  }

  /**
   * Move a region. For expert-admins.
   * Asynchronous operation. Region will be be assigned to the specified
   * preferred host, then closed.
   *
   * @param regionName region name to move
   * @param regionServer the "hostname:port" of the region server to which to
   * move the region
   * @throws IOException if a remote or network exception occurs
   */
  public void moveRegion(final byte[] regionName, final String regionServer)
  throws IOException {
    modifyTable(HConstants.META_TABLE_NAME, HConstants.Modify.MOVE_REGION,
      new Object[]{regionName, regionServer});
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   */
  public void flush(final String tableNameOrRegionName) throws IOException {
    flush(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   */
  public void flush(final byte [] tableNameOrRegionName) throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_FLUSH);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void compact(final String tableNameOrRegionName) throws IOException {
    compact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void compact(final byte [] tableNameOrRegionName) throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_COMPACT);
  }

  /**
   * Compact a column family within a table.
   * Asynchronous operation.
   *
   * @param tableName region to compact
   * @param columnFamily column family within the region to compact
   * @throws IOException if a remote or network exception occurs
   */
  private void compactCF(String tableName, String columnFamily, HConstants.Modify op)
    throws IOException {
    compactCF(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily), op);
  }

  /**
   * Compact a column family within a table.
   * Asynchronous operation.
   *
   * @param tableName region to compact
   * @param columnFamily column family within the region to compact
   * @throws IOException if a remote or network exception occurs
   */
  private void compactCF(final byte[] tableName, final byte[] columnFamily, HConstants.Modify op)
    throws IOException {
    // Validate table name and column family.
    if (!this.connection.tableExists(new StringBytes(tableName))) {
      throw new IllegalArgumentException("HTable " + new StringBytes(tableName)
          + " does not exist");
    } else if (!getTableDescriptor(tableName).hasFamily(columnFamily)) {
      throw new IllegalArgumentException("Column Family "
          + new String(columnFamily) + " does not exist in "
          + new StringBytes(tableName));
    }

    // Get all regions for this table.
    HTable table = new HTable(this.conf, tableName);
    Set <HRegionInfo> regions = table.getRegionsInfo().keySet();
    Iterator <HRegionInfo> regionsIt = regions.iterator();

    // Iterate over all regions and send a compaction request to each.
    while (regionsIt.hasNext()) {
      byte[] regionName = regionsIt.next().getRegionName();
      modifyTable(null, op, new Object[] {regionName, columnFamily});
    }
  }

  /**
   * Compact a column family within a region.
   * Asynchronous operation.
   *
   * @param tableOrRegionName region to compact
   * @param columnFamily column family within the region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void compact(String tableOrRegionName, String columnFamily)
    throws IOException {
    if (tableExists(tableOrRegionName)) {
      compactCF(tableOrRegionName, columnFamily, HConstants.Modify.TABLE_COMPACT);
      return;
    }
    // Validate column family.
    byte[] tableName = HRegionInfo.parseRegionName(Bytes.toBytes(tableOrRegionName))[0];
    if (!getTableDescriptor(tableName).hasFamily(Bytes.toBytes(columnFamily))) {
      throw new IllegalArgumentException("Column Family " + columnFamily +
          " does not exist in table " + new String(tableName));
    }
    compact(Bytes.toBytes(tableOrRegionName), Bytes.toBytes(columnFamily));
  }

  /**
   * Compact a column family within a region.
   * Asynchronous operation.
   *
   * @param tableOrRegionName region to compact
   * @param columnFamily column family within the region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void compact(final byte[] tableOrRegionName, final byte[] columnFamily)
    throws IOException {
    if (tableExists(tableOrRegionName)) {
      compactCF(tableOrRegionName, columnFamily, HConstants.Modify.TABLE_COMPACT);
      return;
    }
    @SuppressWarnings("unused")
    byte[] tableName = HRegionInfo.parseRegionName(tableOrRegionName)[0];
    // Perform compaction only if a valid column family was passed.
    modifyTable(null, HConstants.Modify.TABLE_COMPACT,
        new Object[] {tableOrRegionName, columnFamily});
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   */
  public void majorCompact(final String tableNameOrRegionName)
  throws IOException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   */
  public void majorCompact(final byte [] tableNameOrRegionName)
  throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_MAJOR_COMPACT);
  }

  /**
   * Major compacts a column family within a region or table.
   * Asynchronous operation.
   *
   * @param tableOrRegionName region to compact
   * @param columnFamily column family within the region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void majorCompact(String tableOrRegionName, String columnFamily)
    throws IOException {
    if (tableExists(tableOrRegionName)) {
      compactCF(tableOrRegionName, columnFamily,
          HConstants.Modify.TABLE_MAJOR_COMPACT);
      return;
    }
    byte[] tableName = HRegionInfo.parseRegionName(Bytes.toBytes(tableOrRegionName))[0];
    if (!getTableDescriptor(tableName).hasFamily(Bytes.toBytes(columnFamily))) {
      throw new IllegalArgumentException("Column Family " + columnFamily +
          " does not exist in table " + new String(tableName));
    }
    majorCompact(Bytes.toBytes(tableOrRegionName), Bytes.toBytes(columnFamily));
  }

  /**
   * Major compacts a column family within a region or table.
   * Asynchronous operation.
   *
   * @param tableOrRegionName region to compact
   * @param columnFamily column family within the region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void majorCompact(final byte[] tableOrRegionName, final byte[] columnFamily)
    throws IOException {
    if (tableExists(tableOrRegionName)) {
      compactCF(tableOrRegionName, columnFamily,
          HConstants.Modify.TABLE_MAJOR_COMPACT);
      return;
    }
    modifyTable(null, HConstants.Modify.TABLE_MAJOR_COMPACT,
        new Object[] {tableOrRegionName, columnFamily});
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to split
   * @throws IOException if a remote or network exception occurs
   */
  public void split(final String tableNameOrRegionName) throws IOException {
    split(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Split a table or an individual region.  Implicitly finds an optimal split
   * point.  Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @throws IOException if a remote or network exception occurs
   */
  public void split(final byte [] tableNameOrRegionName) throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_SPLIT);
  }

  public void split(final String tableNameOrRegionName,
    final String splitPoint) throws IOException {
    split(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(splitPoint));
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   */
  public void split(final byte [] tableNameOrRegionName,
      final byte [] splitPoint) throws IOException {
    if (tableNameOrRegionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    byte [] tableName = tableExists(tableNameOrRegionName)?
      tableNameOrRegionName: null;
    byte [] regionName = tableName == null? tableNameOrRegionName: null;
    Object [] args = regionName == null?
      new byte [][] {splitPoint}: new byte [][] {regionName, splitPoint};
    modifyTable(tableName, HConstants.Modify.TABLE_EXPLICIT_SPLIT, args);
  }

  /*
   * Call modifyTable using passed tableName or region name String.  If no
   * such table, presume we have been passed a region name.
   * @param tableNameOrRegionName
   * @param op
   * @throws IOException
   */
  private void modifyTable(final byte [] tableNameOrRegionName,
      final HConstants.Modify op)
  throws IOException {
    if (tableNameOrRegionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    if (tableExists(tableNameOrRegionName)) {
      modifyTable(tableNameOrRegionName, op, (Object[])null);
    } else {
      HRegionInfo.parseRegionName(tableNameOrRegionName); // verify format
      modifyTable(null, op, new Object[] {tableNameOrRegionName});
    }
  }

  /**
   * Modify an existing table, more IRB friendly version.
   * Asynchronous operation.
   *
   * @param tableName name of table.
   * @param htd modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(final byte [] tableName, HTableDescriptor htd)
  throws IOException {
    modifyTable(tableName, HConstants.Modify.TABLE_SET_HTD, htd);
  }

  /**
   * Modify an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of table.  May be null if we are operating on a
   * region.
   * @param op table modification operation
   * @param args operation specific arguments
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(final byte [] tableName, HConstants.Modify op,
      Object... args)
      throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    // Let pass if its a catalog table.  Used by admins.
    if (tableName != null && !MetaUtils.isMetaTableName(tableName)) {
      // This will throw exception
      HTableDescriptor.isLegalTableName(tableName);
    }
    Writable[] arr = null;
    try {
      switch (op) {
      case TABLE_SET_HTD:
        if (args == null || args.length < 1 ||
            !(args[0] instanceof HTableDescriptor)) {
          throw new IllegalArgumentException("SET_HTD requires a HTableDescriptor");
        }
        arr = new Writable[1];
        arr[0] = (HTableDescriptor)args[0];
        this.master.modifyTable(tableName, op, arr);
        break;

      case TABLE_COMPACT:
      case TABLE_SPLIT:
      case TABLE_EXPLICIT_SPLIT:
      case TABLE_MAJOR_COMPACT:
      case TABLE_FLUSH:
        if (args != null && args.length > 0) {
          arr = new Writable[args.length];
          for (int i = 0; i < args.length; i++) {
            arr[i] = toWritable(args[i]);
          }
        }
        this.master.modifyTable(tableName, op, arr);
        break;

      case MOVE_REGION:
        if (args == null || args.length < 2) {
          throw new IllegalArgumentException("Requires at least a region name and hostname");
        }
      case CLOSE_REGION:
        if (args == null || args.length < 1) {
          throw new IllegalArgumentException("Requires at least a region name");
        }
        arr = new Writable[args.length];
        for (int i = 0; i < args.length; i++) {
          arr[i] = toWritable(args[i]);
        }
        this.master.modifyTable(tableName, op, arr);
        break;

      default:
        throw new IOException("unknown modifyTable op " + op);
      }
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  private static Writable toWritable(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof byte[]) {
      return new ImmutableBytesWritable((byte[])o);
    } else if (o instanceof ImmutableBytesWritable) {
      return (ImmutableBytesWritable)o;
    } else if (o instanceof String) {
      return new ImmutableBytesWritable(Bytes.toBytes((String)o));
    } else if (o instanceof Boolean) {
      return new BooleanWritable((Boolean) o);
    } else {
      throw new IllegalArgumentException("Requires byte [] or " +
        "ImmutableBytesWritable, not " + o.getClass() + " : " + o);
    }
  }

  /**
   * Shuts down the HBase instance
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void shutdown() throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    try {
      // Shutdown the whole HBase cluster.
      this.master.shutdown();
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    } finally {
      this.master = null;
    }
  }

  /**
   * Stop the designated RegionServer for a restart.
   *
   * @param hsa
   *          the address of the RegionServer to stop
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public synchronized void stopRegionServerForRestart(final HServerAddress hsa)
      throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
      LOG.info("Restarting RegionServer" + hsa.toString());
      rs.stopForRestart();
  }

  /**
   * Stop the designated RegionServer for a stop.
   *
   * @param hsa
   *          the address of the RegionServer to stop
   * @para message
   *          the reason to stop the RegionServer
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public synchronized void stopRegionServer(final HServerAddress hsa, String reason)
      throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
    LOG.info("Stopping RegionServer" + hsa.toString() + " because " + reason);
    rs.stop(reason);
  }

  /**
   * Set the number of threads to be used for HDFS quorum reads.
   *
   * @param hsa
   *          the address of the RegionServer to stop
   * @param numThreads
   *          the number of threads to be used for HDFS quorum reads
   *          <= 0 will disable quorum Reads.
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public synchronized void setNumHDFSQuorumReadThreads(final HServerAddress hsa,
      int numThreads) throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
      LOG.info("Setting numHDFSQuorumReadThreads for RegionServer" + hsa.toString()
          + " to " + numThreads);
      rs.setNumHDFSQuorumReadThreads(numThreads);
  }

  /**
   * Set the number of threads to be used for HDFS quorum reads.
   *
   * @param hsa
   *          the address of the RegionServer to stop
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public synchronized void setHDFSQuorumReadTimeoutMillis(final HServerAddress hsa,
      long timeoutMillis) throws IOException {
    HRegionInterface rs = this.connection.getHRegionConnection(hsa);
      LOG.info("Setting quorumReadTimeout for RegionServer" + hsa.toString()
          + " to " + timeoutMillis);
      rs.setHDFSQuorumReadTimeoutMillis(timeoutMillis);
  }

  /**
   * @return cluster status
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    return this.master.getClusterStatus();
  }

  private HRegionLocation getFirstMetaServerForTable(final byte [] tableName)
  throws IOException {
    return connection.locateRegion(HConstants.META_TABLE_NAME_STRINGBYTES,
      HRegionInfo.createRegionName(tableName, null, HConstants.NINES, false));
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   *
   * @param conf system configuration
   * @throws MasterNotRunningException if a remote or network exception occurs
   */
  public static void checkHBaseAvailable(Configuration conf)
  throws MasterNotRunningException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);
    copyOfConf.setInt("hbase.client.retries.number", 1);
    new HBaseAdmin(copyOfConf);
  }

  public void close() throws IOException {
    if (this.connection != null) {
      connection.close();
    }
  }

  // Update configuration for all region servers
  public void updateConfiguration() throws IOException {
    Collection<HServerInfo> allRegionServers = this.getClusterStatus().getServerInfo();
    for (HServerInfo serverInfo : allRegionServers) {
      updateConfiguration(serverInfo.getServerAddress());
    }
  }

  // Update configuration for region server at this address
  public void updateConfiguration(String hostNameWithPort) throws IOException {
    updateConfiguration(new HServerAddress(hostNameWithPort));
  }

  // Update configuration for region server at this address.
  public void updateConfiguration(HServerAddress address) throws IOException {
    HRegionInterface server = connection.getHRegionConnection(address);
    server.updateConfiguration();
  }

}
