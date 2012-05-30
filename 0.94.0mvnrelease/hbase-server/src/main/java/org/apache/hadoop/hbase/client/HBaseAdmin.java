/**
 * Copyright 2011 The Apache Software Foundation
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
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

/**
 * Provides an interface to manage HBase database table metadata + general
 * administrative functions.  Use HBaseAdmin to create, drop, list, enable and
 * disable tables. Use it also to add and drop table column families.
 *
 * <p>See {@link HTable} to add, update, and delete data from an individual table.
 * <p>Currently HBaseAdmin instances are not expected to be long-lived.  For
 * example, an HBaseAdmin instance will not ride over a Master restart.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HBaseAdmin implements Abortable, Closeable {
  private static final Log LOG = LogFactory.getLog(HBaseAdmin.class);

  // We use the implementation class rather then the interface because we
  //  need the package protected functions to get the connection to master
  private HConnectionManager.HConnectionImplementation connection;

  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Multiply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;
  private boolean aborted;

  /**
   * Constructor.
   * See {@link #HBaseAdmin(HConnection connection)}
   *
   * @param c Configuration object. Copied internally.
   */
  public HBaseAdmin(Configuration c)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    // Will not leak connections, as the new implementation of the constructor
    // does not throw exceptions anymore.
    this(HConnectionManager.getConnection(new Configuration(c)));
  }

 /**
  * Constructor for externally managed HConnections.
  * The connection to master will be created when required by admin functions.
  *
  * @param connection The HConnection instance to use
  * @throws MasterNotRunningException, ZooKeeperConnectionException are not
  *  thrown anymore but kept into the interface for backward api compatibility
  */
  public HBaseAdmin(HConnection connection)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    this.conf = connection.getConfiguration();

    // We want the real class, without showing it our public interface,
    //  hence the cast.
    this.connection = (HConnectionManager.HConnectionImplementation)connection;

    this.pause = this.conf.getLong("hbase.client.pause", 1000);
    this.numRetries = this.conf.getInt("hbase.client.retries.number", 10);
    this.retryLongerMultiplier = this.conf.getInt(
        "hbase.client.retries.longer.multiplier", 10);
  }

  /**
   * @return A new CatalogTracker instance; call {@link #cleanupCatalogTracker(CatalogTracker)}
   * to cleanup the returned catalog tracker.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   * @see #cleanupCatalogTracker(CatalogTracker)
   */
  private synchronized CatalogTracker getCatalogTracker()
  throws ZooKeeperConnectionException, IOException {
    CatalogTracker ct = null;
    try {
      ct = new CatalogTracker(this.conf);
      ct.start();
    } catch (InterruptedException e) {
      // Let it out as an IOE for now until we redo all so tolerate IEs
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted", e);
    }
    return ct;
  }

  private void cleanupCatalogTracker(final CatalogTracker ct) {
    ct.stop();
  }

  @Override
  public void abort(String why, Throwable e) {
    // Currently does nothing but throw the passed message and exception
    this.aborted = true;
    throw new RuntimeException(why, e);
  }
  
  @Override
  public boolean isAborted(){
    return this.aborted;
  }

  /** @return HConnection used by this object. */
  public HConnection getConnection() {
    return connection;
  }

  /**
   * Get a connection to the currently set master.
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   * @deprecated  Master is an implementation detail for HBaseAdmin.
   * Deprecated in HBase 0.94
   */
  @Deprecated
  public HMasterInterface getMaster()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    // We take a shared master, but we will never release it,
    //  so we will have the same behavior as before.
    return this.connection.getKeepAliveMaster();
  }

  /** @return - true if the master server is running. Throws an exception
   *  otherwise.
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   */
   public boolean isMasterRunning()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return connection.isMasterRunning();
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(final String tableName)
  throws IOException {
    boolean b = false;
    CatalogTracker ct = getCatalogTracker();
    try {
      b = MetaReader.tableExists(ct, tableName);
    } finally {
      cleanupCatalogTracker(ct);
    }
    return b;
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(final byte [] tableName)
  throws IOException {
    return tableExists(Bytes.toString(tableName));
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
   * List all the userspace tables matching the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables()
   */
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> matched = new LinkedList<HTableDescriptor>();
    HTableDescriptor[] tables = listTables();
    for (HTableDescriptor table : tables) {
      if (pattern.matcher(table.getNameAsString()).matches()) {
        matched.add(table);
      }
    }
    return matched.toArray(new HTableDescriptor[matched.size()]);
  }

  /**
   * List all the userspace tables matching the given regular expression.
   *
   * @param regex The regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables(java.util.regex.Pattern)
   */
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex));
  }


  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws TableNotFoundException
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final byte [] tableName)
  throws TableNotFoundException, IOException {
    return this.connection.getHTableDescriptor(tableName);
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
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
   * number of split keys plus one. Synchronous operation.
   * Note : Avoid passing empty split key.
   *
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   *
   * @throws IllegalArgumentException if the table name is reserved, if the split keys
   * are repeated and if the split key has empty byte array.
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(final HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    try {
      createTableAsync(desc, splitKeys);
    } catch (SocketTimeoutException ste) {
      LOG.warn("Creating " + desc.getNameAsString() + " took too long", ste);
    }
    int numRegs = splitKeys == null ? 1 : splitKeys.length + 1;
    int prevRegCount = 0;
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier;
      ++tries) {
      // Wait for new table to come on-line
      final AtomicInteger actualRegCount = new AtomicInteger(0);
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result rowResult) throws IOException {
          HRegionInfo info = Writables.getHRegionInfoOrNull(
              rowResult.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.REGIONINFO_QUALIFIER));
          //If regioninfo is null, skip this row
          if (null == info) {
            return true;
          }
          if (!(Bytes.equals(info.getTableName(), desc.getName()))) {
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
      MetaScanner.metaScan(conf, visitor, desc.getName());
      if (actualRegCount.get() != numRegs) {
        if (tries == this.numRetries * this.retryLongerMultiplier - 1) {
          throw new RegionOfflineException("Only " + actualRegCount.get() +
            " of " + numRegs + " regions are online; retries exhausted.");
        }
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when opening" +
              " regions; " + actualRegCount.get() + " of " + numRegs + 
              " regions processed so far");
        }
        if (actualRegCount.get() > prevRegCount) { // Making progress
          prevRegCount = actualRegCount.get();
          tries = -1;
        }
      } else {
        return;
      }
    }
  }

  /**
   * Creates a new table but does not block and wait for it to come online.
   * Asynchronous operation.  To check if the table exists, use
   * {@link: #isTableAvailable} -- it is not safe to create an HTable
   * instance to this table before it is available.
   * Note : Avoid passing empty split key.
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException Bad table name, if the split keys
   * are repeated and if the split key has empty byte array.
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTableAsync(
    final HTableDescriptor desc, final byte [][] splitKeys)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    if(splitKeys != null && splitKeys.length > 0) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte [] lastKey = null;
      for(byte [] splitKey : splitKeys) {
        if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
          throw new IllegalArgumentException(
              "Empty split key must not be passed in the split keys.");
        }
        if(lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, " +
            "found duplicate: " + Bytes.toStringBinary(splitKey) +
            ", " + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
      }
    }

    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        master.createTable(desc, splitKeys);
        return null;
      }
    });
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
    HTableDescriptor.isLegalTableName(tableName);
    HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
    boolean tableExists = true;

    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        master.deleteTable(tableName);
        return null;
      }
    });

    // Wait until all regions deleted
    ClientProtocol server =
      connection.getClient(firstMetaServer.getHostname(), firstMetaServer.getPort());
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      try {

        Scan scan = MetaReader.getScanForTableName(tableName);
        scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
        ScanRequest request = RequestConverter.buildScanRequest(
          firstMetaServer.getRegionInfo().getRegionName(), scan, 1, true);
        Result[] values = null;
        // Get a batch at a time.
        try {
          ScanResponse response = server.scan(null, request);
          values = ResponseConverter.getResults(response);
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }

        // let us wait until .META. table is updated and
        // HMaster removes the table from its HTableDescriptors
        if (values == null || values.length == 0) {
          tableExists = false;
          HTableDescriptor[] htds;
          MasterKeepAliveConnection master = connection.getKeepAliveMaster();
          try {
            htds = master.getHTableDescriptors();
          } finally {
            master.close();
          }
          if (htds != null && htds.length > 0) {
            for (HTableDescriptor htd: htds) {
              if (Bytes.equals(tableName, htd.getName())) {
                tableExists = true;
                break;
              }
            }
          }
          if (!tableExists) {
            break;
          }
        }
      } catch (IOException ex) {
        if(tries == numRetries - 1) {           // no more tries left
          if (ex instanceof RemoteException) {
            throw ((RemoteException) ex).unwrapRemoteException();
          }else {
            throw ex;
          }
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }
    
    if (tableExists) {
      throw new IOException("Retries exhausted, it took too long to wait"+
        " for the table " + Bytes.toString(tableName) + " to be deleted.");
    }
    // Delete cached information to prevent clients from using old locations
    this.connection.clearRegionCache(tableName);
    LOG.info("Deleted " + Bytes.toString(tableName));
  }

  /**
   * Deletes tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.lang.String)} and
   * {@link #deleteTable(byte[])}
   *
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   * @see #deleteTables(java.util.regex.Pattern)
   * @see #deleteTable(java.lang.String)
   */
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  /**
   * Delete tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #deleteTable(byte[])}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   */
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      try {
        deleteTable(table.getName());
      } catch (IOException ex) {
        LOG.info("Failed to delete table " + table.getNameAsString(), ex);
        failed.add(table);
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }


  public void enableTable(final String tableName)
  throws IOException {
    enableTable(Bytes.toBytes(tableName));
  }

  /**
   * Enable a table.  May timeout.  Use {@link #enableTableAsync(byte[])}
   * and {@link #isTableEnabled(byte[])} instead.
   * The table has to be in disabled state for it to be enabled.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   * There could be couple types of IOException
   * TableNotFoundException means the table doesn't exist.
   * TableNotDisabledException means the table isn't in disabled state.
   * @see #isTableEnabled(byte[])
   * @see #disableTable(byte[])
   * @see #enableTableAsync(byte[])
   */
  public void enableTable(final byte [] tableName)
  throws IOException {
    enableTableAsync(tableName);

    // Wait until all regions are enabled
    boolean enabled = false;
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      enabled = isTableEnabled(tableName);
      if (enabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
          "enabled in " + Bytes.toString(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        throw new IOException("Interrupted", e);
      }
    }
    if (!enabled) {
      throw new IOException("Unable to enable table " +
        Bytes.toString(tableName));
    }
    LOG.info("Enabled table " + Bytes.toString(tableName));
  }

  public void enableTableAsync(final String tableName)
  throws IOException {
    enableTableAsync(Bytes.toBytes(tableName));
  }

  /**
   * Brings a table on-line (enables it).  Method returns immediately though
   * enable of table may take some time to complete, especially if the table
   * is large (All regions are opened as part of enabling process).  Check
   * {@link #isTableEnabled(byte[])} to learn when table is fully online.  If
   * table is taking too long to online, check server logs.
   * @param tableName
   * @throws IOException
   * @since 0.90.0
   */
  public void enableTableAsync(final byte [] tableName)
  throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        LOG.info("Started enable of " + Bytes.toString(tableName));
        master.enableTable(tableName);
        return null;
      }
    });
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.lang.String)} and
   * {@link #enableTable(byte[])}
   *
   * @param regex The regular expression to match table names against
   * @throws IOException
   * @see #enableTables(java.util.regex.Pattern)
   * @see #enableTable(java.lang.String)
   */
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    return enableTables(Pattern.compile(regex));
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #enableTable(byte[])}
   *
   * @param pattern The pattern to match table names against
   * @throws IOException
   */
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableDisabled(table.getName())) {
        try {
          enableTable(table.getName());
        } catch (IOException ex) {
          LOG.info("Failed to enable table " + table.getNameAsString(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  public void disableTableAsync(final String tableName) throws IOException {
    disableTableAsync(Bytes.toBytes(tableName));
  }

  /**
   * Starts the disable of a table.  If it is being served, the master
   * will tell the servers to stop serving it.  This method returns immediately.
   * The disable of a table can take some time if the table is large (all
   * regions are closed as part of table disable operation).
   * Call {@link #isTableDisabled(byte[])} to check for when disable completes.
   * If table is taking too long to online, check server logs.
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   * @see #isTableDisabled(byte[])
   * @see #isTableEnabled(byte[])
   * @since 0.90.0
   */
  public void disableTableAsync(final byte [] tableName) throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        LOG.info("Started disable of " + Bytes.toString(tableName));
        master.disableTable(tableName);
        return null;
      }
    });
  }

  public void disableTable(final String tableName)
  throws IOException {
    disableTable(Bytes.toBytes(tableName));
  }

  /**
   * Disable table and wait on completion.  May timeout eventually.  Use
   * {@link #disableTableAsync(byte[])} and {@link #isTableDisabled(String)}
   * instead.
   * The table has to be in enabled state for it to be disabled.
   * @param tableName
   * @throws IOException
   * There could be couple types of IOException
   * TableNotFoundException means the table doesn't exist.
   * TableNotEnabledException means the table isn't in enabled state.
   */
  public void disableTable(final byte [] tableName)
  throws IOException {
    disableTableAsync(tableName);
    // Wait until table is disabled
    boolean disabled = false;
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      disabled = isTableDisabled(tableName);
      if (disabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
          "disabled in " + Bytes.toString(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted", e);
      }
    }
    if (!disabled) {
      throw new RegionException("Retries exhausted, it took too long to wait"+
        " for the table " + Bytes.toString(tableName) + " to be disabled.");
    }
    LOG.info("Disabled " + Bytes.toString(tableName));
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.lang.String)} and
   * {@link #disableTable(byte[])}
   *
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   * @see #disableTables(java.util.regex.Pattern)
   * @see #disableTable(java.lang.String)
   */
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    return disableTables(Pattern.compile(regex));
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #disableTable(byte[])}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   */
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableEnabled(table.getName())) {
        try {
          disableTable(table.getName());
        } catch (IOException ex) {
          LOG.info("Failed to disable table " + table.getNameAsString(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
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
    HTableDescriptor.isLegalTableName(tableName);
    return connection.isTableEnabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(final String tableName) throws IOException {
    return isTableDisabled(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    HTableDescriptor.isLegalTableName(tableName);
    return connection.isTableDisabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return connection.isTableAvailable(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return connection.isTableAvailable(Bytes.toBytes(tableName));
  }

  /**
   * Get the status of alter command - indicates how many regions have received
   * the updated schema Asynchronous operation.
   *
   * @param tableName
   *          name of the table to get the status of
   * @return Pair indicating the number of regions updated Pair.getFirst() is the
   *         regions that are yet to be updated Pair.getSecond() is the total number
   *         of regions of the table
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName)
  throws IOException {
    HTableDescriptor.isLegalTableName(tableName);
    return execute(new MasterCallable<Pair<Integer, Integer>>() {
      @Override
      public Pair<Integer, Integer> call() throws IOException {
        return master.getAlterStatus(tableName);
      }
    });
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
    addColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final byte [] tableName, final HColumnDescriptor column)
  throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        master.addColumn(tableName, column);
        return null;
      }
    });
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
    deleteColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName));
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        master.deleteColumn(tableName, columnName);
        return null;
      }
    });
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final String tableName, HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(Bytes.toBytes(tableName), descriptor);
  }



  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final byte [] tableName, final HColumnDescriptor descriptor)
  throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        master.modifyColumn(tableName, descriptor);
        return null;
      }
    });
  }

  /**
   * Close a region. For expert-admins.  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param serverName If supplied, we'll use this location rather than
   * the one currently in <code>.META.</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final String regionname, final String serverName)
  throws IOException {
    closeRegion(Bytes.toBytes(regionname), serverName);
  }

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param serverName The servername of the regionserver.  If passed null we
   * will use servername found in the .META. table. A server name
   * is made of host, port and startcode.  Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final byte [] regionname, final String serverName)
  throws IOException {
    CatalogTracker ct = getCatalogTracker();
    try {
      if (serverName != null) {
        Pair<HRegionInfo, ServerName> pair = MetaReader.getRegion(ct, regionname);
        if (pair == null || pair.getFirst() == null) {
          LOG.info("No region in .META. for " +
            Bytes.toStringBinary(regionname) + "; pair=" + pair);
        } else {
          closeRegion(new ServerName(serverName), pair.getFirst());
        }
      } else {
        Pair<HRegionInfo, ServerName> pair = MetaReader.getRegion(ct, regionname);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toStringBinary(regionname) + "; pair=" + pair);
        } else {
          closeRegion(pair.getSecond(), pair.getFirst());
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  /**
   * For expert-admins. Runs close on the regionserver. Closes a region based on
   * the encoded region name. The region server name is mandatory. If the
   * servername is provided then based on the online regions in the specified
   * regionserver the specified region will be closed. The master will not be
   * informed of the close. Note that the regionname is the encoded regionname.
   * 
   * @param encodedRegionName
   *          The encoded region name; i.e. the hash that makes up the region
   *          name suffix: e.g. if regionname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>
   *          , then the encoded region name is:
   *          <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param serverName
   *          The servername of the regionserver. A server name is made of host,
   *          port and startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return true if the region was closed, false if not.
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean closeRegionWithEncodedRegionName(final String encodedRegionName,
      final String serverName) throws IOException {
    if (null == serverName || ("").equals(serverName.trim())) {
      throw new IllegalArgumentException(
          "The servername cannot be null or empty.");
    }
    ServerName sn = new ServerName(serverName);
    AdminProtocol admin = this.connection.getAdmin(
        sn.getHostname(), sn.getPort());
    // Close the region without updating zk state.
    CloseRegionRequest request =
      RequestConverter.buildCloseRegionRequest(encodedRegionName, false);
    try {
      CloseRegionResponse response = admin.closeRegion(null, request);
      boolean isRegionClosed = response.getClosed();
      if (false == isRegionClosed) {
        LOG.error("Not able to close the region " + encodedRegionName + ".");
      }
      return isRegionClosed;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param sn
   * @param hri
   * @throws IOException
   */
  public void closeRegion(final ServerName sn, final HRegionInfo hri)
  throws IOException {
    AdminProtocol admin =
      this.connection.getAdmin(sn.getHostname(), sn.getPort());
    // Close the region without updating zk state.
    ProtobufUtil.closeRegion(admin, hri.getRegionName(), false);
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    flush(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    CatalogTracker ct = getCatalogTracker();
    boolean isRegionName = isRegionName(tableNameOrRegionName, ct);
    try {
      if (isRegionName) {
        Pair<HRegionInfo, ServerName> pair =
          MetaReader.getRegion(ct, tableNameOrRegionName);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toStringBinary(tableNameOrRegionName) + "; pair=" + pair);
        } else {
          flush(pair.getSecond(), pair.getFirst());
        }
      } else {
        final String tableName = tableNameString(tableNameOrRegionName, ct);
        List<Pair<HRegionInfo, ServerName>> pairs =
          MetaReader.getTableRegionsAndLocations(ct,
              tableName);
        for (Pair<HRegionInfo, ServerName> pair: pairs) {
          if (pair.getFirst().isOffline()) continue;
          if (pair.getSecond() == null) continue;
          try {
            flush(pair.getSecond(), pair.getFirst());
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to flush " + pair.getFirst() + ": " +
                StringUtils.stringifyException(e));
            }
          }
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  private void flush(final ServerName sn, final HRegionInfo hri)
  throws IOException {
    AdminProtocol admin =
      this.connection.getAdmin(sn.getHostname(), sn.getPort());
    FlushRegionRequest request =
      RequestConverter.buildFlushRegionRequest(hri.getRegionName());
    try {
      admin.flushRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    compact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    compact(tableNameOrRegionName, false);
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    compact(tableNameOrRegionName, true);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  private void compact(final byte [] tableNameOrRegionName, final boolean major)
  throws IOException, InterruptedException {
    CatalogTracker ct = getCatalogTracker();
    try {
      if (isRegionName(tableNameOrRegionName, ct)) {
        Pair<HRegionInfo, ServerName> pair =
          MetaReader.getRegion(ct, tableNameOrRegionName);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toStringBinary(tableNameOrRegionName) + "; pair=" + pair);
        } else {
          compact(pair.getSecond(), pair.getFirst(), major);
        }
      } else {
        final String tableName = tableNameString(tableNameOrRegionName, ct);
        List<Pair<HRegionInfo, ServerName>> pairs =
          MetaReader.getTableRegionsAndLocations(ct,
              tableName);
        for (Pair<HRegionInfo, ServerName> pair: pairs) {
          if (pair.getFirst().isOffline()) continue;
          if (pair.getSecond() == null) continue;
          try {
            compact(pair.getSecond(), pair.getFirst(), major);
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to" + (major ? " major" : "") + " compact " +
                pair.getFirst() + ": " +
                StringUtils.stringifyException(e));
            }
          }
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  private void compact(final ServerName sn, final HRegionInfo hri,
      final boolean major)
  throws IOException {
    AdminProtocol admin =
      this.connection.getAdmin(sn.getHostname(), sn.getPort());
    CompactRegionRequest request =
      RequestConverter.buildCompactRegionRequest(hri.getRegionName(), major);
    try {
      admin.compactRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Move the region <code>r</code> to <code>dest</code>.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes
   * up the region name suffix: e.g. if regionname is
   * <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   * then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName The servername of the destination regionserver.  If
   * passed the empty byte array we'll assign to a random server.  A server name
   * is made of host, port and startcode.  Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>
   * @throws UnknownRegionException Thrown if we can't find a region named
   * <code>encodedRegionName</code>
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   */
  public void move(final byte [] encodedRegionName, final byte [] destServerName)
  throws UnknownRegionException, MasterNotRunningException, ZooKeeperConnectionException {
    MasterKeepAliveConnection master = connection.getKeepAliveMaster();
    try {
      MoveRegionRequest request = RequestConverter.buildMoveRegionRequest(encodedRegionName, destServerName);
      master.moveRegion(null,request);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof UnknownRegionException) {
        throw (UnknownRegionException)ioe;
      }
      LOG.error("Unexpected exception: " + se + " from calling HMaster.moveRegion");
    } catch (DeserializationException de) {
      LOG.error("Could not parse destination server name: " + de);
    }
    finally {
      master.close();
    }
  }

  /**
   * @param regionName
   *          Region name to assign.
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  public void assign(final byte[] regionName) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        AssignRegionRequest request = RequestConverter.buildAssignRegionRequest(regionName);
        master.assignRegion(null,request);
        return null;
      }
    });
  }

  /**
   * Unassign a region from current hosting regionserver.  Region will then be
   * assigned to a regionserver chosen at random.  Region could be reassigned
   * back to the same server.  Use {@link #move(byte[], byte[])} if you want
   * to control the region movement.
   * @param regionName Region to unassign. Will clear any existing RegionPlan
   * if one found.
   * @param force If true, force unassign (Will remove region from
   * regions-in-transition too if present. If results in double assignment
   * use hbck -fix to resolve. To be used by experts).
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  public void unassign(final byte [] regionName, final boolean force)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        UnassignRegionRequest request =
          RequestConverter.buildUnassignRegionRequest(regionName, force);
        master.unassignRegion(null,request);
        return null;
      }
    });
  }

  /**
   * Turn the load balancer on or off.
   * @param b If true, enable balancer. If false, disable balancer.
   * @return Previous balancer value
   */
  public boolean balanceSwitch(final boolean b)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    MasterKeepAliveConnection master = connection.getKeepAliveMaster();
    try {
      SetBalancerRunningRequest req = RequestConverter.buildLoadBalancerIsRequest(b, false);
      return master.loadBalancerIs(null, req).getPrevBalanceValue();
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof MasterNotRunningException) {
        throw (MasterNotRunningException)ioe;
      }
      if (ioe instanceof ZooKeeperConnectionException) {
        throw (ZooKeeperConnectionException)ioe;
      }

      // Throwing MasterNotRunningException even though not really valid in order to not
      // break interface by adding additional exception type.
      throw new MasterNotRunningException("Unexpected exception when calling balanceSwitch",se);
    } finally {
      master.close();
    }
  }

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will
   * go ahead and do the reassignments.  Can NOT run for various reasons.  Check
   * logs.
   * @return True if balancer ran, false otherwise.
   */
  public boolean balancer()
  throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException {
    MasterKeepAliveConnection master = connection.getKeepAliveMaster();
    try {
      return master.balance(null,RequestConverter.buildBalanceRequest()).getBalancerRan();
    } finally {
      master.close();
    }
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void split(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    split(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Split a table or an individual region.  Implicitly finds an optimal split
   * point.  Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void split(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    split(tableNameOrRegionName, null);
  }

  public void split(final String tableNameOrRegionName,
    final String splitPoint) throws IOException, InterruptedException {
    split(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(splitPoint));
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException interrupt exception occurred
   */
  public void split(final byte [] tableNameOrRegionName,
      final byte [] splitPoint) throws IOException, InterruptedException {
    CatalogTracker ct = getCatalogTracker();
    try {
      if (isRegionName(tableNameOrRegionName, ct)) {
        // Its a possible region name.
        Pair<HRegionInfo, ServerName> pair =
          MetaReader.getRegion(ct, tableNameOrRegionName);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toStringBinary(tableNameOrRegionName) + "; pair=" + pair);
        } else {
          split(pair.getSecond(), pair.getFirst(), splitPoint);
        }
      } else {
        final String tableName = tableNameString(tableNameOrRegionName, ct);
        List<Pair<HRegionInfo, ServerName>> pairs =
          MetaReader.getTableRegionsAndLocations(ct,
              tableName);
        for (Pair<HRegionInfo, ServerName> pair: pairs) {
          // May not be a server for a particular row
          if (pair.getSecond() == null) continue;
          HRegionInfo r = pair.getFirst();
          // check for parents
          if (r.isSplitParent()) continue;
          // if a split point given, only split that particular region
          if (splitPoint != null && !r.containsRow(splitPoint)) continue;
          // call out to region server to do split now
          split(pair.getSecond(), pair.getFirst(), splitPoint);
        }
      }
    } finally {
      cleanupCatalogTracker(ct);
    }
  }

  private void split(final ServerName sn, final HRegionInfo hri,
      byte[] splitPoint) throws IOException {
    AdminProtocol admin =
      this.connection.getAdmin(sn.getHostname(), sn.getPort());
    ProtobufUtil.split(admin, hri, splitPoint);
  }

  /**
   * Modify an existing table, more IRB friendly version.
   * Asynchronous operation.  This means that it may be a while before your
   * schema change is updated across all of the table.
   *
   * @param tableName name of table.
   * @param htd modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(final byte [] tableName, final HTableDescriptor htd)
  throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws IOException {
        master.modifyTable(tableName, htd);
        return null;
      }
    });
  }

  /**
   * @param tableNameOrRegionName Name of a table or name of a region.
   * @param ct A {@link CatalogTracker} instance (caller of this method usually has one).
   * @return True if <code>tableNameOrRegionName</code> is a verified region
   * name (we call {@link  MetaReader#getRegion( CatalogTracker, byte[])}
   *  else false.
   * Throw an exception if <code>tableNameOrRegionName</code> is null.
   * @throws IOException
   */
  private boolean isRegionName(final byte[] tableNameOrRegionName,
      CatalogTracker ct)
  throws IOException {
    if (tableNameOrRegionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    return (MetaReader.getRegion(ct, tableNameOrRegionName) != null);
  }

  /**
   * Convert the table name byte array into a table name string and check if table
   * exists or not.
   * @param tableNameBytes Name of a table.
   * @param ct A {@link CatalogTracker} instance (caller of this method usually has one).
   * @return tableName in string form.
   * @throws IOException if a remote or network exception occurs.
   * @throws TableNotFoundException if table does not exist.
   */
  private String tableNameString(final byte[] tableNameBytes, CatalogTracker ct)
      throws IOException {
    String tableNameString = Bytes.toString(tableNameBytes);
    if (!MetaReader.tableExists(ct, tableNameString)) {
      throw new TableNotFoundException(tableNameString);
    }
    return tableNameString;
  }

  /**
   * Shuts down the HBase cluster
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void shutdown() throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        master.shutdown(null,ShutdownRequest.newBuilder().build());
        return null;
      }
    });
  }

  /**
   * Shuts down the current HBase master only.
   * Does not shutdown the cluster.
   * @see #shutdown()
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopMaster() throws IOException {
    execute(new MasterCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        master.stopMaster(null,StopMasterRequest.newBuilder().build());
        return null;
      }
    });
  }

  /**
   * Stop the designated regionserver
   * @param hostnamePort Hostname and port delimited by a <code>:</code> as in
   * <code>example.org:1234</code>
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopRegionServer(final String hostnamePort)
  throws IOException {
    String hostname = Addressing.parseHostname(hostnamePort);
    int port = Addressing.parsePort(hostnamePort);
    AdminProtocol admin =
      this.connection.getAdmin(hostname, port);
    StopServerRequest request = RequestConverter.buildStopServerRequest(
      "Called by admin client " + this.connection.toString());
    try {
      admin.stopServer(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return execute(new MasterCallable<ClusterStatus>() {
      @Override
      public ClusterStatus call() {
        return master.getClusterStatus();
      }
    });
  }

  private HRegionLocation getFirstMetaServerForTable(final byte [] tableName)
  throws IOException {
    return connection.locateRegion(HConstants.META_TABLE_NAME,
      HRegionInfo.createRegionName(tableName, null, HConstants.NINES, false));
  }

  /**
   * @return Configuration used by the instance.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   * We consider that HBase is running if ZooKeeper and Master are running.
   *
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public static void checkHBaseAvailable(Configuration conf)
    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);

    // We set it to make it fail as soon as possible if HBase is not available
    copyOfConf.setInt("hbase.client.retries.number", 1);
    copyOfConf.setInt("zookeeper.recovery.retry", 0);

    HConnectionManager.HConnectionImplementation connection
      = (HConnectionManager.HConnectionImplementation)
      HConnectionManager.getConnection(copyOfConf);

    try {
      // Check ZK first.
      // If the connection exists, we may have a connection to ZK that does
      //  not work anymore
      ZooKeeperKeepAliveConnection zkw = null;
      try {
        zkw = connection.getKeepAliveZooKeeperWatcher();
        zkw.getRecoverableZooKeeper().getZooKeeper().exists(
          zkw.baseZNode, false);

      } catch (IOException e) {
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } catch (KeeperException e) {
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } finally {
        if (zkw != null) {
          zkw.close();
        }
      }

      // Check Master, same logic.
      MasterKeepAliveConnection master = null;
      try {
        master = connection.getKeepAliveMaster();
        master.isMasterRunning(null,RequestConverter.buildIsMasterRunningRequest());
      } finally {
        if (master != null) {
          master.close();
        }
      }

    } finally {
      connection.close();
    }
  }

  /**
   * get the regions of a given table.
   *
   * @param tableName the name of the table
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */  
  public List<HRegionInfo> getTableRegions(final byte[] tableName)
  throws IOException {
    CatalogTracker ct = getCatalogTracker();
    List<HRegionInfo> Regions = null;
    try {
      Regions = MetaReader.getTableRegions(ct, tableName, true);
    } finally {
      cleanupCatalogTracker(ct);
    }
    return Regions;
  }
  
  @Override
  public void close() throws IOException {
    if (this.connection != null) {
      this.connection.close();
    }
  }

 /**
 * Get tableDescriptors
 * @param tableNames List of table names
 * @return HTD[] the tableDescriptor
 * @throws IOException if a remote or network exception occurs
 */
  public HTableDescriptor[] getTableDescriptors(List<String> tableNames)
  throws IOException {
    return this.connection.getHTableDescriptors(tableNames);
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   * 
   * @param serverName
   *          The servername of the regionserver. A server name is made of host,
   *          port and startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()}  
   * @throws IOException if a remote or network exception occurs
   * @throws FailedLogCloseException
   */
 public synchronized  byte[][] rollHLogWriter(String serverName)
      throws IOException, FailedLogCloseException {
    ServerName sn = new ServerName(serverName);
    AdminProtocol admin = this.connection.getAdmin(
        sn.getHostname(), sn.getPort());
    RollWALWriterRequest request = RequestConverter.buildRollWALWriterRequest();;
    try {
      RollWALWriterResponse response = admin.rollWALWriter(null, request);
      int regionCount = response.getRegionToFlushCount();
      byte[][] regionsToFlush = new byte[regionCount][];
      for (int i = 0; i < regionCount; i++) {
        ByteString region = response.getRegionToFlush(i);
        regionsToFlush[i] = region.toByteArray();
      }
      return regionsToFlush;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  public String[] getMasterCoprocessors() {
    try {
      return getClusterStatus().getMasterCoprocessors();
    } catch (IOException e) {
      LOG.error("Could not getClusterStatus()",e);
      return null;
    }
  }

  /**
   * Get the current compaction state of a table or region.
   * It could be in a major compaction, a minor compaction, both, or none.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   * @return the current compaction state
   */
  public CompactionState getCompactionState(final String tableNameOrRegionName)
      throws IOException, InterruptedException {
    return getCompactionState(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Get the current compaction state of a table or region.
   * It could be in a major compaction, a minor compaction, both, or none.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   * @return the current compaction state
   */
  public CompactionState getCompactionState(final byte [] tableNameOrRegionName)
      throws IOException, InterruptedException {
    CompactionState state = CompactionState.NONE;
    CatalogTracker ct = getCatalogTracker();
    try {
      if (isRegionName(tableNameOrRegionName, ct)) {
        Pair<HRegionInfo, ServerName> pair =
          MetaReader.getRegion(ct, tableNameOrRegionName);
        if (pair == null || pair.getSecond() == null) {
          LOG.info("No server in .META. for " +
            Bytes.toStringBinary(tableNameOrRegionName) + "; pair=" + pair);
        } else {
          ServerName sn = pair.getSecond();
          AdminProtocol admin =
            this.connection.getAdmin(sn.getHostname(), sn.getPort());
          GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
            pair.getFirst().getRegionName(), true);
          GetRegionInfoResponse response = admin.getRegionInfo(null, request);
          return response.getCompactionState();
        }
      } else {
        final String tableName = tableNameString(tableNameOrRegionName, ct);
        List<Pair<HRegionInfo, ServerName>> pairs =
          MetaReader.getTableRegionsAndLocations(ct, tableName);
        for (Pair<HRegionInfo, ServerName> pair: pairs) {
          if (pair.getFirst().isOffline()) continue;
          if (pair.getSecond() == null) continue;
          try {
            ServerName sn = pair.getSecond();
            AdminProtocol admin =
              this.connection.getAdmin(sn.getHostname(), sn.getPort());
            GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
              pair.getFirst().getRegionName(), true);
            GetRegionInfoResponse response = admin.getRegionInfo(null, request);
            switch (response.getCompactionState()) {
            case MAJOR_AND_MINOR:
              return CompactionState.MAJOR_AND_MINOR;
            case MAJOR:
              if (state == CompactionState.MINOR) {
                return CompactionState.MAJOR_AND_MINOR;
              }
              state = CompactionState.MAJOR;
              break;
            case MINOR:
              if (state == CompactionState.MAJOR) {
                return CompactionState.MAJOR_AND_MINOR;
              }
              state = CompactionState.MINOR;
              break;
            case NONE:
              default: // nothing, continue
            }
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to get compaction state of " +
                pair.getFirst() + ": " +
                StringUtils.stringifyException(e));
            }
          }
        }
      }
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      cleanupCatalogTracker(ct);
    }
    return state;
  }

  /**
   * @see {@link #execute}
   */
  private abstract static class MasterCallable<V> implements Callable<V>{
    protected MasterKeepAliveConnection master;
  }

  /**
   * This method allows to execute a function requiring a connection to
   * master without having to manage the connection creation/close.
   * Create a {@link MasterCallable} to use it.
   */
  private <V> V execute(MasterCallable<V> function) throws IOException {
    function.master = connection.getKeepAliveMaster();
    try {
      return function.call();
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    } catch (IOException e) {
      throw e;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } catch (Exception e) {
      // This should not happen...
      throw new IOException("Unexpected exception when calling master", e);
    } finally {
      function.master.close();
    }
  }
}
