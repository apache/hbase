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
import java.io.InterruptedIOException;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.MasterExecRPCInvoker;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest.CompactionState;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.HSnapshotDescription;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;

/**
 * Provides an interface to manage HBase database table metadata + general
 * administrative functions.  Use HBaseAdmin to create, drop, list, enable and
 * disable tables. Use it also to add and drop table column families.
 *
 * <p>See {@link HTable} to add, update, and delete data from an individual table.
 * <p>Currently HBaseAdmin instances are not expected to be long-lived.  For
 * example, an HBaseAdmin instance will not ride over a Master restart.
 */
public class HBaseAdmin implements Abortable, Closeable {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
//  private final HConnection connection;
  private HConnection connection;
  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Mutliply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;
  private boolean aborted;

  private static volatile boolean synchronousBalanceSwitchSupported = true;
  private final boolean cleanupConnectionOnClose; // close the connection in close()

  /**
   * Constructor
   *
   * @param c Configuration object
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HBaseAdmin(Configuration c)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    this.conf = HBaseConfiguration.create(c);
    this.connection = HConnectionManager.getConnection(this.conf);
    this.pause = this.conf.getLong("hbase.client.pause", 1000);
    this.numRetries = this.conf.getInt("hbase.client.retries.number", 10);
    this.retryLongerMultiplier = this.conf.getInt(
        "hbase.client.retries.longer.multiplier", 10);
    this.cleanupConnectionOnClose = true;

    int tries = 0;
    while ( true ){
      try {

        this.connection.getMaster();
        return;

      } catch (MasterNotRunningException mnre) {
        HConnectionManager.deleteStaleConnection(this.connection);
        this.connection = HConnectionManager.getConnection(this.conf);
      }

      tries++;
      if (tries >= numRetries) {
        // we should delete connection between client and zookeeper
        HConnectionManager.deleteStaleConnection(this.connection);
        throw new MasterNotRunningException("Retried " + numRetries + " times");
      }

      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // we should delete connection between client and zookeeper
        HConnectionManager.deleteStaleConnection(this.connection);
        throw new MasterNotRunningException(
          "Interrupted after "+tries+" tries");
      }
    }
  }

 /**
   * Constructor for externally managed HConnections.
   * This constructor fails fast if the HMaster is not running.
   * The HConnection can be re-used again in another attempt.
   * This constructor fails fast.
   *
   * @param connection The HConnection instance to use
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HBaseAdmin(HConnection connection)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    this.conf = connection.getConfiguration();
    this.connection = connection;
    this.cleanupConnectionOnClose = false;

    this.pause = this.conf.getLong("hbase.client.pause", 1000);
    this.numRetries = this.conf.getInt("hbase.client.retries.number", 10);
    this.retryLongerMultiplier = this.conf.getInt(
        "hbase.client.retries.longer.multiplier", 10);

    this.connection.getMaster();
  }

  /**
   * @return A new CatalogTracker instance; call {@link #cleanupCatalogTracker(CatalogTracker)}
   * to cleanup the returned catalog tracker.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   * @see #cleanupCatalogTracker(CatalogTracker)
   */
  @VisibleForTesting
  synchronized CatalogTracker getCatalogTracker()
  throws ZooKeeperConnectionException, IOException {
    boolean succeeded = false;
    CatalogTracker ct = null;
    try {
      ct = new CatalogTracker(this.conf);
      startCatalogTracker(ct);
      succeeded = true;
    } catch (InterruptedException e) {
      // Let it out as an IOE for now until we redo all so tolerate IEs
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted", e);
    } finally {
      // If we did not succeed but created a catalogtracker, clean it up. CT has a ZK instance
      // in it and we'll leak if we don't do the 'stop'.
      if (!succeeded && ct != null) {
        try {
          ct.stop();
        } catch (RuntimeException re) {
          LOG.error("Failed to clean up HBase's internal catalog tracker after a failed initialization. " +
            "We may have leaked network connections to ZooKeeper; they won't be cleaned up until " +
            "the JVM exits. If you see a large number of stale connections to ZooKeeper this is likely " +
            "the cause. The following exception details will be needed for assistance from the " +
            "HBase community.", re);
        }
        ct = null;
      }
    }
    return ct;
  }

  @VisibleForTesting
  CatalogTracker startCatalogTracker(final CatalogTracker ct)
  throws IOException, InterruptedException {
    ct.start();
    return ct;
  }

  @VisibleForTesting
  void cleanupCatalogTracker(final CatalogTracker ct) {
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
    return this.connection.getMaster();
  }

  /** @return - true if the master server is running
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException */
  public boolean isMasterRunning()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return this.connection.isMasterRunning();
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
   * List all of the names of userspace tables.
   * @return the list of table names
   * @throws IOException if a remote or network exception occurs
   */
  public String[] getTableNames() throws IOException {
    return this.connection.getTableNames();
  }

  /**
   * List all of the names of userspace tables matching the given pattern
   * @param pattern The compiled regular expression to match against
   * @return the list of table names
   * @throws IOException if a remote or network exception occurs
   */
  public String[] getTableNames(Pattern pattern) throws IOException {
    List<String> matched = new ArrayList<String>();
    for (String name : this.connection.getTableNames()) {
      if (pattern.matcher(name).matches()) {
        matched.add(name);
      }
    }
    return matched.toArray(new String[matched.size()]);
  }

  /**
   * List all of the names of userspace tables matching the given regex
   * @param regex The regular expression to match against
   * @return the list of table names
   * @throws IOException if a remote or network exception occurs
   */
  public String[] getTableNames(String regex) throws IOException {
    return getTableNames(Pattern.compile(regex));
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
    if (numRegions == 3) {
      createTable(desc, new byte[][] { startKey, endKey });
      return;
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
    boolean doneWithMetaScan = false;
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier;
      ++tries) {
      if (!doneWithMetaScan) {
        // Wait for new table to come on-line
        final AtomicInteger actualRegCount = new AtomicInteger(0);
        MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
          @Override
          public boolean processRow(Result rowResult) throws IOException {
            if (rowResult == null || rowResult.size() <= 0) {
              return true;
            }
            HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(
              rowResult, HConstants.REGIONINFO_QUALIFIER);
            if (info == null) {
              LOG.warn("No serialized HRegionInfo in " + rowResult);
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
        MetaScanner.metaScan(conf, connection, visitor, desc.getName());
        if (actualRegCount.get() < numRegs) {
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
          doneWithMetaScan = true;
          tries = -1;
        }
      } else if (isTableEnabled(desc.getName())) {
        return;
      } else {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting" +
            " for table to be enabled; meta scan was done");
        }
      }
    }
    throw new TableNotEnabledException(
      "Retries exhausted while still waiting for table: "
      + desc.getNameAsString() + " to be enabled");
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
  public void createTableAsync(HTableDescriptor desc, byte [][] splitKeys)
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
    try {
      getMaster().createTable(desc, splitKeys);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException();
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
    isMasterRunning();
    HTableDescriptor.isLegalTableName(tableName);
    HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
    boolean tableExists = true;
    try {
      getMaster().deleteTable(tableName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
    // Wait until all regions deleted
    HRegionInterface server =
      connection.getHRegionConnection(firstMetaServer.getHostname(), firstMetaServer.getPort());
    List<String> tableNameAsList = new ArrayList<String>(1);
    tableNameAsList.add(Bytes.toString(tableName));
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      long scannerId = -1L;
      try {

        Scan scan = MetaReader.getScanForTableName(tableName);
        scan.addColumn(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        scannerId = server.openScanner(
          firstMetaServer.getRegionInfo().getRegionName(), scan);
        // Get a batch at a time.
        Result values = server.next(scannerId);

        // let us wait until .META. table is updated and
        // HMaster removes the table from its HTableDescriptors
        if (values == null) {
          tableExists = false;
          HTableDescriptor[] htds = getMaster().getHTableDescriptors(tableNameAsList);
          tableExists = (htds != null && htds.length > 0);
          if (!tableExists) {
            break;
          }
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
    waitUntilTableIsEnabled(tableName);

    LOG.info("Enabled table " + Bytes.toString(tableName));
  }

  /**
   * Wait for the table to be enabled and available
   * If enabling the table exceeds the retry period, an exception is thrown.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs or
   *    table is not enabled after the retries period.
   */
  private void waitUntilTableIsEnabled(final byte[] tableName) throws IOException {
    boolean enabled = false;
    long start = EnvironmentEdgeManager.currentTimeMillis();
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      enabled = isTableEnabled(tableName) && isTableAvailable(tableName);
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
      long msec = EnvironmentEdgeManager.currentTimeMillis() - start;
      throw new IOException("Table '" + Bytes.toString(tableName) +
        "' not yet enabled, after " + msec + "ms.");
    }
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
    HTableDescriptor.isLegalTableName(tableName);
    isMasterRunning();
    try {
      getMaster().enableTable(tableName);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException();
    }
    LOG.info("Started enable of " + Bytes.toString(tableName));
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
    HTableDescriptor.isLegalTableName(tableName);
    isMasterRunning();
    try {
      getMaster().disableTable(tableName);
    } catch (RemoteException e) {
      throw e.unwrapRemoteException();
    }
    LOG.info("Started disable of " + Bytes.toString(tableName));
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

  /*
   * Checks whether table exists. If not, throws TableNotFoundException
   * @param tableName
   */
  private void checkTableExistence(byte[] tableName) throws IOException {
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
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
    if (!HTableDescriptor.isMetaTable(tableName)) {
      HTableDescriptor.isLegalTableName(tableName);
    }
    checkTableExistence(tableName);
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
    if (!HTableDescriptor.isMetaTable(tableName)) {
      HTableDescriptor.isLegalTableName(tableName);
    }
    checkTableExistence(tableName);
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
    try {
      return getMaster().getAlterStatus(tableName);
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
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException {
    HTableDescriptor.isLegalTableName(tableName);
    try {
      getMaster().addColumn(tableName, column);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
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
    try {
      getMaster().deleteColumn(tableName, columnName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
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
  public void modifyColumn(final byte [] tableName, HColumnDescriptor descriptor)
  throws IOException {
    try {
      getMaster().modifyColumn(tableName, descriptor);
    } catch (RemoteException re) {
      // Convert RE exceptions in here; client shouldn't have to deal with them,
      // at least w/ the type of exceptions that come out of this method:
      // TableNotFoundException, etc.
      throw RemoteExceptionHandler.decodeRemoteException(re);
    }
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
          throw new UnknownRegionException(Bytes.toStringBinary(regionname));
        } else {
          closeRegion(new ServerName(serverName), pair.getFirst());
        }
      } else {
        Pair<HRegionInfo, ServerName> pair = MetaReader.getRegion(ct, regionname);
        if (pair == null) {
          throw new UnknownRegionException(Bytes.toStringBinary(regionname));
        } else if (pair.getSecond() == null) {
          throw new NoServerForRegionException(Bytes.toStringBinary(regionname));
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
    byte[] encodedRegionNameInBytes = Bytes.toBytes(encodedRegionName);
    if (null == serverName || ("").equals(serverName.trim())) {
      throw new IllegalArgumentException(
          "The servername cannot be null or empty.");
    }
    ServerName sn = new ServerName(serverName);
    HRegionInterface rs = this.connection.getHRegionConnection(
        sn.getHostname(), sn.getPort());
    // Close the region without updating zk state.
    boolean isRegionClosed = rs.closeRegion(encodedRegionNameInBytes, false);
    if (false == isRegionClosed) {
      LOG.error("Not able to close the region " + encodedRegionName + ".");
    }
    return isRegionClosed;
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
    HRegionInterface rs =
      this.connection.getHRegionConnection(sn.getHostname(), sn.getPort());
    // Close the region without updating zk state.
    rs.closeRegion(hri, false);
  }

  /**
   * Flush a table or an individual region.
   * Synchronous operation.
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
   * Synchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final byte [] tableNameOrRegionName)
  throws IOException, InterruptedException {
    CatalogTracker ct = getCatalogTracker();
    try {
      Pair<HRegionInfo, ServerName> regionServerPair
        = getRegion(tableNameOrRegionName, ct);
      if (regionServerPair != null) {
        if (regionServerPair.getSecond() == null) {
          throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
        } else {
          flush(regionServerPair.getSecond(), regionServerPair.getFirst());
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
    HRegionInterface rs =
      this.connection.getHRegionConnection(sn.getHostname(), sn.getPort());
    rs.flushRegion(hri);
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
    compact(tableNameOrRegionName, null, false);
  }

  /**
   * Compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(String tableOrRegionName, String columnFamily)
    throws IOException,  InterruptedException {
    compact(Bytes.toBytes(tableOrRegionName), Bytes.toBytes(columnFamily));
  }

  /**
   * Compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final byte [] tableNameOrRegionName, final byte[] columnFamily)
  throws IOException, InterruptedException {
    compact(tableNameOrRegionName, columnFamily, false);
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
    compact(tableNameOrRegionName, null, true);
  }

  /**
   * Major compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final String tableNameOrRegionName,
    final String columnFamily) throws IOException, InterruptedException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName),
      Bytes.toBytes(columnFamily));
  }

  /**
   * Major compact a column family within a table or region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final byte [] tableNameOrRegionName,
    final byte[] columnFamily) throws IOException, InterruptedException {
    compact(tableNameOrRegionName, columnFamily, true);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  private void compact(final byte [] tableNameOrRegionName,
    final byte[] columnFamily, final boolean major)
  throws IOException, InterruptedException {
    CatalogTracker ct = getCatalogTracker();
    try {
      Pair<HRegionInfo, ServerName> regionServerPair
        = getRegion(tableNameOrRegionName, ct);
      if (regionServerPair != null) {
        if (regionServerPair.getSecond() == null) {
          throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
        } else {
          compact(regionServerPair.getSecond(), regionServerPair.getFirst(), major, columnFamily);
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
            compact(pair.getSecond(), pair.getFirst(), major, columnFamily);
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
      final boolean major, final byte [] family)
  throws IOException {
    HRegionInterface rs =
      this.connection.getHRegionConnection(sn.getHostname(), sn.getPort());
    if (family != null) {
      try {
        rs.compactRegion(hri, major, family);
      } catch (IOException ioe) {
        String notFoundMsg = "java.lang.NoSuchMethodException: org.apache.hadoop.hbase.ipc.HRegionInterface."
          + "compactRegion(org.apache.hadoop.hbase.HRegionInfo, boolean, [B)";
        if (ioe.getMessage().contains(notFoundMsg)) {
          throw new IOException("per-column family compaction not supported on this version "
            + "of the HBase server.  You may still compact at the table or region level by "
          	+ "omitting the column family name.  Alternatively, you can upgrade the HBase server");
        }
        throw ioe;
      }
    } else {
      rs.compactRegion(hri, major);
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
    getMaster().move(encodedRegionName, destServerName);
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
    getMaster().assign(regionName);
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
    getMaster().unassign(regionName, force);
  }

  /**
   * Turn the load balancer on or off.
   * @param b If true, enable balancer. If false, disable balancer.
   * @return Previous balancer value
   * @deprecated use setBalancerRunning(boolean, boolean) instead
   */
  @Deprecated
  public boolean balanceSwitch(final boolean b)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return getMaster().balanceSwitch(b);
  }

  /**
   * Turn the load balancer on or off.
   * @param on If true, enable balancer. If false, disable balancer.
   * @param synchronous If true, it waits until current balance() call, if outstanding, to return.
   * @return Previous balancer value
   */
  public boolean setBalancerRunning(final boolean on, final boolean synchronous)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    if (synchronous && synchronousBalanceSwitchSupported) {
      try {
        return getMaster().synchronousBalanceSwitch(on);
      } catch (UndeclaredThrowableException ute) {
        String error = ute.getCause().getMessage();
        if (error != null && error.matches(
            "(?s).+NoSuchMethodException:.+synchronousBalanceSwitch.+")) {
          LOG.info("HMaster doesn't support synchronousBalanceSwitch");
          synchronousBalanceSwitchSupported = false;
        } else {
          throw ute;
        }
      }
    }
    return balanceSwitch(on);
  }

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will
   * go ahead and do the reassignments.  Can NOT run for various reasons.  Check
   * logs.
   * @return True if balancer ran, false otherwise.
   */
  public boolean balancer()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return getMaster().balance();
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
      Pair<HRegionInfo, ServerName> regionServerPair
        = getRegion(tableNameOrRegionName, ct);
      if (regionServerPair != null) {
        if (regionServerPair.getSecond() == null) {
            throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
        } else {
          split(regionServerPair.getSecond(), regionServerPair.getFirst(), splitPoint);
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
    HRegionInterface rs =
      this.connection.getHRegionConnection(sn.getHostname(), sn.getPort());
    rs.splitRegion(hri, splitPoint);
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
  public void modifyTable(final byte [] tableName, HTableDescriptor htd)
  throws IOException {
    try {
      getMaster().modifyTable(tableName, htd);
    } catch (RemoteException re) {
      // Convert RE exceptions in here; client shouldn't have to deal with them,
      // at least w/ the type of exceptions that come out of this method:
      // TableNotFoundException, etc.
      throw RemoteExceptionHandler.decodeRemoteException(re);
    }
  }

  /**
   * @param tableNameOrRegionName Name of a table or name of a region.
   * @param ct A {@link CatalogTracker} instance (caller of this method usually has one).
   * @return a pair of HRegionInfo and ServerName if <code>tableNameOrRegionName</code> is
   *  a verified region name (we call {@link  MetaReader#getRegion( CatalogTracker, byte[])}
   *  else null.
   * Throw an exception if <code>tableNameOrRegionName</code> is null.
   * @throws IOException
   */
  Pair<HRegionInfo, ServerName> getRegion(final byte[] tableNameOrRegionName,
      final CatalogTracker ct) throws IOException {
    if (tableNameOrRegionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    Pair<HRegionInfo, ServerName> pair = MetaReader.getRegion(ct, tableNameOrRegionName);
    if (pair == null) {
      final AtomicReference<Pair<HRegionInfo, ServerName>> result =
        new AtomicReference<Pair<HRegionInfo, ServerName>>(null);
      final String encodedName = Bytes.toString(tableNameOrRegionName);
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result data) throws IOException {
          if (data == null || data.size() <= 0) {
            return true;
          }
          HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(
            data, HConstants.REGIONINFO_QUALIFIER);
          if (info == null) {
            LOG.warn("No serialized HRegionInfo in " + data);
            return true;
          }
          if (!encodedName.equals(info.getEncodedName())) return true;
          ServerName sn = MetaReader.getServerNameFromCatalogResult(data);
          result.set(new Pair<HRegionInfo, ServerName>(info, sn));
          return false; // found the region, stop
        }
      };

      MetaScanner.metaScan(conf, connection, visitor, null);
      pair = result.get();
    }
    return pair;
  }

  /**
   * Convert the table name byte array into a table name string and check if table
   * exists or not.
   * @param tableNameBytes Name of a table.
   * @param ct A {@link #CatalogTracker} instance (caller of this method usually has one).
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
    isMasterRunning();
    try {
      getMaster().shutdown();
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Shuts down the current HBase master only.
   * Does not shutdown the cluster.
   * @see #shutdown()
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopMaster() throws IOException {
    isMasterRunning();
    try {
      getMaster().stopMaster();
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
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
    HRegionInterface rs =
      this.connection.getHRegionConnection(hostname, port);
    rs.stop("Called by admin client " + this.connection.toString());
  }

  /**
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return getMaster().getClusterStatus();
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
   *
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public static void checkHBaseAvailable(Configuration conf)
  throws MasterNotRunningException, ZooKeeperConnectionException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);
    copyOfConf.setInt("hbase.client.retries.number", 1);
    HBaseAdmin admin = new HBaseAdmin(copyOfConf);
    try {
      admin.close();
    } catch (IOException ioe) {
      admin.LOG.info("Failed to close connection", ioe);
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

  public void close() throws IOException {
    if (cleanupConnectionOnClose && this.connection != null) {
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
    HRegionInterface rs = this.connection.getHRegionConnection(
        sn.getHostname(), sn.getPort());
    return rs.rollHLogWriter();
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
      Pair<HRegionInfo, ServerName> regionServerPair
        = getRegion(tableNameOrRegionName, ct);
      if (regionServerPair != null) {
        if (regionServerPair.getSecond() == null) {
          throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
        } else {
          ServerName sn = regionServerPair.getSecond();
          HRegionInterface rs =
            this.connection.getHRegionConnection(sn.getHostname(), sn.getPort());
          return CompactionState.valueOf(
            rs.getCompactionState(regionServerPair.getFirst().getRegionName()));
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
            HRegionInterface rs =
              this.connection.getHRegionConnection(sn.getHostname(), sn.getPort());
            switch (CompactionState.valueOf(
              rs.getCompactionState(pair.getFirst().getRegionName()))) {
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
    } finally {
      cleanupCatalogTracker(ct);
    }
    return state;
  }

  /**
   * Creates and returns a proxy to the CoprocessorProtocol instance running in the
   * master.
   *
   * @param protocol The class or interface defining the remote protocol
   * @return A CoprocessorProtocol instance
   */
  public <T extends CoprocessorProtocol> T coprocessorProxy(
      Class<T> protocol) {
    return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(),
        new Class[]{protocol},
        new MasterExecRPCInvoker(conf,
            connection,
            protocol));
  }


  /**
   * Create a timestamp consistent snapshot for the given table.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link HTableDescriptor#isLegalTableName(byte[])}.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(final String snapshotName, final String tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, tableName, SnapshotDescription.Type.FLUSH);
  }

  /**
   * Create snapshot for the given table of given flush type.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link HTableDescriptor#isLegalTableName(byte[])}.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @param flushType if the snapshot should be taken without flush memstore first
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(final byte[] snapshotName, final byte[] tableName,
                       final SnapshotDescription.Type flushType) throws
      IOException, SnapshotCreationException, IllegalArgumentException {
      snapshot(Bytes.toString(snapshotName), Bytes.toString(tableName), flushType);
  }

  /**
   * Take a snapshot for the given table. If the table is enabled, a FLUSH-type snapshot will be
   * taken. If the table is disabled, an offline snapshot is taken.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link HTableDescriptor#isLegalTableName(byte[])}.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(final byte[] snapshotName, final byte[] tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), Bytes.toString(tableName));
  }

  /**
   * Create typed snapshot of the table.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link HTableDescriptor#isLegalTableName(byte[])}.
   * <p>
   * @param snapshotName name to give the snapshot on the filesystem. Must be unique from all other
   * snapshots stored on the cluster
   * @param tableName name of the table to snapshot
   * @param type type of snapshot to take
   * @throws IOException we fail to reach the master
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(final String snapshotName, final String tableName,
      SnapshotDescription.Type type) throws IOException, SnapshotCreationException,
      IllegalArgumentException {
    SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
    builder.setTable(tableName);
    builder.setName(snapshotName);
    builder.setType(type);
    snapshot(builder.build());
  }

  /**
   * Take a snapshot and wait for the server to complete that snapshot (blocking).
   * <p>
   * Only a single snapshot should be taken at a time for an instance of HBase, or results may be
   * undefined (you can tell multiple HBase clusters to snapshot at the same time, but only one at a
   * time for a single cluster).
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link HTableDescriptor#isLegalTableName(byte[])}.
   * <p>
   * You should probably use {@link #snapshot(String, String)} or {@link #snapshot(byte[], byte[])}
   * unless you are sure about the type of snapshot that you want to take.
   * @param snapshot snapshot to take
   * @throws IOException or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot failed to be taken
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(SnapshotDescription snapshot) throws IOException, SnapshotCreationException,
      IllegalArgumentException {
    HSnapshotDescription snapshotWritable = new HSnapshotDescription(snapshot);

    try {
      // actually take the snapshot
      long max = takeSnapshotAsync(snapshot);
      long start = EnvironmentEdgeManager.currentTimeMillis();
      long maxPauseTime = max / this.numRetries;
      boolean done = false;
      int tries = 0;
      LOG.debug("Waiting a max of " + max + " ms for snapshot '" +
          SnapshotDescriptionUtils.toString(snapshot) + "' to complete. (max " +
          maxPauseTime + " ms per retry)");
      while (tries == 0 || (EnvironmentEdgeManager.currentTimeMillis() - start) < max && !done) {
        try {
          // sleep a backoff <= pauseTime amount
          long sleep = getPauseTime(tries++);
          sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
          LOG.debug("(#" + tries + ") Sleeping: " + sleep +
            "ms while waiting for snapshot completion.");
          Thread.sleep(sleep);

        } catch (InterruptedException e) {
          LOG.debug("Interrupted while waiting for snapshot " + snapshot + " to complete");
          Thread.currentThread().interrupt();
        }
        LOG.debug("Getting current status of snapshot from master...");
        done = getMaster().isSnapshotDone(snapshotWritable);
      }

      if (!done) {
        throw new SnapshotCreationException("Snapshot '" + snapshot.getName()
            + "' wasn't completed in expectedTime:" + max + " ms", snapshot);
      }
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Take a snapshot without waiting for the server to complete that snapshot (asynchronous)
   * <p>
   * Only a single snapshot should be taken at a time, or results may be undefined.
   * @param snapshot snapshot to take
   * @return the max time in millis to wait for the snapshot
   * @throws IOException if the snapshot did not succeed or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public long takeSnapshotAsync(SnapshotDescription snapshot) throws IOException,
      SnapshotCreationException {
    SnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    HSnapshotDescription snapshotWritable = new HSnapshotDescription(snapshot);
    return getMaster().snapshot(snapshotWritable);
  }

  /**
   * Check the current state of the passed snapshot.
   * <p>
   * There are three possible states:
   * <ol>
   * <li>running - returns <tt>false</tt></li>
   * <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the snapshot to fail</li>
   * </ol>
   * <p>
   * The cluster only knows about the most recent snapshot. Therefore, if another snapshot has been
   * run/started since the snapshot your are checking, you will recieve an
   * {@link UnknownSnapshotException}.
   * @param snapshot description of the snapshot to check
   * @return <tt>true</tt> if the snapshot is completed, <tt>false</tt> if the snapshot is still
   * running
   * @throws IOException if we have a network issue
   * @throws HBaseSnapshotException if the snapshot failed
   * @throws UnknownSnapshotException if the requested snapshot is unknown
   */
  public boolean isSnapshotFinished(final SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    try {
      return getMaster().isSnapshotDone(new HSnapshotDescription(snapshot));
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled)
   * Before restoring the table, a new snapshot with the current table state is created.
   * In case of failure, the table will be rolled back to the its original state.
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  public void restoreSnapshot(final byte[] snapshotName)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName));
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled)
   * Before restoring the table, a new snapshot with the current table state is created.
   * In case of failure, the table will be rolled back to its original state.
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  public void restoreSnapshot(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    String rollbackSnapshot = snapshotName + "-" + EnvironmentEdgeManager.currentTimeMillis();

    String tableName = null;
    for (SnapshotDescription snapshotInfo: listSnapshots()) {
      if (snapshotInfo.getName().equals(snapshotName)) {
        tableName = snapshotInfo.getTable();
        break;
      }
    }

    if (tableName == null) {
      throw new RestoreSnapshotException(
        "Unable to find the table name for snapshot=" + snapshotName);
    }

    // Take a snapshot of the current state
    snapshot(rollbackSnapshot, tableName);

    // Restore snapshot
    try {
      internalRestoreSnapshot(snapshotName, tableName);
    } catch (IOException e) {
      // Try to rollback
      try {
        String msg = "Restore snapshot=" + snapshotName +
          " failed. Rollback to snapshot=" + rollbackSnapshot + " succeeded.";
        LOG.error(msg, e);
        internalRestoreSnapshot(rollbackSnapshot, tableName);
        throw new RestoreSnapshotException(msg, e);
      } catch (IOException ex) {
        String msg = "Failed to restore and rollback to snapshot=" + rollbackSnapshot;
        LOG.error(msg, ex);
        throw new RestoreSnapshotException(msg, ex);
      }
    }
  }

  /**
   * Create a new table by cloning the snapshot content.
   *
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  public void cloneSnapshot(final byte[] snapshotName, final byte[] tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    cloneSnapshot(Bytes.toString(snapshotName), Bytes.toString(tableName));
  }

  /**
   * Create a new table by cloning the snapshot content.
   *
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  public void cloneSnapshot(final String snapshotName, final String tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    if (tableExists(tableName)) {
      throw new TableExistsException(tableName);
    }
    internalRestoreSnapshot(snapshotName, tableName);
    waitUntilTableIsEnabled(Bytes.toBytes(tableName));
  }

  /**
   * Execute Restore/Clone snapshot and wait for the server to complete (blocking).
   * To check if the cloned table exists, use {@link #isTableAvailable} -- it is not safe to
   * create an HTable instance to this table before it is available.
   * @param snapshot snapshot to restore
   * @param tableName table name to restore the snapshot on
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  private void internalRestoreSnapshot(final String snapshotName, final String tableName)
      throws IOException, RestoreSnapshotException {
    HSnapshotDescription snapshot = new HSnapshotDescription(
      SnapshotDescription.newBuilder().setName(snapshotName).setTable(tableName).build());

    try {
      // actually restore the snapshot
      getMaster().restoreSnapshot(snapshot);

      final long maxPauseTime = 5000;
      boolean done = false;
      int tries = 0;
      while (!done) {
        try {
          // sleep a backoff <= pauseTime amount
          long sleep = getPauseTime(tries++);
          sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
          LOG.debug(tries + ") Sleeping: " + sleep + " ms while we wait for snapshot restore to complete.");
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while waiting for snapshot " + snapshot + " restore to complete");
          Thread.currentThread().interrupt();
        }
        LOG.debug("Getting current status of snapshot restore from master...");
        done = getMaster().isRestoreSnapshotDone(snapshot);
      }
      if (!done) {
        throw new RestoreSnapshotException("Snapshot '" + snapshot.getName() + "' wasn't restored.");
      }
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * List completed snapshots.
   * @return a list of snapshot descriptors for completed snapshots
   * @throws IOException if a network error occurs
   */
  public List<SnapshotDescription> listSnapshots() throws IOException {
    List<SnapshotDescription> snapshots = new LinkedList<SnapshotDescription>();
    try {
      for (HSnapshotDescription snapshot: getMaster().getCompletedSnapshots()) {
        snapshots.add(snapshot.getProto());
      }
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
    return snapshots;
  }

  /**
   * Delete an existing snapshot.
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteSnapshot(final byte[] snapshotName) throws IOException {
    // make sure the snapshot is possibly valid
    HTableDescriptor.isLegalTableName(snapshotName, true);
    // do the delete
    SnapshotDescription snapshot = SnapshotDescription.newBuilder()
      .setName(Bytes.toString(snapshotName)).build();
    try {
      getMaster().deleteSnapshot(new HSnapshotDescription(snapshot));
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Delete an existing snapshot.
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteSnapshot(final String snapshotName) throws IOException {
    deleteSnapshot(Bytes.toBytes(snapshotName));
  }
}
