/**
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.MasterCoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RegionServerCoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MajorCompactionTimestampForRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MajorCompactionTimestampRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

/**
 * HBaseAdmin is no longer a client API. It is marked InterfaceAudience.Private indicating that
 * this is an HBase-internal class as defined in
 * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/InterfaceClassification.html
 * There are no guarantees for backwards source / binary compatibility and methods or class can
 * change or go away without deprecation.
 * Use {@link Connection#getAdmin()} to obtain an instance of {@link Admin} instead of constructing
 * an HBaseAdmin directly.
 *
 * <p>Connection should be an <i>unmanaged</i> connection obtained via
 * {@link ConnectionFactory#createConnection(Configuration)}
 *
 * @see ConnectionFactory
 * @see Connection
 * @see Admin
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HBaseAdmin implements Admin {
  private static final Log LOG = LogFactory.getLog(HBaseAdmin.class);

  private static final String ZK_IDENTIFIER_PREFIX =  "hbase-admin-on-";

  private ClusterConnection connection;

  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Multiply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;
  private final int syncWaitTimeout;
  private boolean aborted;
  private boolean cleanupConnectionOnClose = false; // close the connection in close()
  private boolean closed = false;
  private int operationTimeout;

  private RpcRetryingCallerFactory rpcCallerFactory;

  /**
   * Constructor.
   * See {@link #HBaseAdmin(Connection connection)}
   *
   * @param c Configuration object. Copied internally.
   * @deprecated Constructing HBaseAdmin objects manually has been deprecated.
   * Use {@link Connection#getAdmin()} to obtain an instance of {@link Admin} instead.
   */
  @Deprecated
  public HBaseAdmin(Configuration c)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    this(ConnectionFactory.createConnection(new Configuration(c)));
    this.cleanupConnectionOnClose = true;
  }

  @Override
  public int getOperationTimeout() {
    return operationTimeout;
  }


  /**
   * Constructor for externally managed Connections.
   * The connection to master will be created when required by admin functions.
   *
   * @param connection The Connection instance to use
   * @throws MasterNotRunningException, ZooKeeperConnectionException are not
   *  thrown anymore but kept into the interface for backward api compatibility
   * @deprecated Constructing HBaseAdmin objects manually has been deprecated.
   * Use {@link Connection#getAdmin()} to obtain an instance of {@link Admin} instead.
   */
  @Deprecated
  public HBaseAdmin(Connection connection)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    this((ClusterConnection)connection);
  }

  HBaseAdmin(ClusterConnection connection) {
    this.conf = connection.getConfiguration();
    this.connection = connection;

    this.pause = this.conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.retryLongerMultiplier = this.conf.getInt(
        "hbase.client.retries.longer.multiplier", 10);
    this.operationTimeout = this.conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.syncWaitTimeout = this.conf.getInt(
      "hbase.client.sync.wait.timeout.msec", 10 * 60000); // 10min

    this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(this.conf);
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
  @Override
  public HConnection getConnection() {
    return connection;
  }

  /** @return - true if the master server is running. Throws an exception
   *  otherwise.
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   * @deprecated this has been deprecated without a replacement
   */
  @Deprecated
  public boolean isMasterRunning()
  throws MasterNotRunningException, ZooKeeperConnectionException {
    return connection.isMasterRunning();
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  @Override
  public boolean tableExists(final TableName tableName) throws IOException {
    return executeCallable(new ConnectionCallable<Boolean>(getConnection()) {
      @Override
      public Boolean call(int callTimeout) throws ServiceException, IOException {
        return MetaTableAccessor.tableExists(connection, tableName);
      }
    });
  }

  public boolean tableExists(final byte[] tableName)
  throws IOException {
    return tableExists(TableName.valueOf(tableName));
  }

  public boolean tableExists(final String tableName)
  throws IOException {
    return tableExists(TableName.valueOf(tableName));
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return listTables((Pattern)null, false);
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    return listTables(pattern, false);
  }

  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex), false);
  }

  @Override
  public HTableDescriptor[] listTables(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection()) {
      @Override
      public HTableDescriptor[] call(int callTimeout) throws ServiceException {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(pattern, includeSysTables);
        return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
      }
    });
  }

  @Override
  public HTableDescriptor[] listTables(String regex, boolean includeSysTables)
      throws IOException {
    return listTables(Pattern.compile(regex), includeSysTables);
  }

  /**
   * List all of the names of userspace tables.
   * @return String[] table names
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link Admin#listTableNames()} instead
   */
  @Deprecated
  public String[] getTableNames() throws IOException {
    TableName[] tableNames = listTableNames();
    String[] result = new String[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      result[i] = tableNames[i].getNameAsString();
    }
    return result;
  }

  /**
   * List all of the names of userspace tables matching the given regular expression.
   * @param pattern The regular expression to match against
   * @return String[] table names
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link Admin#listTableNames(Pattern)} instead.
   */
  @Deprecated
  public String[] getTableNames(Pattern pattern) throws IOException {
    TableName[] tableNames = listTableNames(pattern);
    String[] result = new String[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      result[i] = tableNames[i].getNameAsString();
    }
    return result;
  }

  /**
   * List all of the names of userspace tables matching the given regular expression.
   * @param regex The regular expression to match against
   * @return String[] table names
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link Admin#listTableNames(Pattern)} instead.
   */
  @Deprecated
  public String[] getTableNames(String regex) throws IOException {
    return getTableNames(Pattern.compile(regex));
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return listTableNames((Pattern)null, false);
  }

  @Override
  public TableName[] listTableNames(Pattern pattern) throws IOException {
    return listTableNames(pattern, false);
  }

  @Override
  public TableName[] listTableNames(String regex) throws IOException {
    return listTableNames(Pattern.compile(regex), false);
  }

  @Override
  public TableName[] listTableNames(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return executeCallable(new MasterCallable<TableName[]>(getConnection()) {
      @Override
      public TableName[] call(int callTimeout) throws ServiceException {
        GetTableNamesRequest req =
            RequestConverter.buildGetTableNamesRequest(pattern, includeSysTables);
        return ProtobufUtil.getTableNameArray(master.getTableNames(null, req)
            .getTableNamesList());
      }
    });
  }

  @Override
  public TableName[] listTableNames(final String regex, final boolean includeSysTables)
      throws IOException {
    return listTableNames(Pattern.compile(regex), includeSysTables);
  }

  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws TableNotFoundException
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public HTableDescriptor getTableDescriptor(final TableName tableName)
  throws TableNotFoundException, IOException {
    if (tableName == null) return null;
    HTableDescriptor htd = executeCallable(new MasterCallable<HTableDescriptor>(getConnection()) {
      @Override
      public HTableDescriptor call(int callTimeout) throws ServiceException {
        GetTableDescriptorsResponse htds;
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(tableName);
        htds = master.getTableDescriptors(null, req);

        if (!htds.getTableSchemaList().isEmpty()) {
          return HTableDescriptor.convert(htds.getTableSchemaList().get(0));
        }
        return null;
      }
    });
    if (htd != null) {
      return htd;
    }
    throw new TableNotFoundException(tableName.getNameAsString());
  }

  public HTableDescriptor getTableDescriptor(final byte[] tableName)
  throws TableNotFoundException, IOException {
    return getTableDescriptor(TableName.valueOf(tableName));
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
  @Override
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
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  @Override
  public void createTable(HTableDescriptor desc, byte [] startKey,
      byte [] endKey, int numRegions)
  throws IOException {
    if(numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if(Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      createTable(desc, new byte[][]{startKey, endKey});
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
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  @Override
  public void createTable(final HTableDescriptor desc, byte [][] splitKeys)
      throws IOException {
    Future<Void> future = createTableAsync(desc, splitKeys);
    try {
      // TODO: how long should we wait? spin forever?
      future.get(syncWaitTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting" +
          " for table to be enabled; meta scan was done");
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  /**
   * Creates a new table but does not block and wait for it to come online.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param desc table descriptor for table
   * @param splitKeys keys to check if the table has been created with all split keys
   * @throws IllegalArgumentException Bad table name, if the split keys
   *    are repeated and if the split key has empty byte array.
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async creation. You can use Future.get(long, TimeUnit)
   *    to wait on the operation to complete.
   */
  @Override
  public Future<Void> createTableAsync(final HTableDescriptor desc, final byte[][] splitKeys)
      throws IOException {
    if (desc.getTableName() == null) {
      throw new IllegalArgumentException("TableName cannot be null");
    }
    if (splitKeys != null && splitKeys.length > 0) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
          throw new IllegalArgumentException(
              "Empty split key must not be passed in the split keys.");
        }
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, " +
            "found duplicate: " + Bytes.toStringBinary(splitKey) +
            ", " + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
      }
    }

    CreateTableResponse response = executeCallable(
        new MasterCallable<CreateTableResponse>(getConnection()) {
      @Override
      public CreateTableResponse call(int callTimeout) throws ServiceException {
        CreateTableRequest request = RequestConverter.buildCreateTableRequest(desc, splitKeys);
        return master.createTable(null, request);
      }
    });
    return new CreateTableFuture(this, desc, splitKeys, response);
  }

  private static class CreateTableFuture extends TableFuture<Void> {
    private final HTableDescriptor desc;
    private final byte[][] splitKeys;

    public CreateTableFuture(final HBaseAdmin admin, final HTableDescriptor desc,
        final byte[][] splitKeys, final CreateTableResponse response) {
      super(admin, desc.getTableName(),
              (response != null && response.hasProcId()) ? response.getProcId() : null);
      this.splitKeys = splitKeys;
      this.desc = desc;
    }

    @Override
    protected HTableDescriptor getTableDescriptor() {
      return desc;
    }

    @Override
    protected String getDescription() {
      return "Creating " + desc.getNameAsString();
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      waitForAllRegionsOnline(deadlineTs, splitKeys);
      return null;
    }
  }

  public void deleteTable(final String tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  public void deleteTable(final byte[] tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void deleteTable(final TableName tableName) throws IOException {
    Future<Void> future = deleteTableAsync(tableName);
    try {
      future.get(syncWaitTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for table to be deleted");
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  /**
   * Deletes the table but does not block and wait for it be completely removed.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async delete. You can use Future.get(long, TimeUnit)
   *    to wait on the operation to complete.
   */
  @Override
  public Future<Void> deleteTableAsync(final TableName tableName) throws IOException {
    DeleteTableResponse response = executeCallable(
        new MasterCallable<DeleteTableResponse>(getConnection()) {
      @Override
      public DeleteTableResponse call(int callTimeout) throws ServiceException {
        DeleteTableRequest req = RequestConverter.buildDeleteTableRequest(tableName);
        return master.deleteTable(null,req);
      }
    });
    return new DeleteTableFuture(this, tableName, response);
  }

  private static class DeleteTableFuture extends TableFuture<Void> {
    public DeleteTableFuture(final HBaseAdmin admin, final TableName tableName,
        final DeleteTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    protected String getDescription() {
      return "Deleting " + getTableName();
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      waitTableNotFound(deadlineTs);
      return null;
    }

    @Override
    protected Void postOperationResult(final Void result, final long deadlineTs)
        throws IOException, TimeoutException {
      // Delete cached information to prevent clients from using old locations
      getAdmin().getConnection().clearRegionCache(getTableName());
      return super.postOperationResult(result, deadlineTs);
    }
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
  @Override
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
  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      try {
        deleteTable(table.getTableName());
      } catch (IOException ex) {
        LOG.info("Failed to delete table " + table.getTableName(), ex);
        failed.add(table);
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  /**
   * Truncate a table. Synchronous operation.
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void truncateTable(final TableName tableName, final boolean preserveSplits)
      throws IOException {
    Future<Void> future = truncateTableAsync(tableName, preserveSplits);
    try {
      future.get(syncWaitTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for table " + tableName
          + " to be enabled.");
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  /**
   * Truncate the table but does not block and wait for it be completely enabled. You can use
   * Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete. Asynchronous
   * operation.
   * @param tableName name of table to delete
   * @param preserveSplits true if the splits should be preserved
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async truncate. You can use Future.get(long, TimeUnit) to wait on the
   *         operation to complete.
   */
  @Override
  public Future<Void> truncateTableAsync(final TableName tableName, final boolean preserveSplits)
      throws IOException {
    TruncateTableResponse response =
        executeCallable(new MasterCallable<TruncateTableResponse>(getConnection()) {
          @Override
          public TruncateTableResponse call(int callTimeout) throws ServiceException {
            LOG.info("Started enable of " + tableName);
            TruncateTableRequest req =
                RequestConverter.buildTruncateTableRequest(tableName, preserveSplits);
            return master.truncateTable(null, req);
          }
        });
    return new TruncateTableFuture(this, tableName, preserveSplits, response);
  }

  private static class TruncateTableFuture extends TableFuture<Void> {
    private final boolean preserveSplits;

    public TruncateTableFuture(final HBaseAdmin admin, final TableName tableName,
        final boolean preserveSplits, final TruncateTableResponse response) {
      super(admin, tableName,
             (response != null && response.hasProcId()) ? response.getProcId() : null);
      this.preserveSplits = preserveSplits;
    }

    @Override
    public String getDescription() {
      return "Truncating " + getTableName();
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      // once the table is enabled, we know the operation is done. so we can fetch the splitKeys
      byte[][] splitKeys = preserveSplits ? getAdmin().getTableSplits(getTableName()) : null;
      waitForAllRegionsOnline(deadlineTs, splitKeys);
      return null;
    }
  }

  private byte[][] getTableSplits(final TableName tableName) throws IOException {
    byte[][] splits = null;
    try (RegionLocator locator = getConnection().getRegionLocator(tableName)) {
      byte[][] startKeys = locator.getStartKeys();
      if (startKeys.length == 1) {
        return splits;
      }
      splits = new byte[startKeys.length - 1][];
      for (int i = 1; i < startKeys.length; i++) {
        splits[i - 1] = startKeys[i];
      }
    }
    return splits;
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
  @Override
  public void enableTable(final TableName tableName)
  throws IOException {
    Future<Void> future = enableTableAsync(tableName);
    try {
      future.get(syncWaitTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for table to be disabled");
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  public void enableTable(final byte[] tableName)
  throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  public void enableTable(final String tableName)
  throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  /**
   * Wait for the table to be enabled and available
   * If enabling the table exceeds the retry period, an exception is thrown.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs or
   *    table is not enabled after the retries period.
   */
  private void waitUntilTableIsEnabled(final TableName tableName) throws IOException {
    boolean enabled = false;
    long start = EnvironmentEdgeManager.currentTime();
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      try {
        enabled = isTableEnabled(tableName);
      } catch (TableNotFoundException tnfe) {
        // wait for table to be created
        enabled = false;
      }
      enabled = enabled && isTableAvailable(tableName);
      if (enabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
          "enabled in " + tableName);
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
    }
    if (!enabled) {
      long msec = EnvironmentEdgeManager.currentTime() - start;
      throw new IOException("Table '" + tableName +
        "' not yet enabled, after " + msec + "ms.");
    }
  }

  public void enableTableAsync(final byte[] tableName)
  throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  public void enableTableAsync(final String tableName)
  throws IOException {
    enableTableAsync(TableName.valueOf(tableName));
  }

  /**
   * Enable the table but does not block and wait for it be completely enabled.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async enable. You can use Future.get(long, TimeUnit)
   *    to wait on the operation to complete.
   */
  @Override
  public Future<Void> enableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    EnableTableResponse response = executeCallable(
        new MasterCallable<EnableTableResponse>(getConnection()) {
      @Override
      public EnableTableResponse call(int callTimeout) throws ServiceException {
        LOG.info("Started enable of " + tableName);
        EnableTableRequest req = RequestConverter.buildEnableTableRequest(tableName);
        return master.enableTable(null,req);
      }
    });
    return new EnableTableFuture(this, tableName, response);
  }

  private static class EnableTableFuture extends TableFuture<Void> {
    public EnableTableFuture(final HBaseAdmin admin, final TableName tableName,
        final EnableTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    protected String getDescription() {
      return "Enabling " + getTableName();
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      return null;
    }
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
  @Override
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
  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableDisabled(table.getTableName())) {
        try {
          enableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to enable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  public void disableTableAsync(final byte[] tableName) throws IOException {
    disableTableAsync(TableName.valueOf(tableName));
  }

  public void disableTableAsync(final String tableName) throws IOException {
    disableTableAsync(TableName.valueOf(tableName));
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
  @Override
  public void disableTable(final TableName tableName)
  throws IOException {
    Future<Void> future = disableTableAsync(tableName);
    try {
      future.get(syncWaitTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for table to be disabled");
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  public void disableTable(final byte[] tableName)
  throws IOException {
    disableTable(TableName.valueOf(tableName));
  }

  public void disableTable(final String tableName)
  throws IOException {
    disableTable(TableName.valueOf(tableName));
  }

  /**
   * Disable the table but does not block and wait for it be completely disabled.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async disable. You can use Future.get(long, TimeUnit)
   *    to wait on the operation to complete.
   */
  @Override
  public Future<Void> disableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    DisableTableResponse response = executeCallable(
        new MasterCallable<DisableTableResponse>(getConnection()) {
      @Override
      public DisableTableResponse call(int callTimeout) throws ServiceException {
        LOG.info("Started disable of " + tableName);
        DisableTableRequest req = RequestConverter.buildDisableTableRequest(tableName);
        return master.disableTable(null, req);
      }
    });
    return new DisableTableFuture(this, tableName, response);
  }

  private static class DisableTableFuture extends TableFuture<Void> {
    public DisableTableFuture(final HBaseAdmin admin, final TableName tableName,
        final DisableTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    protected String getDescription() {
      return "Disabling " + getTableName();
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForTableDisabled(deadlineTs);
      return null;
    }
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
  @Override
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
  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableEnabled(table.getTableName())) {
        try {
          disableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to disable table " + table.getTableName(), ex);
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
  private void checkTableExistence(TableName tableName) throws IOException {
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public boolean isTableEnabled(final TableName tableName) throws IOException {
    checkTableExistence(tableName);
    return executeCallable(new ConnectionCallable<Boolean>(getConnection()) {
      @Override
      public Boolean call(int callTimeout) throws ServiceException, IOException {
        TableState tableState = MetaTableAccessor.getTableState(connection, tableName);
        if (tableState == null)
          throw new TableNotFoundException(tableName);
        return tableState.inStates(TableState.State.ENABLED);
      }
    });
  }

  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }



  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    checkTableExistence(tableName);
    return connection.isTableDisabled(tableName);
  }

  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  public boolean isTableDisabled(String tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return connection.isTableAvailable(tableName);
  }

  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName));
  }

  public boolean isTableAvailable(String tableName) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName));
  }

  /**
   * Use this api to check if the table has been created with the specified number of
   * splitkeys which was used while creating the given table.
   * Note : If this api is used after a table's region gets splitted, the api may return
   * false.
   * @param tableName
   *          name of table to check
   * @param splitKeys
   *          keys to check if the table has been created with all split keys
   * @throws IOException
   *           if a remote or network excpetion occurs
   */
  @Override
  public boolean isTableAvailable(TableName tableName,
                                  byte[][] splitKeys) throws IOException {
    return connection.isTableAvailable(tableName, splitKeys);
  }

  public boolean isTableAvailable(byte[] tableName,
                                  byte[][] splitKeys) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName), splitKeys);
  }

  public boolean isTableAvailable(String tableName,
                                  byte[][] splitKeys) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName), splitKeys);
  }

  /**
   * Get the status of alter command - indicates how many regions have received
   * the updated schema Asynchronous operation.
   *
   * @param tableName TableName instance
   * @return Pair indicating the number of regions updated Pair.getFirst() is the
   *         regions that are yet to be updated Pair.getSecond() is the total number
   *         of regions of the table
   * @throws IOException
   *           if a remote or network exception occurs
   */
  @Override
  public Pair<Integer, Integer> getAlterStatus(final TableName tableName)
  throws IOException {
    return executeCallable(new MasterCallable<Pair<Integer, Integer>>(getConnection()) {
      @Override
      public Pair<Integer, Integer> call(int callTimeout) throws ServiceException {
        GetSchemaAlterStatusRequest req = RequestConverter
            .buildGetSchemaAlterStatusRequest(tableName);
        GetSchemaAlterStatusResponse ret = master.getSchemaAlterStatus(null, req);
        Pair<Integer, Integer> pair = new Pair<Integer, Integer>(Integer.valueOf(ret
            .getYetToUpdateRegions()), Integer.valueOf(ret.getTotalRegions()));
        return pair;
      }
    });
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
  @Override
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName)
   throws IOException {
    return getAlterStatus(TableName.valueOf(tableName));
  }

  /**
   * Add a column family to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #addColumnFamily(TableName, HColumnDescriptor)}.
   */
  @Deprecated
  public void addColumn(final byte[] tableName, HColumnDescriptor columnFamily)
  throws IOException {
    addColumnFamily(TableName.valueOf(tableName), columnFamily);
  }

  /**
   * Add a column family to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #addColumnFamily(TableName, HColumnDescriptor)}.
   */
  @Deprecated
  public void addColumn(final String tableName, HColumnDescriptor columnFamily)
  throws IOException {
    addColumnFamily(TableName.valueOf(tableName), columnFamily);
  }

  /**
   * Add a column family to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #addColumnFamily(TableName, HColumnDescriptor)}.
   */
  @Override
  @Deprecated
  public void addColumn(final TableName tableName, final HColumnDescriptor columnFamily)
  throws IOException {
    addColumnFamily(tableName, columnFamily);
  }

  @Override
  public void addColumnFamily(final TableName tableName, final HColumnDescriptor columnFamily)
  throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        AddColumnRequest req = RequestConverter.buildAddColumnRequest(tableName, columnFamily);
        master.addColumn(null, req);
        return null;
      }
    });
  }

  /**
   * Delete a column family from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #deleteColumnFamily(TableName, byte[])}.
   */
  @Deprecated
  public void deleteColumn(final byte[] tableName, final String columnFamily)
  throws IOException {
    deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(columnFamily));
  }

  /**
   * Delete a column family from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #deleteColumnFamily(TableName, byte[])}.
   */
  @Deprecated
  public void deleteColumn(final String tableName, final String columnFamily)
  throws IOException {
    deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(columnFamily));
  }

  /**
   * Delete a column family from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #deleteColumnFamily(TableName, byte[])}.
   */
  @Override
  @Deprecated
  public void deleteColumn(final TableName tableName, final byte[] columnFamily)
  throws IOException {
    deleteColumnFamily(tableName, columnFamily);
  }

  @Override
  public void deleteColumnFamily(final TableName tableName, final byte[] columnFamily)
  throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        DeleteColumnRequest req =
          RequestConverter.buildDeleteColumnRequest(tableName, columnFamily);
        master.deleteColumn(null, req);
        return null;
      }
    });
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #modifyColumnFamily(TableName, HColumnDescriptor)}.
   */
  @Deprecated
  public void modifyColumn(final String tableName, HColumnDescriptor columnFamily)
  throws IOException {
    modifyColumnFamily(TableName.valueOf(tableName), columnFamily);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #modifyColumnFamily(TableName, HColumnDescriptor)}.
   */
  @Deprecated
  public void modifyColumn(final byte[] tableName, HColumnDescriptor columnFamily)
  throws IOException {
    modifyColumnFamily(TableName.valueOf(tableName), columnFamily);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0.
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-1989">HBASE-1989</a>).
   *             This will be removed in HBase 3.0.0.
   *             Use {@link #modifyColumnFamily(TableName, HColumnDescriptor)}.
   */
  @Override
  @Deprecated
  public void modifyColumn(final TableName tableName, final HColumnDescriptor columnFamily)
  throws IOException {
    modifyColumnFamily(tableName, columnFamily);
  }

  @Override
  public void modifyColumnFamily(final TableName tableName, final HColumnDescriptor columnFamily)
  throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        ModifyColumnRequest req =
          RequestConverter.buildModifyColumnRequest(tableName, columnFamily);
        master.modifyColumn(null,req);
        return null;
      }
    });
  }

  /**
   * Close a region. For expert-admins.  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param serverName If supplied, we'll use this location rather than
   * the one currently in <code>hbase:meta</code>
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void closeRegion(final String regionname, final String serverName)
  throws IOException {
    closeRegion(Bytes.toBytes(regionname), serverName);
  }

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The
   * master will not be informed of the close.
   * @param regionname region name to close
   * @param serverName The servername of the regionserver.  If passed null we
   * will use servername found in the hbase:meta table. A server name
   * is made of host, port and startcode.  Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void closeRegion(final byte [] regionname, final String serverName)
      throws IOException {
    if (serverName != null) {
      Pair<HRegionInfo, ServerName> pair = MetaTableAccessor.getRegion(connection, regionname);
      if (pair == null || pair.getFirst() == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionname));
      } else {
        closeRegion(ServerName.valueOf(serverName), pair.getFirst());
      }
    } else {
      Pair<HRegionInfo, ServerName> pair = MetaTableAccessor.getRegion(connection, regionname);
      if (pair == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionname));
      } else if (pair.getSecond() == null) {
        throw new NoServerForRegionException(Bytes.toStringBinary(regionname));
      } else {
        closeRegion(pair.getSecond(), pair.getFirst());
      }
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
  @Override
  public boolean closeRegionWithEncodedRegionName(final String encodedRegionName,
      final String serverName) throws IOException {
    if (null == serverName || ("").equals(serverName.trim())) {
      throw new IllegalArgumentException(
          "The servername cannot be null or empty.");
    }
    ServerName sn = ServerName.valueOf(serverName);
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // Close the region without updating zk state.
    CloseRegionRequest request =
      RequestConverter.buildCloseRegionRequest(sn, encodedRegionName);
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
  @Override
  public void closeRegion(final ServerName sn, final HRegionInfo hri)
  throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // Close the region without updating zk state.
    ProtobufUtil.closeRegion(admin, sn, hri.getRegionName());
  }

  /**
   * Get all the online regions on a region server.
   */
  @Override
  public List<HRegionInfo> getOnlineRegions(final ServerName sn) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    return ProtobufUtil.getOnlineRegions(admin);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush(final TableName tableName) throws IOException {
    checkTableExists(tableName);
    if (isTableDisabled(tableName)) {
      LOG.info("Table is disabled: " + tableName.getNameAsString());
      return;
    }
    execProcedure("flush-table-proc", tableName.getNameAsString(),
      new HashMap<String, String>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushRegion(final byte[] regionName) throws IOException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Unknown regionname: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    flush(regionServerPair.getSecond(), regionServerPair.getFirst());
  }

  /**
   * @deprecated Use {@link #flush(org.apache.hadoop.hbase.TableName)} or {@link #flushRegion
   * (byte[])} instead.
   */
  @Deprecated
  public void flush(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    flush(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * @deprecated Use {@link #flush(org.apache.hadoop.hbase.TableName)} or {@link #flushRegion
   * (byte[])} instead.
   */
  @Deprecated
  public void flush(final byte[] tableNameOrRegionName)
  throws IOException, InterruptedException {
    try {
      flushRegion(tableNameOrRegionName);
    } catch (IllegalArgumentException e) {
      // Unknown region.  Try table.
      flush(TableName.valueOf(tableNameOrRegionName));
    }
  }

  private void flush(final ServerName sn, final HRegionInfo hri)
  throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    FlushRegionRequest request =
      RequestConverter.buildFlushRegionRequest(hri.getRegionName());
    try {
      admin.flushRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName)
    throws IOException {
    compact(tableName, null, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compactRegion(final byte[] regionName)
    throws IOException {
    compactRegion(regionName, null, false);
  }

  /**
   * @deprecated Use {@link #compact(org.apache.hadoop.hbase.TableName)} or {@link #compactRegion
   * (byte[])} instead.
   */
  @Deprecated
  public void compact(final String tableNameOrRegionName)
  throws IOException {
    compact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * @deprecated Use {@link #compact(org.apache.hadoop.hbase.TableName)} or {@link #compactRegion
   * (byte[])} instead.
   */
  @Deprecated
  public void compact(final byte[] tableNameOrRegionName)
  throws IOException {
    try {
      compactRegion(tableNameOrRegionName, null, false);
    } catch (IllegalArgumentException e) {
      compact(TableName.valueOf(tableNameOrRegionName), null, false);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName, final byte[] columnFamily)
    throws IOException {
    compact(tableName, columnFamily, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compactRegion(final byte[] regionName, final byte[] columnFamily)
    throws IOException {
    compactRegion(regionName, columnFamily, false);
  }

  /**
   * @deprecated Use {@link #compact(org.apache.hadoop.hbase.TableName)} or {@link #compactRegion
   * (byte[], byte[])} instead.
   */
  @Deprecated
  public void compact(String tableOrRegionName, String columnFamily)
    throws IOException {
    compact(Bytes.toBytes(tableOrRegionName), Bytes.toBytes(columnFamily));
  }

  /**
   * @deprecated Use {@link #compact(org.apache.hadoop.hbase.TableName)} or {@link #compactRegion
   * (byte[], byte[])} instead.
   */
  @Deprecated
  public void compact(final byte[] tableNameOrRegionName, final byte[] columnFamily)
  throws IOException {
    try {
      compactRegion(tableNameOrRegionName, columnFamily, false);
    } catch (IllegalArgumentException e) {
      // Bad region, try table
      compact(TableName.valueOf(tableNameOrRegionName), columnFamily, false);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compactRegionServer(final ServerName sn, boolean major)
  throws IOException, InterruptedException {
    for (HRegionInfo region : getOnlineRegions(sn)) {
      compact(sn, region, major, null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName)
  throws IOException {
    compact(tableName, null, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompactRegion(final byte[] regionName)
  throws IOException {
    compactRegion(regionName, null, true);
  }

  /**
   * @deprecated Use {@link #majorCompact(org.apache.hadoop.hbase.TableName)} or {@link
   * #majorCompactRegion(byte[])} instead.
   */
  @Deprecated
  public void majorCompact(final String tableNameOrRegionName)
  throws IOException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * @deprecated Use {@link #majorCompact(org.apache.hadoop.hbase.TableName)} or {@link
   * #majorCompactRegion(byte[])} instead.
   */
  @Deprecated
  public void majorCompact(final byte[] tableNameOrRegionName)
  throws IOException {
    try {
      compactRegion(tableNameOrRegionName, null, true);
    } catch (IllegalArgumentException e) {
      // Invalid region, try table
      compact(TableName.valueOf(tableNameOrRegionName), null, true);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName, final byte[] columnFamily)
  throws IOException {
    compact(tableName, columnFamily, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompactRegion(final byte[] regionName, final byte[] columnFamily)
  throws IOException {
    compactRegion(regionName, columnFamily, true);
  }

  /**
   * @deprecated Use {@link #majorCompact(org.apache.hadoop.hbase.TableName,
   * byte[])} or {@link #majorCompactRegion(byte[], byte[])} instead.
   */
  @Deprecated
  public void majorCompact(final String tableNameOrRegionName, final String columnFamily)
  throws IOException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(columnFamily));
  }

  /**
   * @deprecated Use {@link #majorCompact(org.apache.hadoop.hbase.TableName,
   * byte[])} or {@link #majorCompactRegion(byte[], byte[])} instead.
   */
  @Deprecated
  public void majorCompact(final byte[] tableNameOrRegionName, final byte[] columnFamily)
  throws IOException {
    try {
      compactRegion(tableNameOrRegionName, columnFamily, true);
    } catch (IllegalArgumentException e) {
      // Invalid region, try table
      compact(TableName.valueOf(tableNameOrRegionName), columnFamily, true);
    }
  }

  /**
   * Compact a table.
   * Asynchronous operation.
   *
   * @param tableName table or region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  private void compact(final TableName tableName, final byte[] columnFamily,final boolean major)
  throws IOException {
    ZooKeeperWatcher zookeeper = null;
    try {
      checkTableExists(tableName);
      zookeeper = new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
          new ThrowableAbortable());
      List<Pair<HRegionInfo, ServerName>> pairs;
      if (TableName.META_TABLE_NAME.equals(tableName)) {
        pairs = new MetaTableLocator().getMetaRegionsAndLocations(zookeeper);
      } else {
        pairs = MetaTableAccessor.getTableRegionsAndLocations(connection, tableName);
      }
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
    } finally {
      if (zookeeper != null) {
        zookeeper.close();
      }
    }
  }

  /**
   * Compact an individual region.
   * Asynchronous operation.
   *
   * @param regionName region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  private void compactRegion(final byte[] regionName, final byte[] columnFamily,final boolean major)
  throws IOException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    compact(regionServerPair.getSecond(), regionServerPair.getFirst(), major, columnFamily);
  }

  private void compact(final ServerName sn, final HRegionInfo hri,
      final boolean major, final byte [] family)
  throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    CompactRegionRequest request =
      RequestConverter.buildCompactRegionRequest(hri.getRegionName(), major, family);
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
   */
  @Override
  public void move(final byte [] encodedRegionName, final byte [] destServerName)
      throws IOException {

    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        try {
          MoveRegionRequest request =
              RequestConverter.buildMoveRegionRequest(encodedRegionName, destServerName);
            master.moveRegion(null, request);
        } catch (DeserializationException de) {
          LOG.error("Could not parse destination server name: " + de);
          throw new ServiceException(new DoNotRetryIOException(de));
        }
        return null;
      }
    });
  }

  /**
   * @param regionName
   *          Region name to assign.
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  @Override
  public void assign(final byte[] regionName) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    final byte[] toBeAssigned = getRegionName(regionName);
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        AssignRegionRequest request =
          RequestConverter.buildAssignRegionRequest(toBeAssigned);
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
  @Override
  public void unassign(final byte [] regionName, final boolean force)
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    final byte[] toBeUnassigned = getRegionName(regionName);
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        UnassignRegionRequest request =
          RequestConverter.buildUnassignRegionRequest(toBeUnassigned, force);
        master.unassignRegion(null, request);
        return null;
      }
    });
  }

  /**
   * Offline specified region from master's in-memory state. It will not attempt to reassign the
   * region as in unassign. This API can be used when a region not served by any region server and
   * still online as per Master's in memory state. If this API is incorrectly used on active region
   * then master will loose track of that region.
   *
   * This is a special method that should be used by experts or hbck.
   *
   * @param regionName
   *          Region to offline.
   * @throws IOException
   */
  @Override
  public void offline(final byte [] regionName)
  throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        master.offlineRegion(null,RequestConverter.buildOfflineRegionRequest(regionName));
        return null;
      }
    });
  }

  /**
   * Turn the load balancer on or off.
   * @param on If true, enable balancer. If false, disable balancer.
   * @param synchronous If true, it waits until current balance() call, if outstanding, to return.
   * @return Previous balancer value
   */
  @Override
  public boolean setBalancerRunning(final boolean on, final boolean synchronous)
  throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      public Boolean call(int callTimeout) throws ServiceException {
        SetBalancerRunningRequest req =
            RequestConverter.buildSetBalancerRunningRequest(on, synchronous);
        return master.setBalancerRunning(null, req).getPrevBalanceValue();
      }
    });
  }

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will
   * go ahead and do the reassignments.  Can NOT run for various reasons.  Check
   * logs.
   * @return True if balancer ran, false otherwise.
   */
  @Override
  public boolean balancer() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      public Boolean call(int callTimeout) throws ServiceException {
        return master.balance(null, RequestConverter.buildBalanceRequest()).getBalancerRan();
      }
    });
  }

  /**
   * Query the state of the balancer from the Master. It's not a guarantee that the balancer is
   * actually running this very moment, but that it will run.
   *
   * @return True if the balancer is enabled, false otherwise.
   */
  @Override
  public boolean isBalancerEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      public Boolean call(int callTimeout) throws ServiceException {
        return master.isBalancerEnabled(null, RequestConverter.buildIsBalancerEnabledRequest())
            .getEnabled();
      }
    });
  }

  /**
   * Enable/Disable the catalog janitor
   * @param enable if true enables the catalog janitor
   * @return the previous state
   * @throws MasterNotRunningException
   */
  @Override
  public boolean enableCatalogJanitor(final boolean enable)
      throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      public Boolean call(int callTimeout) throws ServiceException {
        return master.enableCatalogJanitor(null,
          RequestConverter.buildEnableCatalogJanitorRequest(enable)).getPrevValue();
      }
    });
  }

  /**
   * Ask for a scan of the catalog table
   * @return the number of entries cleaned
   * @throws MasterNotRunningException
   */
  @Override
  public int runCatalogScan() throws IOException {
    return executeCallable(new MasterCallable<Integer>(getConnection()) {
      @Override
      public Integer call(int callTimeout) throws ServiceException {
        return master.runCatalogScan(null,
          RequestConverter.buildCatalogScanRequest()).getScanResult();
      }
    });
  }

  /**
   * Query on the catalog janitor state (Enabled/Disabled?)
   * @throws org.apache.hadoop.hbase.MasterNotRunningException
   */
  @Override
  public boolean isCatalogJanitorEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      public Boolean call(int callTimeout) throws ServiceException {
        return master.isCatalogJanitorEnabled(null,
          RequestConverter.buildIsCatalogJanitorEnabledRequest()).getValue();
      }
    });
  }

  /**
   * Merge two regions. Asynchronous operation.
   * @param encodedNameOfRegionA encoded name of region a
   * @param encodedNameOfRegionB encoded name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException
   */
  @Override
  public void mergeRegions(final byte[] encodedNameOfRegionA,
      final byte[] encodedNameOfRegionB, final boolean forcible)
      throws IOException {
    Pair<HRegionInfo, ServerName> pair = getRegion(encodedNameOfRegionA);
    if (pair != null && pair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID)
      throw new IllegalArgumentException("Can't invoke merge on non-default regions directly");
    pair = getRegion(encodedNameOfRegionB);
    if (pair != null && pair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID)
      throw new IllegalArgumentException("Can't invoke merge on non-default regions directly");
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        try {
          DispatchMergingRegionsRequest request = RequestConverter
              .buildDispatchMergingRegionsRequest(encodedNameOfRegionA,
                encodedNameOfRegionB, forcible);
          master.dispatchMergingRegions(null, request);
        } catch (DeserializationException de) {
          LOG.error("Could not parse destination server name: " + de);
        }
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void split(final TableName tableName)
    throws IOException {
    split(tableName, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void splitRegion(final byte[] regionName)
    throws IOException {
    splitRegion(regionName, null);
  }

  /**
   * @deprecated Use {@link #split(org.apache.hadoop.hbase.TableName)} or {@link #splitRegion
   * (byte[])} instead.
   */
  @Deprecated
  public void split(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    split(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * @deprecated Use {@link #split(org.apache.hadoop.hbase.TableName)} or {@link #splitRegion
   * (byte[])} instead.
   */
  @Deprecated
  public void split(final byte[] tableNameOrRegionName)
  throws IOException, InterruptedException {
    split(tableNameOrRegionName, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void split(final TableName tableName, final byte [] splitPoint)
  throws IOException {
    ZooKeeperWatcher zookeeper = null;
    try {
      checkTableExists(tableName);
      zookeeper = new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
        new ThrowableAbortable());
      List<Pair<HRegionInfo, ServerName>> pairs;
      if (TableName.META_TABLE_NAME.equals(tableName)) {
        pairs = new MetaTableLocator().getMetaRegionsAndLocations(zookeeper);
      } else {
        pairs = MetaTableAccessor.getTableRegionsAndLocations(connection, tableName);
      }
      for (Pair<HRegionInfo, ServerName> pair: pairs) {
        // May not be a server for a particular row
        if (pair.getSecond() == null) continue;
        HRegionInfo r = pair.getFirst();
        // check for parents
        if (r.isSplitParent()) continue;
        // if a split point given, only split that particular region
        if (r.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID ||
           (splitPoint != null && !r.containsRow(splitPoint))) continue;
        // call out to region server to do split now
        split(pair.getSecond(), pair.getFirst(), splitPoint);
      }
    } finally {
      if (zookeeper != null) {
        zookeeper.close();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void splitRegion(final byte[] regionName, final byte [] splitPoint)
  throws IOException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getFirst() != null &&
        regionServerPair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException("Can't split replicas directly. "
          + "Replicas are auto-split when their primary is split.");
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    split(regionServerPair.getSecond(), regionServerPair.getFirst(), splitPoint);
  }

  /**
   * @deprecated Use {@link #split(org.apache.hadoop.hbase.TableName,
   * byte[])} or {@link #splitRegion(byte[], byte[])} instead.
   */
  @Deprecated
  public void split(final String tableNameOrRegionName,
    final String splitPoint) throws IOException {
    split(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(splitPoint));
  }

  /**
   * @deprecated Use {@link #split(org.apache.hadoop.hbase.TableName,
   * byte[])} or {@link #splitRegion(byte[], byte[])} instead.
   */
  @Deprecated
  public void split(final byte[] tableNameOrRegionName,
      final byte [] splitPoint) throws IOException {
    try {
      splitRegion(tableNameOrRegionName, splitPoint);
    } catch (IllegalArgumentException e) {
      // Bad region, try table
      split(TableName.valueOf(tableNameOrRegionName), splitPoint);
    }
  }

  @VisibleForTesting
  public void split(final ServerName sn, final HRegionInfo hri,
      byte[] splitPoint) throws IOException {
    if (hri.getStartKey() != null && splitPoint != null &&
         Bytes.compareTo(hri.getStartKey(), splitPoint) == 0) {
       throw new IOException("should not give a splitkey which equals to startkey!");
    }
    // TODO: This is not executed via retries
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
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
  @Override
  public void modifyTable(final TableName tableName, final HTableDescriptor htd)
  throws IOException {
    if (!tableName.equals(htd.getTableName())) {
      throw new IllegalArgumentException("the specified table name '" + tableName +
        "' doesn't match with the HTD one: " + htd.getTableName());
    }

    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        ModifyTableRequest request = RequestConverter.buildModifyTableRequest(tableName, htd);
        master.modifyTable(null, request);
        return null;
      }
    });
  }

  public void modifyTable(final byte[] tableName, final HTableDescriptor htd)
  throws IOException {
    modifyTable(TableName.valueOf(tableName), htd);
  }

  public void modifyTable(final String tableName, final HTableDescriptor htd)
  throws IOException {
    modifyTable(TableName.valueOf(tableName), htd);
  }

  /**
   * @param regionName Name of a region.
   * @return a pair of HRegionInfo and ServerName if <code>regionName</code> is
   *  a verified region name (we call {@link
   *  MetaTableAccessor#getRegion(HConnection, byte[])}
   *  else null.
   * Throw IllegalArgumentException if <code>regionName</code> is null.
   * @throws IOException
   */
  Pair<HRegionInfo, ServerName> getRegion(final byte[] regionName) throws IOException {
    if (regionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    Pair<HRegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, regionName);
    if (pair == null) {
      final AtomicReference<Pair<HRegionInfo, ServerName>> result =
        new AtomicReference<Pair<HRegionInfo, ServerName>>(null);
      final String encodedName = Bytes.toString(regionName);
      MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
        @Override
        public boolean visit(Result data) throws IOException {
          HRegionInfo info = HRegionInfo.getHRegionInfo(data);
          if (info == null) {
            LOG.warn("No serialized HRegionInfo in " + data);
            return true;
          }
          RegionLocations rl = MetaTableAccessor.getRegionLocations(data);
          boolean matched = false;
          ServerName sn = null;
          if (rl != null) {
            for (HRegionLocation h : rl.getRegionLocations()) {
              if (h != null && encodedName.equals(h.getRegionInfo().getEncodedName())) {
                sn = h.getServerName();
                info = h.getRegionInfo();
                matched = true;
              }
            }
          }
          if (!matched) return true;
          result.set(new Pair<HRegionInfo, ServerName>(info, sn));
          return false; // found the region, stop
        }
      };

      MetaTableAccessor.fullScanRegions(connection, visitor);
      pair = result.get();
    }
    return pair;
  }

  /**
   * If the input is a region name, it is returned as is. If it's an
   * encoded region name, the corresponding region is found from meta
   * and its region name is returned. If we can't find any region in
   * meta matching the input as either region name or encoded region
   * name, the input is returned as is. We don't throw unknown
   * region exception.
   */
  private byte[] getRegionName(
      final byte[] regionNameOrEncodedRegionName) throws IOException {
    if (Bytes.equals(regionNameOrEncodedRegionName,
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
          || Bytes.equals(regionNameOrEncodedRegionName,
            HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      return HRegionInfo.FIRST_META_REGIONINFO.getRegionName();
    }
    byte[] tmp = regionNameOrEncodedRegionName;
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionNameOrEncodedRegionName);
    if (regionServerPair != null && regionServerPair.getFirst() != null) {
      tmp = regionServerPair.getFirst().getRegionName();
    }
    return tmp;
  }

  /**
   * Check if table exists or not
   * @param tableName Name of a table.
   * @return tableName instance
   * @throws IOException if a remote or network exception occurs.
   * @throws TableNotFoundException if table does not exist.
   */
  private TableName checkTableExists(final TableName tableName)
      throws IOException {
    return executeCallable(new ConnectionCallable<TableName>(getConnection()) {
      @Override
      public TableName call(int callTimeout) throws ServiceException, IOException {
        if (!MetaTableAccessor.tableExists(connection, tableName)) {
          throw new TableNotFoundException(tableName);
        }
        return tableName;
      }
    });
  }

  /**
   * Shuts down the HBase cluster
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public synchronized void shutdown() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
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
  @Override
  public synchronized void stopMaster() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        master.stopMaster(null, StopMasterRequest.newBuilder().build());
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
  @Override
  public synchronized void stopRegionServer(final String hostnamePort)
  throws IOException {
    String hostname = Addressing.parseHostname(hostnamePort);
    int port = Addressing.parsePort(hostnamePort);
    AdminService.BlockingInterface admin =
      this.connection.getAdmin(ServerName.valueOf(hostname, port, 0));
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
  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    return executeCallable(new MasterCallable<ClusterStatus>(getConnection()) {
      @Override
      public ClusterStatus call(int callTimeout) throws ServiceException {
        GetClusterStatusRequest req = RequestConverter.buildGetClusterStatusRequest();
        return ClusterStatus.convert(master.getClusterStatus(null, req).getClusterStatus());
      }
    });
  }

  /**
   * @return Configuration used by the instance.
   */
  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Create a new namespace
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException
   */
  @Override
  public void createNamespace(final NamespaceDescriptor descriptor) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws Exception {
        master.createNamespace(null,
          CreateNamespaceRequest.newBuilder()
            .setNamespaceDescriptor(ProtobufUtil
              .toProtoNamespaceDescriptor(descriptor)).build()
        );
        return null;
      }
    });
  }

  /**
   * Modify an existing namespace
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException
   */
  @Override
  public void modifyNamespace(final NamespaceDescriptor descriptor) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws Exception {
        master.modifyNamespace(null, ModifyNamespaceRequest.newBuilder().
          setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(descriptor)).build());
        return null;
      }
    });
  }

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
   * @param name namespace name
   * @throws IOException
   */
  @Override
  public void deleteNamespace(final String name) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws Exception {
        master.deleteNamespace(null, DeleteNamespaceRequest.newBuilder().
          setNamespaceName(name).build());
        return null;
      }
    });
  }

  /**
   * Get a namespace descriptor by name
   * @param name name of namespace descriptor
   * @return A descriptor
   * @throws IOException
   */
  @Override
  public NamespaceDescriptor getNamespaceDescriptor(final String name) throws IOException {
    return
        executeCallable(new MasterCallable<NamespaceDescriptor>(getConnection()) {
          @Override
          public NamespaceDescriptor call(int callTimeout) throws Exception {
            return ProtobufUtil.toNamespaceDescriptor(
              master.getNamespaceDescriptor(null, GetNamespaceDescriptorRequest.newBuilder().
                setNamespaceName(name).build()).getNamespaceDescriptor());
          }
        });
  }

  /**
   * List available namespace descriptors
   * @return List of descriptors
   * @throws IOException
   */
  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return
        executeCallable(new MasterCallable<NamespaceDescriptor[]>(getConnection()) {
          @Override
          public NamespaceDescriptor[] call(int callTimeout) throws Exception {
            List<HBaseProtos.NamespaceDescriptor> list =
              master.listNamespaceDescriptors(null, ListNamespaceDescriptorsRequest.newBuilder().
                build()).getNamespaceDescriptorList();
            NamespaceDescriptor[] res = new NamespaceDescriptor[list.size()];
            for(int i = 0; i < list.size(); i++) {
              res[i] = ProtobufUtil.toNamespaceDescriptor(list.get(i));
            }
            return res;
          }
        });
  }

  /**
   * Get list of table descriptors by namespace
   * @param name namespace name
   * @return A descriptor
   * @throws IOException
   */
  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(final String name) throws IOException {
    return
        executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection()) {
          @Override
          public HTableDescriptor[] call(int callTimeout) throws Exception {
            List<TableSchema> list =
              master.listTableDescriptorsByNamespace(null, ListTableDescriptorsByNamespaceRequest.
                newBuilder().setNamespaceName(name).build()).getTableSchemaList();
            HTableDescriptor[] res = new HTableDescriptor[list.size()];
            for(int i=0; i < list.size(); i++) {

              res[i] = HTableDescriptor.convert(list.get(i));
            }
            return res;
          }
        });
  }

  /**
   * Get list of table names by namespace
   * @param name namespace name
   * @return The list of table names in the namespace
   * @throws IOException
   */
  @Override
  public TableName[] listTableNamesByNamespace(final String name) throws IOException {
    return
        executeCallable(new MasterCallable<TableName[]>(getConnection()) {
          @Override
          public TableName[] call(int callTimeout) throws Exception {
            List<HBaseProtos.TableName> tableNames =
              master.listTableNamesByNamespace(null, ListTableNamesByNamespaceRequest.
                newBuilder().setNamespaceName(name).build())
                .getTableNameList();
            TableName[] result = new TableName[tableNames.size()];
            for (int i = 0; i < tableNames.size(); i++) {
              result[i] = ProtobufUtil.toTableName(tableNames.get(i));
            }
            return result;
          }
        });
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  // Used by tests and by the Merge tool. Merge tool uses it to figure if HBase is up or not.
  public static void checkHBaseAvailable(Configuration conf)
  throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);
    // We set it to make it fail as soon as possible if HBase is not available
    copyOfConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    copyOfConf.setInt("zookeeper.recovery.retry", 0);
    try (ClusterConnection connection =
        (ClusterConnection)ConnectionFactory.createConnection(copyOfConf)) {
        // Check ZK first.
        // If the connection exists, we may have a connection to ZK that does not work anymore
        ZooKeeperKeepAliveConnection zkw = null;
        try {
          // This is NASTY. FIX!!!! Dependent on internal implementation! TODO
          zkw = ((ConnectionImplementation)connection).
            getKeepAliveZooKeeperWatcher();
          zkw.getRecoverableZooKeeper().getZooKeeper().exists(zkw.baseZNode, false);
        } catch (IOException e) {
          throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)
            new InterruptedIOException("Can't connect to ZooKeeper").initCause(e);
        } catch (KeeperException e) {
          throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
        } finally {
          if (zkw != null) {
            zkw.close();
          }
        }
      connection.isMasterRunning();
    }
  }

  /**
   * get the regions of a given table.
   *
   * @param tableName the name of the table
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  @Override
  public List<HRegionInfo> getTableRegions(final TableName tableName)
  throws IOException {
    ZooKeeperWatcher zookeeper =
      new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
        new ThrowableAbortable());
    List<HRegionInfo> regions = null;
    try {
      if (TableName.META_TABLE_NAME.equals(tableName)) {
        regions = new MetaTableLocator().getMetaRegions(zookeeper);
      } else {
        regions = MetaTableAccessor.getTableRegions(connection, tableName, true);
      }
    } finally {
      zookeeper.close();
    }
    return regions;
  }

  public List<HRegionInfo> getTableRegions(final byte[] tableName)
  throws IOException {
    return getTableRegions(TableName.valueOf(tableName));
  }

  @Override
  public synchronized void close() throws IOException {
    if (cleanupConnectionOnClose && this.connection != null && !this.closed) {
      this.connection.close();
      this.closed = true;
    }
  }

  /**
   * Get tableDescriptors
   * @param tableNames List of table names
   * @return HTD[] the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(final List<TableName> tableNames)
  throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection()) {
      @Override
      public HTableDescriptor[] call(int callTimeout) throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(tableNames);
          return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
      }
    });
  }

  /**
   * Get tableDescriptor
   * @param tableName one table name
   * @return HTD the HTableDescriptor or null if the table not exists
   * @throws IOException if a remote or network exception occurs
   */
  private HTableDescriptor getTableDescriptorByTableName(TableName tableName)
      throws IOException {
    List<TableName> tableNames = new ArrayList<TableName>(1);
    tableNames.add(tableName);

    HTableDescriptor[] htdl = getTableDescriptorsByTableName(tableNames);

    if (htdl == null || htdl.length == 0) {
      return null;
    }
    else {
      return htdl[0];
    }
  }

  /**
   * Get tableDescriptors
   * @param names List of table names
   * @return HTD[] the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> names)
  throws IOException {
    List<TableName> tableNames = new ArrayList<TableName>(names.size());
    for(String name : names) {
      tableNames.add(TableName.valueOf(name));
    }
    return getTableDescriptorsByTableName(tableNames);
  }

  private RollWALWriterResponse rollWALWriterImpl(final ServerName sn) throws IOException,
      FailedLogCloseException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    RollWALWriterRequest request = RequestConverter.buildRollWALWriterRequest();
    try {
      return admin.rollWALWriter(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Roll the log writer. I.e. when using a file system based write ahead log,
   * start writing log messages to a new file.
   *
   * Note that when talking to a version 1.0+ HBase deployment, the rolling is asynchronous.
   * This method will return as soon as the roll is requested and the return value will
   * always be null. Additionally, the named region server may schedule store flushes at the
   * request of the wal handling the roll request.
   *
   * When talking to a 0.98 or older HBase deployment, the rolling is synchronous and the
   * return value may be either null or a list of encoded region names.
   *
   * @param serverName
   *          The servername of the regionserver. A server name is made of host,
   *          port and startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return a set of {@link HRegionInfo#getEncodedName()} that would allow the wal to
   *         clean up some underlying files. null if there's nothing to flush.
   * @throws IOException if a remote or network exception occurs
   * @throws FailedLogCloseException
   * @deprecated use {@link #rollWALWriter(ServerName)}
   */
  @Deprecated
  public synchronized byte[][] rollHLogWriter(String serverName)
      throws IOException, FailedLogCloseException {
    ServerName sn = ServerName.valueOf(serverName);
    final RollWALWriterResponse response = rollWALWriterImpl(sn);
    int regionCount = response.getRegionToFlushCount();
    if (0 == regionCount) {
      return null;
    }
    byte[][] regionsToFlush = new byte[regionCount][];
    for (int i = 0; i < regionCount; i++) {
      ByteString region = response.getRegionToFlush(i);
      regionsToFlush[i] = region.toByteArray();
    }
    return regionsToFlush;
  }

  @Override
  public synchronized void rollWALWriter(ServerName serverName)
      throws IOException, FailedLogCloseException {
    rollWALWriterImpl(serverName);
  }

  @Override
  public String[] getMasterCoprocessors() {
    try {
      return getClusterStatus().getMasterCoprocessors();
    } catch (IOException e) {
      LOG.error("Could not getClusterStatus()",e);
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompactionState getCompactionState(final TableName tableName)
  throws IOException {
    CompactionState state = CompactionState.NONE;
    ZooKeeperWatcher zookeeper =
      new ZooKeeperWatcher(conf, ZK_IDENTIFIER_PREFIX + connection.toString(),
        new ThrowableAbortable());
    try {
      checkTableExists(tableName);
      List<Pair<HRegionInfo, ServerName>> pairs;
      if (TableName.META_TABLE_NAME.equals(tableName)) {
        pairs = new MetaTableLocator().getMetaRegionsAndLocations(zookeeper);
      } else {
        pairs = MetaTableAccessor.getTableRegionsAndLocations(connection, tableName);
      }
      for (Pair<HRegionInfo, ServerName> pair: pairs) {
        if (pair.getFirst().isOffline()) continue;
        if (pair.getSecond() == null) continue;
        try {
          ServerName sn = pair.getSecond();
          AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
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
        } catch (RemoteException e) {
          if (e.getMessage().indexOf(NotServingRegionException.class.getName()) >= 0) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to get compaction state of " + pair.getFirst() + ": "
                + StringUtils.stringifyException(e));
            }
          } else {
            throw e;
          }
        }
      }
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      zookeeper.close();
    }
    return state;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompactionState getCompactionStateForRegion(final byte[] regionName)
  throws IOException {
    try {
      Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionName);
      if (regionServerPair == null) {
        throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
      }
      if (regionServerPair.getSecond() == null) {
        throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
      }
      ServerName sn = regionServerPair.getSecond();
      AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
      GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
        regionServerPair.getFirst().getRegionName(), true);
      GetRegionInfoResponse response = admin.getRegionInfo(null, request);
      return response.getCompactionState();
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * @deprecated Use {@link #getCompactionState(org.apache.hadoop.hbase.TableName)} or {@link
   * #getCompactionStateForRegion(byte[])} instead.
   */
  @Deprecated
  public CompactionState getCompactionState(final String tableNameOrRegionName)
  throws IOException, InterruptedException {
    return getCompactionState(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * @deprecated Use {@link #getCompactionState(org.apache.hadoop.hbase.TableName)} or {@link
   * #getCompactionStateForRegion(byte[])} instead.
   */
  @Deprecated
  public CompactionState getCompactionState(final byte[] tableNameOrRegionName)
  throws IOException, InterruptedException {
    try {
      return getCompactionStateForRegion(tableNameOrRegionName);
    } catch (IllegalArgumentException e) {
      // Invalid region, try table
      return getCompactionState(TableName.valueOf(tableNameOrRegionName));
    }
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
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  @Override
  public void snapshot(final String snapshotName,
                       final TableName tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, tableName, SnapshotDescription.Type.FLUSH);
  }

  public void snapshot(final String snapshotName,
                       final String tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, TableName.valueOf(tableName),
        SnapshotDescription.Type.FLUSH);
  }

  /**
   * Create snapshot for the given table of given flush type.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase.
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
   public void snapshot(final String snapshotName,
    * Create a timestamp consistent snapshot for the given table.
                        final byte[] tableName) throws IOException,
    * <p>
    * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
    * snapshot with the same name (even a different type or with different parameters) will fail
    * with a {@link SnapshotCreationException} indicating the duplicate naming.
    * <p>
    * Snapshot names follow the same naming constraints as tables in HBase.
    * @param snapshotName name of the snapshot to be created
    * @param tableName name of the table for which snapshot is created
    * @throws IOException if a remote or network exception occurs
    * @throws SnapshotCreationException if snapshot creation failed
    * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
    */
  @Override
  public void snapshot(final byte[] snapshotName,
                       final TableName tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), tableName, SnapshotDescription.Type.FLUSH);
  }

  public void snapshot(final byte[] snapshotName,
                       final byte[] tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), TableName.valueOf(tableName),
      SnapshotDescription.Type.FLUSH);
  }

  /**
   * Create typed snapshot of the table.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * <p>
   * @param snapshotName name to give the snapshot on the filesystem. Must be unique from all other
   *          snapshots stored on the cluster
   * @param tableName name of the table to snapshot
   * @param type type of snapshot to take
   * @throws IOException we fail to reach the master
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  @Override
  public void snapshot(final String snapshotName,
                       final TableName tableName,
                      SnapshotDescription.Type type) throws IOException, SnapshotCreationException,
      IllegalArgumentException {
    SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
    builder.setTable(tableName.getNameAsString());
    builder.setName(snapshotName);
    builder.setType(type);
    snapshot(builder.build());
  }

  public void snapshot(final String snapshotName,
                       final String tableName,
                      SnapshotDescription.Type type) throws IOException, SnapshotCreationException,
      IllegalArgumentException {
    snapshot(snapshotName, TableName.valueOf(tableName), type);
  }

  public void snapshot(final String snapshotName,
                       final byte[] tableName,
                      SnapshotDescription.Type type) throws IOException, SnapshotCreationException,
      IllegalArgumentException {
    snapshot(snapshotName, TableName.valueOf(tableName), type);
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
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * <p>
   * You should probably use {@link #snapshot(String, String)} or {@link #snapshot(byte[], byte[])}
   * unless you are sure about the type of snapshot that you want to take.
   * @param snapshot snapshot to take
   * @throws IOException or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot failed to be taken
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  @Override
  public void snapshot(SnapshotDescription snapshot) throws IOException, SnapshotCreationException,
      IllegalArgumentException {
    // actually take the snapshot
    SnapshotResponse response = takeSnapshotAsync(snapshot);
    final IsSnapshotDoneRequest request = IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot)
        .build();
    IsSnapshotDoneResponse done = null;
    long start = EnvironmentEdgeManager.currentTime();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for snapshot '" +
        ClientSnapshotDescriptionUtils.toString(snapshot) + "'' to complete. (max " +
        maxPauseTime + " ms per retry)");
    while (tries == 0
        || ((EnvironmentEdgeManager.currentTime() - start) < max && !done.getDone())) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug("(#" + tries + ") Sleeping: " + sleep +
          "ms while waiting for snapshot completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of snapshot from master...");
      done = executeCallable(new MasterCallable<IsSnapshotDoneResponse>(getConnection()) {
        @Override
        public IsSnapshotDoneResponse call(int callTimeout) throws ServiceException {
          return master.isSnapshotDone(null, request);
        }
      });
    }
    if (!done.getDone()) {
      throw new SnapshotCreationException("Snapshot '" + snapshot.getName()
          + "' wasn't completed in expectedTime:" + max + " ms", snapshot);
    }
  }

  /**
   * Take a snapshot without waiting for the server to complete that snapshot (asynchronous)
   * <p>
   * Only a single snapshot should be taken at a time, or results may be undefined.
   * @param snapshot snapshot to take
   * @return response from the server indicating the max time to wait for the snapshot
   * @throws IOException if the snapshot did not succeed or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  @Override
  public SnapshotResponse takeSnapshotAsync(SnapshotDescription snapshot) throws IOException,
      SnapshotCreationException {
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    final SnapshotRequest request = SnapshotRequest.newBuilder().setSnapshot(snapshot)
        .build();
    // run the snapshot on the master
    return executeCallable(new MasterCallable<SnapshotResponse>(getConnection()) {
      @Override
      public SnapshotResponse call(int callTimeout) throws ServiceException {
        return master.snapshot(null, request);
      }
    });
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
   *         running
   * @throws IOException if we have a network issue
   * @throws HBaseSnapshotException if the snapshot failed
   * @throws UnknownSnapshotException if the requested snapshot is unknown
   */
  @Override
  public boolean isSnapshotFinished(final SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {

    return executeCallable(new MasterCallable<IsSnapshotDoneResponse>(getConnection()) {
      @Override
      public IsSnapshotDoneResponse call(int callTimeout) throws ServiceException {
        return master.isSnapshotDone(null,
          IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot).build());
      }
    }).getDone();
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled)
   * If the "hbase.snapshot.restore.take.failsafe.snapshot" configuration property
   * is set to true, a snapshot of the current table is taken
   * before executing the restore operation.
   * In case of restore failure, the failsafe snapshot will be restored.
   * If the restore completes without problem the failsafe snapshot is deleted.
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  @Override
  public void restoreSnapshot(final byte[] snapshotName)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName));
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled)
   * If the "hbase.snapshot.restore.take.failsafe.snapshot" configuration property
   * is set to true, a snapshot of the current table is taken
   * before executing the restore operation.
   * In case of restore failure, the failsafe snapshot will be restored.
   * If the restore completes without problem the failsafe snapshot is deleted.
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  @Override
  public void restoreSnapshot(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    boolean takeFailSafeSnapshot =
      conf.getBoolean("hbase.snapshot.restore.take.failsafe.snapshot", false);
    restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled)
   * If 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken
   * before executing the restore operation.
   * In case of restore failure, the failsafe snapshot will be restored.
   * If the restore completes without problem the failsafe snapshot is deleted.
   *
   * The failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   *
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  @Override
  public void restoreSnapshot(final byte[] snapshotName, final boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName), takeFailSafeSnapshot);
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled)
   * If 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken
   * before executing the restore operation.
   * In case of restore failure, the failsafe snapshot will be restored.
   * If the restore completes without problem the failsafe snapshot is deleted.
   *
   * The failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   *
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  @Override
  public void restoreSnapshot(final String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    TableName tableName = null;
    for (SnapshotDescription snapshotInfo: listSnapshots()) {
      if (snapshotInfo.getName().equals(snapshotName)) {
        tableName = TableName.valueOf(snapshotInfo.getTable());
        break;
      }
    }

    if (tableName == null) {
      throw new RestoreSnapshotException(
        "Unable to find the table name for snapshot=" + snapshotName);
    }

    // The table does not exists, switch to clone.
    if (!tableExists(tableName)) {
      cloneSnapshot(snapshotName, tableName);
      return;
    }

    // Check if the table is disabled
    if (!isTableDisabled(tableName)) {
      throw new TableNotDisabledException(tableName);
    }

    // Take a snapshot of the current state
    String failSafeSnapshotSnapshotName = null;
    if (takeFailSafeSnapshot) {
      failSafeSnapshotSnapshotName = conf.get("hbase.snapshot.restore.failsafe.name",
        "hbase-failsafe-{snapshot.name}-{restore.timestamp}");
      failSafeSnapshotSnapshotName = failSafeSnapshotSnapshotName
        .replace("{snapshot.name}", snapshotName)
        .replace("{table.name}", tableName.toString().replace(TableName.NAMESPACE_DELIM, '.'))
        .replace("{restore.timestamp}", String.valueOf(EnvironmentEdgeManager.currentTime()));
      LOG.info("Taking restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
      snapshot(failSafeSnapshotSnapshotName, tableName);
    }

    try {
      // Restore snapshot
      internalRestoreSnapshot(snapshotName, tableName);
    } catch (IOException e) {
      // Somthing went wrong during the restore...
      // if the pre-restore snapshot is available try to rollback
      if (takeFailSafeSnapshot) {
        try {
          internalRestoreSnapshot(failSafeSnapshotSnapshotName, tableName);
          String msg = "Restore snapshot=" + snapshotName +
            " failed. Rollback to snapshot=" + failSafeSnapshotSnapshotName + " succeeded.";
          LOG.error(msg, e);
          throw new RestoreSnapshotException(msg, e);
        } catch (IOException ex) {
          String msg = "Failed to restore and rollback to snapshot=" + failSafeSnapshotSnapshotName;
          LOG.error(msg, ex);
          throw new RestoreSnapshotException(msg, e);
        }
      } else {
        throw new RestoreSnapshotException("Failed to restore snapshot=" + snapshotName, e);
      }
    }

    // If the restore is succeeded, delete the pre-restore snapshot
    if (takeFailSafeSnapshot) {
      try {
        LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
        deleteSnapshot(failSafeSnapshotSnapshotName);
      } catch (IOException e) {
        LOG.error("Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName, e);
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
      throws IOException, TableExistsException, RestoreSnapshotException {
    cloneSnapshot(Bytes.toString(snapshotName), TableName.valueOf(tableName));
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
  @Override
  public void cloneSnapshot(final byte[] snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    cloneSnapshot(Bytes.toString(snapshotName), tableName);
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
    cloneSnapshot(snapshotName, TableName.valueOf(tableName));
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
  @Override
  public void cloneSnapshot(final String snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    if (tableExists(tableName)) {
      throw new TableExistsException(tableName);
    }
    internalRestoreSnapshot(snapshotName, tableName);
    waitUntilTableIsEnabled(tableName);
  }

  /**
   * Execute a distributed procedure on a cluster synchronously with return data
   *
   * @param signature A distributed procedure is uniquely identified
   * by its signature (default the root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   * optional.
   * @param props Property/Value pairs of properties passing to the procedure
   * @return data returned after procedure execution. null if no return data.
   * @throws IOException
   */
  @Override
  public byte[] execProcedureWithRet(String signature, String instance,
      Map<String, String> props) throws IOException {
    ProcedureDescription.Builder builder = ProcedureDescription.newBuilder();
    builder.setSignature(signature).setInstance(instance);
    for (Entry<String, String> entry : props.entrySet()) {
      NameStringPair pair = NameStringPair.newBuilder().setName(entry.getKey())
          .setValue(entry.getValue()).build();
      builder.addConfiguration(pair);
    }

    final ExecProcedureRequest request = ExecProcedureRequest.newBuilder()
        .setProcedure(builder.build()).build();
    // run the procedure on the master
    ExecProcedureResponse response = executeCallable(new MasterCallable<ExecProcedureResponse>(
        getConnection()) {
      @Override
      public ExecProcedureResponse call(int callTimeout) throws ServiceException {
        return master.execProcedureWithRet(null, request);
      }
    });

    return response.hasReturnData() ? response.getReturnData().toByteArray() : null;
  }
  /**
   * Execute a distributed procedure on a cluster.
   *
   * @param signature A distributed procedure is uniquely identified
   * by its signature (default the root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   * optional.
   * @param props Property/Value pairs of properties passing to the procedure
   * @throws IOException
   */
  @Override
  public void execProcedure(String signature, String instance,
      Map<String, String> props) throws IOException {
    ProcedureDescription.Builder builder = ProcedureDescription.newBuilder();
    builder.setSignature(signature).setInstance(instance);
    for (Entry<String, String> entry : props.entrySet()) {
      NameStringPair pair = NameStringPair.newBuilder().setName(entry.getKey())
          .setValue(entry.getValue()).build();
      builder.addConfiguration(pair);
    }

    final ExecProcedureRequest request = ExecProcedureRequest.newBuilder()
        .setProcedure(builder.build()).build();
    // run the procedure on the master
    ExecProcedureResponse response = executeCallable(new MasterCallable<ExecProcedureResponse>(
        getConnection()) {
      @Override
      public ExecProcedureResponse call(int callTimeout) throws ServiceException {
        return master.execProcedure(null, request);
      }
    });

    long start = EnvironmentEdgeManager.currentTime();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for procedure '" +
        signature + " : " + instance + "'' to complete. (max " + maxPauseTime + " ms per retry)");
    boolean done = false;
    while (tries == 0
        || ((EnvironmentEdgeManager.currentTime() - start) < max && !done)) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug("(#" + tries + ") Sleeping: " + sleep +
          "ms while waiting for procedure completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of procedure from master...");
      done = isProcedureFinished(signature, instance, props);
    }
    if (!done) {
      throw new IOException("Procedure '" + signature + " : " + instance
          + "' wasn't completed in expectedTime:" + max + " ms");
    }
  }

  /**
   * Check the current state of the specified procedure.
   * <p>
   * There are three possible states:
   * <ol>
   * <li>running - returns <tt>false</tt></li>
   * <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the procedure to fail</li>
   * </ol>
   * <p>
   *
   * @param signature The signature that uniquely identifies a procedure
   * @param instance The instance name of the procedure
   * @param props Property/Value pairs of properties passing to the procedure
   * @return true if the specified procedure is finished successfully, false if it is still running
   * @throws IOException if the specified procedure finished with error
   */
  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    final ProcedureDescription.Builder builder = ProcedureDescription.newBuilder();
    builder.setSignature(signature).setInstance(instance);
    for (Entry<String, String> entry : props.entrySet()) {
      NameStringPair pair = NameStringPair.newBuilder().setName(entry.getKey())
          .setValue(entry.getValue()).build();
      builder.addConfiguration(pair);
    }
    final ProcedureDescription desc = builder.build();
    return executeCallable(
        new MasterCallable<IsProcedureDoneResponse>(getConnection()) {
          @Override
          public IsProcedureDoneResponse call(int callTimeout) throws ServiceException {
            return master.isProcedureDone(null, IsProcedureDoneRequest
                .newBuilder().setProcedure(desc).build());
          }
        }).getDone();
  }

  /**
   * Execute Restore/Clone snapshot and wait for the server to complete (blocking).
   * To check if the cloned table exists, use {@link #isTableAvailable} -- it is not safe to
   * create an HTable instance to this table before it is available.
   * @param snapshotName snapshot to restore
   * @param tableName table name to restore the snapshot on
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  private void internalRestoreSnapshot(final String snapshotName, final TableName
      tableName)
      throws IOException, RestoreSnapshotException {
    SnapshotDescription snapshot = SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(tableName.getNameAsString()).build();

    // actually restore the snapshot
    internalRestoreSnapshotAsync(snapshot);

    final IsRestoreSnapshotDoneRequest request = IsRestoreSnapshotDoneRequest.newBuilder()
        .setSnapshot(snapshot).build();
    IsRestoreSnapshotDoneResponse done = IsRestoreSnapshotDoneResponse.newBuilder()
        .setDone(false).buildPartial();
    final long maxPauseTime = 5000;
    int tries = 0;
    while (!done.getDone()) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug(tries + ") Sleeping: " + sleep
            + " ms while we wait for snapshot restore to complete.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of snapshot restore from master...");
      done = executeCallable(new MasterCallable<IsRestoreSnapshotDoneResponse>(
          getConnection()) {
        @Override
        public IsRestoreSnapshotDoneResponse call(int callTimeout) throws ServiceException {
          return master.isRestoreSnapshotDone(null, request);
        }
      });
    }
    if (!done.getDone()) {
      throw new RestoreSnapshotException("Snapshot '" + snapshot.getName() + "' wasn't restored.");
    }
  }

  /**
   * Execute Restore/Clone snapshot and wait for the server to complete (asynchronous)
   * <p>
   * Only a single snapshot should be restored at a time, or results may be undefined.
   * @param snapshot snapshot to restore
   * @return response from the server indicating the max time to wait for the snapshot
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  private RestoreSnapshotResponse internalRestoreSnapshotAsync(final SnapshotDescription snapshot)
      throws IOException, RestoreSnapshotException {
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);

    final RestoreSnapshotRequest request = RestoreSnapshotRequest.newBuilder().setSnapshot(snapshot)
        .build();

    // run the snapshot restore on the master
    return executeCallable(new MasterCallable<RestoreSnapshotResponse>(getConnection()) {
      @Override
      public RestoreSnapshotResponse call(int callTimeout) throws ServiceException {
        return master.restoreSnapshot(null, request);
      }
    });
  }

  /**
   * List completed snapshots.
   * @return a list of snapshot descriptors for completed snapshots
   * @throws IOException if a network error occurs
   */
  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    return executeCallable(new MasterCallable<List<SnapshotDescription>>(getConnection()) {
      @Override
      public List<SnapshotDescription> call(int callTimeout) throws ServiceException {
        return master.getCompletedSnapshots(null, GetCompletedSnapshotsRequest.newBuilder().build())
            .getSnapshotsList();
      }
    });
  }

  /**
   * List all the completed snapshots matching the given regular expression.
   *
   * @param regex The regular expression to match against
   * @return - returns a List of SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  /**
   * List all the completed snapshots matching the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @return - returns a List of SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    List<SnapshotDescription> matched = new LinkedList<SnapshotDescription>();
    List<SnapshotDescription> snapshots = listSnapshots();
    for (SnapshotDescription snapshot : snapshots) {
      if (pattern.matcher(snapshot.getName()).matches()) {
        matched.add(snapshot);
      }
    }
    return matched;
  }

  /**
   * Delete an existing snapshot.
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void deleteSnapshot(final byte[] snapshotName) throws IOException {
    deleteSnapshot(Bytes.toString(snapshotName));
  }

  /**
   * Delete an existing snapshot.
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void deleteSnapshot(final String snapshotName) throws IOException {
    // make sure the snapshot is possibly valid
    TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(snapshotName));
    // do the delete
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        master.deleteSnapshot(null,
          DeleteSnapshotRequest.newBuilder().
            setSnapshot(SnapshotDescription.newBuilder().setName(snapshotName).build()).build()
        );
        return null;
      }
    });
  }

  /**
   * Delete existing snapshots whose names match the pattern passed.
   * @param regex The regular expression to match against
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void deleteSnapshots(final String regex) throws IOException {
    deleteSnapshots(Pattern.compile(regex));
  }

  /**
   * Delete existing snapshots whose names match the pattern passed.
   * @param pattern pattern for names of the snapshot to match
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void deleteSnapshots(final Pattern pattern) throws IOException {
    List<SnapshotDescription> snapshots = listSnapshots(pattern);
    for (final SnapshotDescription snapshot : snapshots) {
      try {
        internalDeleteSnapshot(snapshot);
      } catch (IOException ex) {
        LOG.info(
          "Failed to delete snapshot " + snapshot.getName() + " for table " + snapshot.getTable(),
          ex);
      }
    }
  }

  private void internalDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        this.master.deleteSnapshot(null, DeleteSnapshotRequest.newBuilder().setSnapshot(snapshot)
            .build());
        return null;
      }
    });
  }

  /**
   * Apply the new quota settings.
   *
   * @param quota the quota settings
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public void setQuota(final QuotaSettings quota) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void call(int callTimeout) throws ServiceException {
        this.master.setQuota(null, QuotaSettings.buildSetQuotaRequestProto(quota));
        return null;
      }
    });
  }

  /**
   * Return a Quota Scanner to list the quotas based on the filter.
   *
   * @param filter the quota settings filter
   * @return the quota scanner
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public QuotaRetriever getQuotaRetriever(final QuotaFilter filter) throws IOException {
    return QuotaRetriever.open(conf, filter);
  }

  private <C extends RetryingCallable<V> & Closeable, V> V executeCallable(C callable)
      throws IOException {
    RpcRetryingCaller<V> caller = rpcCallerFactory.newCaller();
    try {
      return caller.callWithRetries(callable, operationTimeout);
    } finally {
      callable.close();
    }
  }

  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance
   * connected to the active master.
   *
   * <p>
   * The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access a published
   * coprocessor {@link com.google.protobuf.Service} using standard protobuf service invocations:
   * </p>
   *
   * <div style="background-color: #cccccc; padding: 2px">
   * <blockquote><pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService();
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre></blockquote></div>
   *
   * @return A MasterCoprocessorRpcChannel instance
   */
  @Override
  public CoprocessorRpcChannel coprocessorService() {
    return new MasterCoprocessorRpcChannel(connection);
  }

  /**
   * Simple {@link Abortable}, throwing RuntimeException on abort.
   */
  private static class ThrowableAbortable implements Abortable {

    @Override
    public void abort(String why, Throwable e) {
      throw new RuntimeException(why, e);
    }

    @Override
    public boolean isAborted() {
      return true;
    }
  }

  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance
   * connected to the passed region server.
   *
   * <p>
   * The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access a published
   * coprocessor {@link com.google.protobuf.Service} using standard protobuf service invocations:
   * </p>
   *
   * <div style="background-color: #cccccc; padding: 2px">
   * <blockquote><pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService(serverName);
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre></blockquote></div>
   *
   * @param sn the server name to which the endpoint call is made
   * @return A RegionServerCoprocessorRpcChannel instance
   */
  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName sn) {
    return new RegionServerCoprocessorRpcChannel(connection, sn);
  }

  @Override
  public void updateConfiguration(ServerName server) throws IOException {
    try {
      this.connection.getAdmin(server).updateConfiguration(null,
        UpdateConfigurationRequest.getDefaultInstance());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void updateConfiguration() throws IOException {
    for (ServerName server : this.getClusterStatus().getServers()) {
      updateConfiguration(server);
    }
  }

  @Override
  public int getMasterInfoPort() throws IOException {
    // TODO: Fix!  Reaching into internal implementation!!!!
    ConnectionImplementation connection =
        (ConnectionImplementation)this.connection;
    ZooKeeperKeepAliveConnection zkw = connection.getKeepAliveZooKeeperWatcher();
    try {
      return MasterAddressTracker.getMasterInfoPort(zkw);
    } catch (KeeperException e) {
      throw new IOException("Failed to get master info port from MasterAddressTracker", e);
    }
  }

  @Override
  public long getLastMajorCompactionTimestamp(final TableName tableName) throws IOException {
    return executeCallable(new MasterCallable<Long>(getConnection()) {
      @Override
      public Long call(int callTimeout) throws ServiceException {
        MajorCompactionTimestampRequest req =
            MajorCompactionTimestampRequest.newBuilder()
                .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
        return master.getLastMajorCompactionTimestamp(null, req).getCompactionTimestamp();
      }
    });
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(final byte[] regionName) throws IOException {
    return executeCallable(new MasterCallable<Long>(getConnection()) {
      @Override
      public Long call(int callTimeout) throws ServiceException {
        MajorCompactionTimestampForRegionRequest req =
            MajorCompactionTimestampForRegionRequest
                .newBuilder()
                .setRegion(
                  RequestConverter
                      .buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName)).build();
        return master.getLastMajorCompactionTimestampForRegion(null, req).getCompactionTimestamp();
      }
    });
  }

  /**
   * Future that waits on a procedure result.
   * Returned by the async version of the Admin calls,
   * and used internally by the sync calls to wait on the result of the procedure.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static class ProcedureFuture<V> implements Future<V> {
    private ExecutionException exception = null;
    private boolean procResultFound = false;
    private boolean done = false;
    private V result = null;

    private final HBaseAdmin admin;
    private final Long procId;

    public ProcedureFuture(final HBaseAdmin admin, final Long procId) {
      this.admin = admin;
      this.procId = procId;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
      // TODO: Abort not implemented yet
      return false;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      // TODO: should we ever spin forever?
      throw new UnsupportedOperationException();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!done) {
        long deadlineTs = EnvironmentEdgeManager.currentTime() + unit.toMillis(timeout);
        try {
          try {
            // if the master support procedures, try to wait the result
            if (procId != null) {
              result = waitProcedureResult(procId, deadlineTs);
            }
            // if we don't have a proc result, try the compatibility wait
            if (!procResultFound) {
              result = waitOperationResult(deadlineTs);
            }
            result = postOperationResult(result, deadlineTs);
            done = true;
          } catch (IOException e) {
            result = postOperationFailure(e, deadlineTs);
            done = true;
          }
        } catch (IOException e) {
          exception = new ExecutionException(e);
          done = true;
        }
      }
      if (exception != null) {
        throw exception;
      }
      return result;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    protected HBaseAdmin getAdmin() {
      return admin;
    }

    private V waitProcedureResult(long procId, long deadlineTs)
        throws IOException, TimeoutException, InterruptedException {
      GetProcedureResultRequest request = GetProcedureResultRequest.newBuilder()
          .setProcId(procId)
          .build();

      int tries = 0;
      IOException serviceEx = null;
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        GetProcedureResultResponse response = null;
        try {
          // Try to fetch the result
          response = getProcedureResult(request);
        } catch (IOException e) {
          serviceEx = unwrapException(e);

          // the master may be down
          LOG.warn("failed to get the procedure result procId=" + procId, serviceEx);

          // Not much to do, if we have a DoNotRetryIOException
          if (serviceEx instanceof DoNotRetryIOException) {
            // TODO: looks like there is no way to unwrap this exception and get the proper
            // UnsupportedOperationException aside from looking at the message.
            // anyway, if we fail here we just failover to the compatibility side
            // and that is always a valid solution.
            LOG.warn("Proc-v2 is unsupported on this master: " + serviceEx.getMessage(), serviceEx);
            procResultFound = false;
            return null;
          }
        }

        // If the procedure is no longer running, we should have a result
        if (response != null && response.getState() != GetProcedureResultResponse.State.RUNNING) {
          procResultFound = response.getState() != GetProcedureResultResponse.State.NOT_FOUND;
          return convertResult(response);
        }

        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          throw new InterruptedException(
            "Interrupted while waiting for the result of proc " + procId);
        }
      }
      if (serviceEx != null) {
        throw serviceEx;
      } else {
        throw new TimeoutException("The procedure " + procId + " is still running");
      }
    }

    private static IOException unwrapException(IOException e) {
      if (e instanceof RemoteException) {
        return ((RemoteException)e).unwrapRemoteException();
      }
      return e;
    }

    protected GetProcedureResultResponse getProcedureResult(final GetProcedureResultRequest request)
        throws IOException {
      return admin.executeCallable(new MasterCallable<GetProcedureResultResponse>(
          admin.getConnection()) {
        @Override
        public GetProcedureResultResponse call(int callTimeout) throws ServiceException {
          return master.getProcedureResult(null, request);
        }
      });
    }

    /**
     * Convert the procedure result response to a specified type.
     * @param response the procedure result object to parse
     * @return the result data of the procedure.
     */
    protected V convertResult(final GetProcedureResultResponse response) throws IOException {
      if (response.hasException()) {
        throw ForeignExceptionUtil.toIOException(response.getException());
      }
      return null;
    }

    /**
     * Fallback implementation in case the procedure is not supported by the server.
     * It should try to wait until the operation is completed.
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result data of the operation
     */
    protected V waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      return null;
    }

    /**
     * Called after the operation is completed and the result fetched. this allows to perform extra
     * steps after the procedure is completed. it allows to apply transformations to the result that
     * will be returned by get().
     * @param result the result of the procedure
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result of the procedure, which may be the same as the passed one
     */
    protected V postOperationResult(final V result, final long deadlineTs)
        throws IOException, TimeoutException {
      return result;
    }

    /**
     * Called after the operation is terminated with a failure.
     * this allows to perform extra steps after the procedure is terminated.
     * it allows to apply transformations to the result that will be returned by get().
     * The default implementation will rethrow the exception
     * @param exception the exception got from fetching the result
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result of the procedure, which may be the same as the passed one
     */
    protected V postOperationFailure(final IOException exception, final long deadlineTs)
        throws IOException, TimeoutException {
      throw exception;
    }

    protected interface WaitForStateCallable {
      boolean checkState(int tries) throws IOException;
      void throwInterruptedException() throws InterruptedIOException;
      void throwTimeoutException(long elapsed) throws TimeoutException;
    }

    protected void waitForState(final long deadlineTs, final WaitForStateCallable callable)
        throws IOException, TimeoutException {
      int tries = 0;
      IOException serverEx = null;
      long startTime = EnvironmentEdgeManager.currentTime();
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        serverEx = null;
        try {
          if (callable.checkState(tries)) {
            return;
          }
        } catch (IOException e) {
          serverEx = e;
        }
        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          callable.throwInterruptedException();
        }
      }
      if (serverEx != null) {
        throw unwrapException(serverEx);
      } else {
        callable.throwTimeoutException(EnvironmentEdgeManager.currentTime() - startTime);
      }
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static abstract class TableFuture<V> extends ProcedureFuture<V> {
    private final TableName tableName;

    public TableFuture(final HBaseAdmin admin, final TableName tableName, final Long procId) {
      super(admin, procId);
      this.tableName = tableName;
    }

    /**
     * @return the table name
     */
    protected TableName getTableName() {
      return tableName;
    }

    /**
     * @return the table descriptor
     */
    protected HTableDescriptor getTableDescriptor() throws IOException {
      return getAdmin().getTableDescriptorByTableName(getTableName());
    }

    /**
     * @return a description of the operation
     */
    protected abstract String getDescription();

    @Override
    protected V postOperationResult(final V result, final long deadlineTs)
        throws IOException, TimeoutException {
      LOG.info(getDescription() + " completed");
      return super.postOperationResult(result, deadlineTs);
    }

    @Override
    protected V postOperationFailure(final IOException exception, final long deadlineTs)
        throws IOException, TimeoutException {
      LOG.info(getDescription() + " failed with " + exception.getMessage());
      return super.postOperationFailure(exception, deadlineTs);
    }

    protected void waitForTableEnabled(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new WaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          try {
            if (getAdmin().isTableAvailable(tableName)) {
              return true;
            }
          } catch (TableNotFoundException tnfe) {
            LOG.debug("Table " + tableName.getNameAsString()
                + " was not enabled, sleeping. tries=" + tries);
          }
          return false;
        }

        @Override
        public void throwInterruptedException() throws InterruptedIOException {
          throw new InterruptedIOException("Interrupted when waiting for table "
              + tableName.getNameAsString() + " to be enabled");
        }

        @Override
        public void throwTimeoutException(long elapsedTime) throws TimeoutException {
          throw new TimeoutException("Table " + tableName.getNameAsString()
              + " not enabled after " + elapsedTime + "msec");
        }
      });
    }

    protected void waitForTableDisabled(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new WaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return getAdmin().isTableDisabled(tableName);
        }

        @Override
        public void throwInterruptedException() throws InterruptedIOException {
          throw new InterruptedIOException("Interrupted when waiting for table to be disabled");
        }

        @Override
        public void throwTimeoutException(long elapsedTime) throws TimeoutException {
          throw new TimeoutException("Table " + tableName + " not yet disabled after "
              + elapsedTime + "msec");
        }
      });
    }

    protected void waitForAllRegionsOnline(final long deadlineTs, final byte[][] splitKeys)
        throws IOException, TimeoutException {
      final HTableDescriptor desc = getTableDescriptor();
      final AtomicInteger actualRegCount = new AtomicInteger(0);
      final MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
        @Override
        public boolean visit(Result rowResult) throws IOException {
          RegionLocations list = MetaTableAccessor.getRegionLocations(rowResult);
          if (list == null) {
            LOG.warn("No serialized HRegionInfo in " + rowResult);
            return true;
          }
          HRegionLocation l = list.getRegionLocation();
          if (l == null) {
            return true;
          }
          if (!l.getRegionInfo().getTable().equals(desc.getTableName())) {
            return false;
          }
          if (l.getRegionInfo().isOffline() || l.getRegionInfo().isSplit()) return true;
          HRegionLocation[] locations = list.getRegionLocations();
          for (HRegionLocation location : locations) {
            if (location == null) continue;
            ServerName serverName = location.getServerName();
            // Make sure that regions are assigned to server
            if (serverName != null && serverName.getHostAndPort() != null) {
              actualRegCount.incrementAndGet();
            }
          }
          return true;
        }
      };

      int tries = 0;
      int numRegs = (splitKeys == null ? 1 : splitKeys.length + 1) * desc.getRegionReplication();
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        actualRegCount.set(0);
        MetaTableAccessor.scanMetaForTableRegions(getAdmin().getConnection(), visitor,
          desc.getTableName());
        if (actualRegCount.get() == numRegs) {
          // all the regions are online
          return;
        }

        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when opening" + " regions; "
              + actualRegCount.get() + " of " + numRegs + " regions processed so far");
        }
      }
      throw new TimeoutException("Only " + actualRegCount.get() + " of " + numRegs
          + " regions are online; retries exhausted.");
    }

    protected void waitTableNotFound(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new WaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return !getAdmin().tableExists(tableName);
        }

        @Override
        public void throwInterruptedException() throws InterruptedIOException {
          throw new InterruptedIOException("Interrupted when waiting for table to be deleted");
        }

        @Override
        public void throwTimeoutException(long elapsedTime) throws TimeoutException {
          throw new TimeoutException("Table " + tableName + " not yet deleted after "
              + elapsedTime + "msec");
        }
      });
    }
  }
}
