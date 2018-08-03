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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility used by client connections.
 */
@InterfaceAudience.Private
public class ConnectionUtils {

  /**
   * Calculate pause time.
   * Built on {@link HConstants#RETRY_BACKOFF}.
   * @param pause
   * @param tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= HConstants.RETRY_BACKOFF.length) {
      ntries = HConstants.RETRY_BACKOFF.length - 1;
    }
    if (ntries < 0) {
      ntries = 0;
    }

    long normalPause = pause * HConstants.RETRY_BACKOFF[ntries];
    // 1% possible jitter
    long jitter = (long) (normalPause * ThreadLocalRandom.current().nextFloat() * 0.01f);
    return normalPause + jitter;
  }


  /**
   * @param conn The connection for which to replace the generator.
   * @param cnm Replaces the nonce generator used, for testing.
   * @return old nonce generator.
   */
  public static NonceGenerator injectNonceGeneratorForTesting(
      ClusterConnection conn, NonceGenerator cnm) {
    return ConnectionManager.injectNonceGeneratorForTesting(conn, cnm);
  }

  /**
   * Changes the configuration to set the number of retries needed when using HConnection
   * internally, e.g. for  updating catalog tables, etc.
   * Call this method before we create any Connections.
   * @param c The Configuration instance to set the retries into.
   * @param log Used to log what we set in here.
   */
  public static void setServerSideHConnectionRetriesConfig(
      final Configuration c, final String sn, final Log log) {
    // TODO: Fix this. Not all connections from server side should have 10 times the retries.
    int hcRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // Go big.  Multiply by 10.  If we can't get to meta after this many retries
    // then something seriously wrong.
    int serversideMultiplier = c.getInt("hbase.client.serverside.retries.multiplier", 10);
    int retries = hcRetries * serversideMultiplier;
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    log.info(sn + " server-side HConnection retries=" + retries);
  }

  /**
   * Adapt a HConnection so that it can bypass the RPC layer (serialization,
   * deserialization, networking, etc..) -- i.e. short-circuit -- when talking to a local server.
   * @param conn the connection to adapt
   * @param serverName the local server name
   * @param admin the admin interface of the local server
   * @param client the client interface of the local server
   * @return an adapted/decorated HConnection
   */
  @Deprecated
  public static ClusterConnection createShortCircuitHConnection(final Connection conn,
      final ServerName serverName, final AdminService.BlockingInterface admin,
      final ClientService.BlockingInterface client) {
    return new ConnectionAdapter(conn) {
      @Override
      public AdminService.BlockingInterface getAdmin(
          ServerName sn, boolean getMaster) throws IOException {
        return serverName.equals(sn) ? admin : super.getAdmin(sn, getMaster);
      }

      @Override
      public ClientService.BlockingInterface getClient(
          ServerName sn) throws IOException {
        return serverName.equals(sn) ? client : super.getClient(sn);
      }
    };
  }

  /**
   * Creates a short-circuit connection that can bypass the RPC layer (serialization,
   * deserialization, networking, etc..) when talking to a local server.
   * @param conf the current configuration
   * @param pool the thread pool to use for batch operations
   * @param user the user the connection is for
   * @param serverName the local server name
   * @param admin the admin interface of the local server
   * @param client the client interface of the local server
   * @return a short-circuit connection.
   * @throws IOException
   */
  public static ClusterConnection createShortCircuitConnection(final Configuration conf,
    ExecutorService pool, User user, final ServerName serverName,
    final AdminService.BlockingInterface admin, final ClientService.BlockingInterface client)
    throws IOException {
    if (user == null) {
      user = UserProvider.instantiate(conf).getCurrent();
    }
    return new ConnectionManager.HConnectionImplementation(conf, false, pool, user) {
      @Override
      public AdminService.BlockingInterface getAdmin(ServerName sn, boolean getMaster)
        throws IOException {
        return serverName.equals(sn) ? admin : super.getAdmin(sn, getMaster);
      }

      @Override
      public ClientService.BlockingInterface getClient(ServerName sn) throws IOException {
        return serverName.equals(sn) ? client : super.getClient(sn);
      }
    };
  }

  /**
   * Setup the connection class, so that it will not depend on master being online. Used for testing
   * @param conf configuration to set
   */
  @VisibleForTesting
  public static void setupMasterlessConnection(Configuration conf) {
    conf.set(HConnection.HBASE_CLIENT_CONNECTION_IMPL,
      MasterlessConnection.class.getName());
  }

  /**
   * Some tests shut down the master. But table availability is a master RPC which is performed on
   * region re-lookups.
   */
  static class MasterlessConnection extends ConnectionManager.HConnectionImplementation {
    MasterlessConnection(Configuration conf, boolean managed,
      ExecutorService pool, User user) throws IOException {
      super(conf, managed, pool, user);
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException {
      // treat all tables as enabled
      return false;
    }
  }

  // A byte array in which all elements are the max byte, and it is used to
  // construct closest front row
  static final byte[] MAX_BYTE_ARRAY = Bytes.createMaxByteArray(9);

  /**
   * Create the closest row after the specified row
   */
  static byte[] createClosestRowAfter(byte[] row) {
    return Arrays.copyOf(row, row.length + 1);
  }

  /**
   * Create a row before the specified row and very close to the specified row.
   */
  static byte[] createCloseRowBefore(byte[] row) {
    if (row.length == 0) {
      return MAX_BYTE_ARRAY;
    }
    if (row[row.length - 1] == 0) {
      return Arrays.copyOf(row, row.length - 1);
    } else {
      byte[] nextRow = new byte[row.length + MAX_BYTE_ARRAY.length];
      System.arraycopy(row, 0, nextRow, 0, row.length - 1);
      nextRow[row.length - 1] = (byte) ((row[row.length - 1] & 0xFF) - 1);
      System.arraycopy(MAX_BYTE_ARRAY, 0, nextRow, row.length, MAX_BYTE_ARRAY.length);
      return nextRow;
    }
  }

  static boolean isEmptyStartRow(byte[] row) {
    return Bytes.equals(row, EMPTY_START_ROW);
  }

  static boolean isEmptyStopRow(byte[] row) {
    return Bytes.equals(row, EMPTY_END_ROW);
  }

  private static final Comparator<Cell> COMPARE_WITHOUT_ROW = new Comparator<Cell>() {

    @Override
    public int compare(Cell o1, Cell o2) {
      return CellComparator.compareWithoutRow(o1, o2);
    }
  };

  static Result filterCells(Result result, Cell keepCellsAfter) {
    if (keepCellsAfter == null) {
      // do not need to filter
      return result;
    }
    // not the same row
    if (!CellUtil.matchingRow(keepCellsAfter, result.getRow(), 0, result.getRow().length)) {
      return result;
    }
    Cell[] rawCells = result.rawCells();
    int index = Arrays.binarySearch(rawCells, keepCellsAfter, COMPARE_WITHOUT_ROW);
    if (index < 0) {
      index = -index - 1;
    } else {
      index++;
    }
    if (index == 0) {
      return result;
    }
    if (index == rawCells.length) {
      return null;
    }
    return Result.create(Arrays.copyOfRange(rawCells, index, rawCells.length), null,
      result.isStale(), result.mayHaveMoreCellsInRow());
  }

  static boolean noMoreResultsForScan(Scan scan, HRegionInfo info) {
    if (isEmptyStopRow(info.getEndKey())) {
      return true;
    }
    if (isEmptyStopRow(scan.getStopRow())) {
      return false;
    }
    int c = Bytes.compareTo(info.getEndKey(), scan.getStopRow());
    // 1. if our stop row is less than the endKey of the region
    // 2. if our stop row is equal to the endKey of the region and we do not include the stop row
    // for scan.
    return c > 0 || (c == 0 && !scan.includeStopRow());
  }

  static boolean noMoreResultsForReverseScan(Scan scan, HRegionInfo info) {
    if (isEmptyStartRow(info.getStartKey())) {
      return true;
    }
    if (isEmptyStopRow(scan.getStopRow())) {
      return false;
    }
    // no need to test the inclusive of the stop row as the start key of a region is included in
    // the region.
    return Bytes.compareTo(info.getStartKey(), scan.getStopRow()) <= 0;
  }

  public static ScanResultCache createScanResultCache(Scan scan, List<Result> cache) {
    if (scan.getAllowPartialResults()) {
      return new AllowPartialScanResultCache(cache);
    } else if (scan.getBatch() > 0) {
      return new BatchScanResultCache(cache, scan.getBatch());
    } else {
      return new CompleteScanResultCache(cache);
    }
  }
}
