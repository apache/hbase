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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementations call a RegionServer and implement {@link #call(int)}.
 * Passed to a {@link RpcRetryingCaller} so we retry on fail.
 * TODO: this class is actually tied to one region, because most of the paths make use of
 *       the regioninfo part of location when building requests. The only reason it works for
 *       multi-region requests (e.g. batch) is that they happen to not use the region parts.
 *       This could be done cleaner (e.g. having a generic parameter and 2 derived classes,
 *       RegionCallable and actual RegionServerCallable with ServerName.
 * @param <T> the class that the ServerCallable handles
 */
@InterfaceAudience.Private
public abstract class RegionServerCallable<T> implements RetryingCallable<T> {
  // Public because used outside of this package over in ipc.
  private static final Log LOG = LogFactory.getLog(RegionServerCallable.class);
  protected final Connection connection;
  protected final TableName tableName;
  protected final byte[] row;
  protected HRegionLocation location;
  private ClientService.BlockingInterface stub;

  protected final static int MIN_WAIT_DEAD_SERVER = 10000;

  /**
   * @param connection Connection to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
   */
  public RegionServerCallable(Connection connection, TableName tableName, byte [] row) {
    this.connection = connection;
    this.tableName = tableName;
    this.row = row;
  }

  /**
   * Prepare for connection to the server hosting region with row from tablename.  Does lookup
   * to find region location and hosting server.
   * @param reload Set to true to re-check the table state
   * @throws IOException e
   */
  @Override
  public void prepare(final boolean reload) throws IOException {
    // check table state if this is a retry
    if (reload &&
        !tableName.equals(TableName.META_TABLE_NAME) &&
        getConnection().isTableDisabled(tableName)) {
      throw new TableNotEnabledException(tableName.getNameAsString() + " is disabled.");
    }
    try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
      this.location = regionLocator.getRegionLocation(row);
    }
    if (this.location == null) {
      throw new IOException("Failed to find location, tableName=" + tableName +
        ", row=" + Bytes.toString(row) + ", reload=" + reload);
    }
    setStub(getConnection().getClient(this.location.getServerName()));
  }

  /**
   * @return {@link HConnection} instance used by this Callable.
   */
  HConnection getConnection() {
    return (HConnection) this.connection;
  }

  protected ClientService.BlockingInterface getStub() {
    return this.stub;
  }

  void setStub(final ClientService.BlockingInterface stub) {
    this.stub = stub;
  }

  protected HRegionLocation getLocation() {
    return this.location;
  }

  protected void setLocation(final HRegionLocation location) {
    this.location = location;
  }

  public TableName getTableName() {
    return this.tableName;
  }

  public byte [] getRow() {
    return this.row;
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
    if (location != null) {
      getConnection().updateCachedLocations(tableName, location.getRegionInfo().getRegionName(),
          row, t, location.getServerName());
    }
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return "row '" + Bytes.toString(row) + "' on table '" + tableName + "' at " + location;
  }

  @Override
  public long sleep(long pause, int tries) {
    long sleep = ConnectionUtils.getPauseTime(pause, tries);
    if (sleep < MIN_WAIT_DEAD_SERVER
        && (location == null || getConnection().isDeadServer(location.getServerName()))) {
      sleep = ConnectionUtils.addJitter(MIN_WAIT_DEAD_SERVER, 0.10f);
    }
    return sleep;
  }

  /**
   * @return the HRegionInfo for the current region
   */
  public HRegionInfo getHRegionInfo() {
    if (this.location == null) {
      return null;
    }
    return this.location.getRegionInfo();
  }
}
