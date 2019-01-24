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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An implementation of {@link RegionLocator}. Used to view region location information for a single
 * HBase table. Lightweight. Get as needed and just close when done. Instances of this class SHOULD
 * NOT be constructed directly. Obtain an instance via {@link Connection}. See
 * {@link ConnectionFactory} class comment for an example of how.
 * <p/>
 * This class is thread safe
 */
@InterfaceAudience.Private
public class HRegionLocator implements RegionLocator {

  private final TableName tableName;
  private final ConnectionImplementation connection;

  public HRegionLocator(TableName tableName, ConnectionImplementation connection) {
    this.connection = connection;
    this.tableName = tableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    // This method is required by the RegionLocator interface. This implementation does not have any
    // persistent state, so there is no need to do anything here.
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] row, int replicaId, boolean reload)
      throws IOException {
    return connection.locateRegion(tableName, row, !reload, true, replicaId)
      .getRegionLocation(replicaId);
  }

  @Override
  public List<HRegionLocation> getRegionLocations(byte[] row, boolean reload) throws IOException {
    RegionLocations locs =
      connection.locateRegion(tableName, row, !reload, true, RegionInfo.DEFAULT_REPLICA_ID);
    return Arrays.asList(locs.getRegionLocations());
  }

  @Override
  public List<HRegionLocation> getAllRegionLocations() throws IOException {
    ArrayList<HRegionLocation> regions = new ArrayList<>();
    for (RegionLocations locations : listRegionLocations()) {
      for (HRegionLocation location : locations.getRegionLocations()) {
        regions.add(location);
      }
      connection.cacheLocation(tableName, locations);
    }
    return regions;
  }

  @Override
  public void clearRegionLocationCache() {
    connection.clearRegionCache(tableName);
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  private List<RegionLocations> listRegionLocations() throws IOException {
    if (TableName.isMetaTableName(tableName)) {
      return Collections
        .singletonList(connection.locateRegion(tableName, HConstants.EMPTY_START_ROW, false, true));
    }
    final List<RegionLocations> regions = new ArrayList<>();
    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.TableVisitorBase(tableName) {
      @Override
      public boolean visitInternal(Result result) throws IOException {
        RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
        if (locations == null) {
          return true;
        }
        regions.add(locations);
        return true;
      }
    };
    MetaTableAccessor.scanMetaForTableRegions(connection, visitor, tableName);
    return regions;
  }
}
