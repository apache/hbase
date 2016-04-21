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
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Pair;

/**
 * An implementation of {@link RegionLocator}. Used to view region location information for a single
 * HBase table. Lightweight. Get as needed and just close when done. Instances of this class SHOULD
 * NOT be constructed directly. Obtain an instance via {@link Connection}. See
 * {@link ConnectionFactory} class comment for an example of how.
 *
 * <p> This class is thread safe
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class HRegionLocator implements RegionLocator {

  private final TableName tableName;
  private final ClusterConnection connection;

  public HRegionLocator(TableName tableName, ClusterConnection connection) {
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

  /**
   * {@inheritDoc}
   */
  @Override
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HRegionLocation getRegionLocation(final byte [] row, boolean reload)
  throws IOException {
    return connection.getRegionLocation(tableName, row, reload);
  }

  @Override
  public List<HRegionLocation> getAllRegionLocations() throws IOException {
    TableName tableName = getName();
    List<Pair<HRegionInfo, ServerName>> locations =
        MetaTableAccessor.getTableRegionsAndLocations(this.connection, tableName);
    ArrayList<HRegionLocation> regions = new ArrayList<>(locations.size());
    for (Pair<HRegionInfo, ServerName> entry : locations) {
      regions.add(new HRegionLocation(entry.getFirst(), entry.getSecond()));

    }
    if (regions.size() > 0) {
      connection.cacheLocation(tableName, new RegionLocations(regions));
    }
    return regions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    return getStartEndKeys(listRegionLocations());
  }

  @VisibleForTesting
  Pair<byte[][], byte[][]> getStartEndKeys(List<RegionLocations> regions) {
    final byte[][] startKeyList = new byte[regions.size()][];
    final byte[][] endKeyList = new byte[regions.size()][];

    for (int i = 0; i < regions.size(); i++) {
      HRegionInfo region = regions.get(i).getRegionLocation().getRegionInfo();
      startKeyList[i] = region.getStartKey();
      endKeyList[i] = region.getEndKey();
    }

    return new Pair<>(startKeyList, endKeyList);
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @VisibleForTesting
  List<RegionLocations> listRegionLocations() throws IOException {
    final List<RegionLocations> regions = new ArrayList<RegionLocations>();
    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.TableVisitorBase(tableName) {
      @Override
      public boolean visitInternal(Result result) throws IOException {
        RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
        if (locations == null) return true;
        regions.add(locations);
        return true;
      }
    };
    MetaTableAccessor.scanMetaForTableRegions(connection, visitor, tableName);
    return regions;
  }

  public Configuration getConfiguration() {
    return connection.getConfiguration();
  }
}
