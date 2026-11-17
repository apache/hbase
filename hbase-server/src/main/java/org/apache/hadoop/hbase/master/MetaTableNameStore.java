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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.master.region.MasterRegionFactory.STATE_FAMILY;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores and retrieves the meta table name for this cluster in the Master Local Region. This
 * provides cluster-specific storage for the meta table name.
 * <p>
 * This class is intentionally stateless: it is instantiated once by {@code HMaster} during
 * {@code finishActiveMasterInitialization()}, {@link #load()} is called exactly once, and
 * {@link #store(TableName)} is called at most once (on first-boot bootstrap). The caller
 * ({@code HMaster.cachedMetaTableName}) owns the in-memory cache of the resolved value, so there is
 * no benefit to caching here and no concurrency to protect.
 * <p>
 * Writes to the master region's {@code state} column family (same family used by sibling
 * {@code MasterStateStore} subclasses). This deliberately avoids the {@code info} /
 * {@link org.apache.hadoop.hbase.HConstants#CATALOG_FAMILY} family that holds meta region location
 * rows, since {@code HMaster.tryMigrateMetaLocationsFromZooKeeper} treats a non-empty info family
 * as "migration already done" — colocating the meta table name there was fragile and required
 * loosening {@code TestMigrateAndMirrorMetaLocations} to filter non-location rows out of its scans.
 */
@InterfaceAudience.Private
public class MetaTableNameStore {
  private static final Logger LOG = LoggerFactory.getLogger(MetaTableNameStore.class);
  private static final byte[] META_TABLE_NAME_ROW = Bytes.toBytes("meta_table_name");
  private static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");

  private final MasterRegion masterRegion;

  public MetaTableNameStore(MasterRegion masterRegion) {
    this.masterRegion = masterRegion;
  }

  /**
   * Store the meta table name in the Master Local Region. This should be called once during cluster
   * initialization. The stored value is cluster-specific and should not conflict with other
   * clusters sharing the same HDFS.
   * @param metaTableName the meta table name to store
   * @throws IOException if the operation fails
   */
  public void store(TableName metaTableName) throws IOException {
    LOG.info("Storing meta table name in Master Local Region: {}", metaTableName);
    Put put = new Put(META_TABLE_NAME_ROW);
    put.addColumn(STATE_FAMILY, NAME_QUALIFIER, Bytes.toBytes(metaTableName.getNameAsString()));
    masterRegion.update(r -> r.put(put));
    LOG.info("Successfully stored meta table name: {}", metaTableName);
  }

  /**
   * Load the meta table name from the Master Local Region.
   * @return the meta table name for this cluster, or {@code null} if no value has been persisted
   *         (i.e. first-boot bootstrap).
   * @throws IOException if the load operation fails
   */
  public TableName load() throws IOException {
    Get get = new Get(META_TABLE_NAME_ROW);
    get.addColumn(STATE_FAMILY, NAME_QUALIFIER);
    Result result = masterRegion.get(get);
    if (result.isEmpty()) {
      LOG.info("No stored meta table name found in Master Local Region (first-boot bootstrap)");
      return null;
    }
    byte[] value = result.getValue(STATE_FAMILY, NAME_QUALIFIER);
    TableName metaTableName = TableName.valueOf(Bytes.toString(value));
    LOG.debug("Loaded meta table name from Master Local Region: {}", metaTableName);
    return metaTableName;
  }
}
