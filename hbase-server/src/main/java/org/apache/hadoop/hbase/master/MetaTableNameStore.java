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
 * Stores and retrieves the meta table name for this cluster in the Master Local Region.
 * <p>
 * This provides cluster-specific storage for the meta table name.
 * <p>
 */
@InterfaceAudience.Private
public class MetaTableNameStore {
  private static final Logger LOG = LoggerFactory.getLogger(MetaTableNameStore.class);
  private static final byte[] META_TABLE_NAME_ROW = Bytes.toBytes("meta_table_name");
  private static final byte[] INFO_FAMILY = Bytes.toBytes("info");
  private static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");

  private final MasterRegion masterRegion;
  private volatile TableName cachedMetaTableName;

  public MetaTableNameStore(MasterRegion masterRegion) {
    this.masterRegion = masterRegion;
  }

  /**
   * Store the meta table name in the Master Local Region.
   * <p>
   * This should be called once during cluster initialization (InitMetaProcedure).
   * The stored value is cluster-specific and should not conflict with other clusters
   * sharing the same HDFS.
   * @param metaTableName the meta table name to store
   * @throws IOException if the storage operation fails
   */
  public void store(TableName metaTableName) throws IOException {
    LOG.info("Storing meta table name in Master Local Region: {}", metaTableName);
    Put put = new Put(META_TABLE_NAME_ROW);
    put.addColumn(INFO_FAMILY, NAME_QUALIFIER,
      Bytes.toBytes(metaTableName.getNameAsString()));
    masterRegion.update(r -> r.put(put));
    cachedMetaTableName = metaTableName;
    LOG.info("Successfully stored meta table name: {}", metaTableName);
  }

  /**
   * Load the meta table name from the Master Local Region.
   * <p>
   * @return the meta table name for this cluster
   * @throws IOException if the load operation fails
   */
  public TableName load() throws IOException {
    if (cachedMetaTableName != null) {
      return cachedMetaTableName;
    }

    synchronized (this) {
      if (cachedMetaTableName != null) {
        return cachedMetaTableName;
      }
      Get get = new Get(META_TABLE_NAME_ROW);
      get.addColumn(INFO_FAMILY, NAME_QUALIFIER);
      Result result = masterRegion.get(get);

      if (!result.isEmpty()) {
        byte[] value = result.getValue(INFO_FAMILY, NAME_QUALIFIER);
        cachedMetaTableName = TableName.valueOf(Bytes.toString(value));
        LOG.debug("Loaded meta table name from Master Local Region: {}", cachedMetaTableName);
        return cachedMetaTableName;
      }
      cachedMetaTableName = TableName.valueOf("hbase", "meta");
      LOG.info("No stored meta table name found in Master Local Region, using default: {}",
        cachedMetaTableName);
      return cachedMetaTableName;
    }
  }

  /**
   * Check if a meta table name has been stored in the Master Local Region.
   * @return true if a meta table name is stored, false otherwise
   * @throws IOException if the check operation fails
   */
  public boolean isStored() throws IOException {
    Get get = new Get(META_TABLE_NAME_ROW);
    get.addColumn(INFO_FAMILY, NAME_QUALIFIER);
    Result result = masterRegion.get(get);
    return !result.isEmpty();
  }
}

