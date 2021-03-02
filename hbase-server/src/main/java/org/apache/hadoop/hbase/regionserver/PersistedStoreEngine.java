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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * The StoreEngine that implements persisted and renameless store compaction and flush
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class PersistedStoreEngine extends DefaultStoreEngine {

  @Override
  public void createComponents(
    Configuration conf, HStore store, CellComparator kvComparator) throws IOException {
    Preconditions.checkArgument(StoreFileTrackingUtils.isStoreFileTrackingPersistEnabled(conf));

    createCompactor(conf, store);
    createCompactionPolicy(conf, store);
    createStoreFlusher(conf, store);
    createStoreFileManager(conf, store, kvComparator);
  }

  @Override
  protected void createStoreFileManager(Configuration conf, HStore store,
    CellComparator kvComparator) {
    TableName tableName = store.getTableName();
    // for master region, hbase:meta and hbase:storefile table, DefaultStoreManager is used.
    // such these tables scan from the filesystem directly
    if (tableName.equals(TableName.META_TABLE_NAME)
      || tableName.equals(MasterRegionFactory.TABLE_NAME)
      || tableName.equals(TableName.STOREFILE_TABLE_NAME)) {
      super.createStoreFileManager(conf, store, kvComparator);
      return;
    }

    RegionServerServices regionServerServices = store.getHRegion().getRegionServerServices();
    Connection connection = regionServerServices.getConnection();
    boolean readOnly = store.getHRegion().isReadOnly();

    storeFileManager =
      new PersistedStoreFileManager(kvComparator, StoreFileComparators.SEQ_ID, conf,
        compactionPolicy.getConf(), store.getRegionFileSystem(), store.getRegionInfo(),
        store.getColumnFamilyName(),
        StoreFileTrackingUtils.createStoreFilePathAccessor(conf, connection),
        readOnly);
  }
}
