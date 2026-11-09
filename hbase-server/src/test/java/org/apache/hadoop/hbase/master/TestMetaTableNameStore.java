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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.region.MasterRegionParams;
import org.apache.hadoop.hbase.master.region.MasterRegionTestBase;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests the round-trip of {@link MetaTableNameStore} across a master-local-region restart: a value
 * stored before the region is closed must be loaded back verbatim once the region is re-created
 * against the same on-disk data. Also covers the first-boot bootstrap case where no value has been
 * stored yet.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestMetaTableNameStore extends MasterRegionTestBase {

  @Override
  protected void configure(MasterRegionParams params) {
    // The default TD on MasterRegionTestBase only has f1/f2; MetaTableNameStore writes to the
    // production STATE_FAMILY ("state"), so swap in a TD that keeps the same TableName (so the
    // on-disk region dir is consistent across restart) but adds the state family. Build on top of
    // whatever the base class already set — in particular preserving its TRACKER_IMPL table-value
    // which is required by MasterRegion.create on re-open.
    TableDescriptor base = params.tableDescriptor();
    params.tableDescriptor(TableDescriptorBuilder.newBuilder(base)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(STATE_FAMILY)).build());
  }

  @Test
  public void testLoadReturnsNullBeforeAnyStore() throws IOException {
    MetaTableNameStore store = new MetaTableNameStore(region);
    assertNull(store.load(),
      "load() must return null before any value has been persisted (first-boot bootstrap)");
  }

  @Test
  public void testStoreThenLoadDefaultMeta() throws IOException {
    MetaTableNameStore store = new MetaTableNameStore(region);
    store.store(TableName.META_TABLE_NAME);
    assertEquals(TableName.META_TABLE_NAME, store.load(),
      "load() must return the value just stored");
  }

  @Test
  public void testStoreThenLoadCustomSuffixMeta() throws IOException {
    MetaTableNameStore store = new MetaTableNameStore(region);
    TableName custom = TableName.valueOf("hbase:meta_cluster1");
    store.store(custom);
    assertEquals(custom, store.load(),
      "load() must preserve a custom cluster-specific meta table name verbatim");
  }

  @Test
  public void testRoundTripAcrossRegionRestart() throws IOException {
    // Store a value through the first MasterRegion instance.
    TableName persisted = TableName.valueOf("hbase:meta_restartcheck");
    new MetaTableNameStore(region).store(persisted);

    // Close and re-create the MasterRegion against the same test dir — simulates a master
    // restart. createMasterRegion() on MasterRegionTestBase reuses htu's data test dir.
    region.close(false);
    createMasterRegion();

    // A fresh store wrapping the re-opened region must see the value we persisted earlier.
    assertEquals(persisted, new MetaTableNameStore(region).load(),
      "Persisted meta table name must survive a MasterRegion close/re-open cycle");
  }

  @Test
  public void testOverwriteStoreUpdatesPersistedValue() throws IOException {
    MetaTableNameStore store = new MetaTableNameStore(region);
    store.store(TableName.valueOf("hbase:meta_first"));
    store.store(TableName.valueOf("hbase:meta_second"));
    // The second store must replace the first — read-back returns the second value.
    assertEquals(TableName.valueOf("hbase:meta_second"), store.load(),
      "Second store() must overwrite the first; load() returns the latest persisted value");
  }
}
