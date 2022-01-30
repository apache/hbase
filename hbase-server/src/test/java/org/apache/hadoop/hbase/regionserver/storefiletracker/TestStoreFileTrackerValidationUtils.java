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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.Assert.assertThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileTrackerValidationUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileTrackerValidationUtils.class);

  @Test
  public void testCheckSFTCompatibility() throws Exception {
    // checking default value change on different configuration levels
    Configuration conf = new Configuration();
    conf.set(StoreFileTrackerFactory.TRACKER_IMPL, "DEFAULT");

    // creating a TD with only TableDescriptor level config
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("TableX"));
    builder.setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE");
    ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of("cf");
    builder.setColumnFamily(cf);
    TableDescriptor td = builder.build();

    // creating a TD with matching ColumnFamilyDescriptor level setting
    TableDescriptorBuilder snapBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("TableY"));
    snapBuilder.setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE");
    ColumnFamilyDescriptorBuilder snapCFBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
    snapCFBuilder.setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE");
    snapBuilder.setColumnFamily(snapCFBuilder.build());
    TableDescriptor snapTd = snapBuilder.build();

    // adding a cf config that matches the td config is fine even when it does not match the default
    StoreFileTrackerValidationUtils.validatePreRestoreSnapshot(td, snapTd, conf);
    // removing cf level config is fine when it matches the td config
    StoreFileTrackerValidationUtils.validatePreRestoreSnapshot(snapTd, td, conf);

    TableDescriptorBuilder defaultBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("TableY"));
    defaultBuilder.setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE");
    ColumnFamilyDescriptorBuilder defaultCFBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
    defaultCFBuilder.setValue(StoreFileTrackerFactory.TRACKER_IMPL, "DEFAULT");
    defaultBuilder.setColumnFamily(defaultCFBuilder.build());
    TableDescriptor defaultTd = defaultBuilder.build();

    assertThrows(RestoreSnapshotException.class, () -> {
      StoreFileTrackerValidationUtils.validatePreRestoreSnapshot(td, defaultTd, conf);
    });
    assertThrows(RestoreSnapshotException.class, () -> {
      StoreFileTrackerValidationUtils.validatePreRestoreSnapshot(snapTd, defaultTd, conf);
    });
  }
}
