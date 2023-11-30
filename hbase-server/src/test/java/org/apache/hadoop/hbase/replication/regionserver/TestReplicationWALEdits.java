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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestReplicationWALEdits {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationWALEdits.class);

  private static final Configuration CONF = HBaseConfiguration.create();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] F1 = Bytes.toBytes("f1");

  private static final byte[] F2 = Bytes.toBytes("f2");

  private static final RegionInfo RI = RegionInfoBuilder.newBuilder(TABLE_NAME).build();

  /**
   * Test for HBASE-9038, Replication.scopeWALEdits would NPE if it wasn't filtering out the
   * compaction WALEdit.
   */
  @Test
  public void testCompactionWALEdits() throws Exception {
    TableName tableName = TableName.valueOf("testCompactionWALEdits");
    WALProtos.CompactionDescriptor compactionDescriptor =
      WALProtos.CompactionDescriptor.getDefaultInstance();
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).setStartKey(HConstants.EMPTY_START_ROW)
      .setEndKey(HConstants.EMPTY_END_ROW).build();
    WALEdit edit = WALEdit.createCompaction(hri, compactionDescriptor);
    ReplicationSourceWALActionListener.scopeWALEdits(new WALKeyImpl(), edit, CONF);
  }

  private WALEdit getBulkLoadWALEdit(NavigableMap<byte[], Integer> scope) {
    // 1. Create store files for the families
    Map<byte[], List<Path>> storeFiles = new HashMap<>(1);
    Map<String, Long> storeFilesSize = new HashMap<>(1);
    List<Path> p = new ArrayList<>(1);
    Path hfilePath1 = new Path(Bytes.toString(F1));
    p.add(hfilePath1);
    storeFilesSize.put(hfilePath1.getName(), 0L);
    storeFiles.put(F1, p);
    scope.put(F1, 1);
    p = new ArrayList<>(1);
    Path hfilePath2 = new Path(Bytes.toString(F2));
    p.add(hfilePath2);
    storeFilesSize.put(hfilePath2.getName(), 0L);
    storeFiles.put(F2, p);
    // 2. Create bulk load descriptor
    BulkLoadDescriptor desc = ProtobufUtil.toBulkLoadDescriptor(RI.getTable(),
      UnsafeByteOperations.unsafeWrap(RI.getEncodedNameAsBytes()), storeFiles, storeFilesSize, 1);

    // 3. create bulk load wal edit event
    WALEdit logEdit = WALEdit.createBulkLoadEvent(RI, desc);
    return logEdit;
  }

  @Test
  public void testBulkLoadWALEditsWithoutBulkLoadReplicationEnabled() throws Exception {
    NavigableMap<byte[], Integer> scope = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    // 1. Get the bulk load wal edit event
    WALEdit logEdit = getBulkLoadWALEdit(scope);
    // 2. Create wal key
    WALKeyImpl logKey = new WALKeyImpl(scope);

    // 3. Get the scopes for the key
    ReplicationSourceWALActionListener.scopeWALEdits(logKey, logEdit, CONF);

    // 4. Assert that no bulk load entry scopes are added if bulk load hfile replication is disabled
    assertNull("No bulk load entries scope should be added if bulk load replication is disabled.",
      logKey.getReplicationScopes());
  }

  @Test
  public void testBulkLoadWALEdits() throws Exception {
    // 1. Get the bulk load wal edit event
    NavigableMap<byte[], Integer> scope = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    WALEdit logEdit = getBulkLoadWALEdit(scope);
    // 2. Create wal key
    WALKeyImpl logKey = new WALKeyImpl(scope);
    // 3. Enable bulk load hfile replication
    Configuration bulkLoadConf = HBaseConfiguration.create(CONF);
    bulkLoadConf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);

    // 4. Get the scopes for the key
    ReplicationSourceWALActionListener.scopeWALEdits(logKey, logEdit, bulkLoadConf);

    NavigableMap<byte[], Integer> scopes = logKey.getReplicationScopes();
    // Assert family with replication scope global is present in the key scopes
    assertTrue("This family scope is set to global, should be part of replication key scopes.",
      scopes.containsKey(F1));
    // Assert family with replication scope local is not present in the key scopes
    assertFalse("This family scope is set to local, should not be part of replication key scopes",
      scopes.containsKey(F2));
  }
}
