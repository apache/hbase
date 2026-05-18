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
package org.apache.hadoop.hbase.master.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Unfortunately, couldn't test TakeSnapshotHandler using mocks, because it relies on TableLock,
 * which is tightly coupled to LockManager and LockProcedure classes, which are both final and
 * prevents us from mocking its behaviour. Looks like an overkill having to emulate a whole cluster
 * run for such a small optional property behaviour.
 */
@Tag(MediumTests.TAG)
public class TestTakeSnapshotHandler {

  private static HBaseTestingUtil UTIL;
  private String currentTestName;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    currentTestName = testInfo.getTestMethod().get().getName();
    UTIL = new HBaseTestingUtil();
  }

  public TableDescriptor createTableInsertDataAndTakeSnapshot(Map<String, Object> snapshotProps)
    throws Exception {
    TableDescriptor descriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(currentTestName))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f")).build())
        .build();
    UTIL.getConnection().getAdmin().createTable(descriptor);
    Table table = UTIL.getConnection().getTable(descriptor.getTableName());
    Put put = new Put(Bytes.toBytes("1"));
    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("1"), Bytes.toBytes("v1"));
    table.put(put);
    String snapName = "snap" + currentTestName;
    UTIL.getAdmin().snapshot(snapName, descriptor.getTableName(), snapshotProps);
    TableName cloned = TableName.valueOf(currentTestName + "clone");
    UTIL.getAdmin().cloneSnapshot(snapName, cloned);
    return descriptor;
  }

  @Test
  public void testPreparePreserveMaxFileSizeEnabled() throws Exception {
    UTIL.startMiniCluster();
    Map<String, Object> snapshotProps = new HashMap<>();
    snapshotProps.put(TableDescriptorBuilder.MAX_FILESIZE, Long.parseLong("21474836480"));
    TableDescriptor descriptor = createTableInsertDataAndTakeSnapshot(snapshotProps);
    TableName cloned = TableName.valueOf(currentTestName + "clone");
    assertEquals(-1, UTIL.getAdmin().getDescriptor(descriptor.getTableName()).getMaxFileSize());
    assertEquals(21474836480L, UTIL.getAdmin().getDescriptor(cloned).getMaxFileSize());
  }

  @Test
  public void testPreparePreserveMaxFileSizeDisabled() throws Exception {
    UTIL.startMiniCluster();
    TableDescriptor descriptor = createTableInsertDataAndTakeSnapshot(null);
    TableName cloned = TableName.valueOf(currentTestName + "clone");
    assertEquals(-1, UTIL.getAdmin().getDescriptor(descriptor.getTableName()).getMaxFileSize());
    assertEquals(-1, UTIL.getAdmin().getDescriptor(cloned).getMaxFileSize());
  }

  @Test
  public void testSnapshotEarlyExpiration() throws Exception {
    UTIL.startMiniCluster();
    Map<String, Object> snapshotProps = new HashMap<>();
    snapshotProps.put("TTL", 1L);
    assertThrows(SnapshotTTLExpiredException.class, () -> {
      createTableInsertDataAndTakeSnapshot(snapshotProps);
    });
  }

  @AfterEach
  public void shutdown() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
