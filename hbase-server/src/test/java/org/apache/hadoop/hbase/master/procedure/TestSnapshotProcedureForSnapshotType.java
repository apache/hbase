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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestSnapshotProcedureForSnapshotType extends TestSnapshotProcedure {
  private String testMethodName;

  @BeforeEach
  public void setTestMethod(TestInfo testInfo) {
    testMethodName = testInfo.getTestMethod().get().getName();
  }

  @Test
  public void testSnapshotTypeForEnabledTable() throws IOException {
    TableName tableName = TableName.valueOf(testMethodName);
    String snapshotName = "snapshot_" + testMethodName;
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    admin.createTable(tableDescriptorBuilder.build());

    assertTrue(admin.tableExists(tableName));
    assertTrue(admin.isTableEnabled(tableName));

    admin.snapshot(snapshotName, tableName);
    Optional<SnapshotDescription> optional =
      admin.listSnapshots().stream().filter(s -> snapshotName.equals(s.getName())).findFirst();
    assertTrue(optional.isPresent());
    SnapshotDescription snapshotDescription = optional.get();
    assertEquals(SnapshotType.FLUSH, snapshotDescription.getType());
  }

  @Test
  public void testSnapshotTypeForDisabledTable() throws IOException {
    TableName tableName = TableName.valueOf(testMethodName);
    String snapshotName = "snapshot_" + testMethodName;
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    admin.createTable(tableDescriptorBuilder.build());
    assertTrue(admin.tableExists(tableName));
    assertTrue(admin.isTableEnabled(tableName));

    admin.disableTable(tableName);
    assertTrue(admin.isTableDisabled(tableName));

    admin.snapshot(snapshotName, tableName);
    Optional<SnapshotDescription> optional =
      admin.listSnapshots().stream().filter(s -> snapshotName.equals(s.getName())).findFirst();
    assertTrue(optional.isPresent());
    SnapshotDescription snapshotDescription = optional.get();
    assertEquals(SnapshotType.DISABLED, snapshotDescription.getType());
  }
}
