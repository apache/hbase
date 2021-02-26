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
package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

final class TestHDFSAclHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TestHDFSAclHelper.class);

  private TestHDFSAclHelper() {
  }

  static void grantOnTable(HBaseTestingUtility util, String user, TableName tableName,
                           Permission.Action... actions) throws Exception {
    SecureTestUtil.grantOnTable(util, user, tableName, null, null, actions);
  }

  static void createNamespace(HBaseTestingUtility util, String namespace) throws IOException {
    if (Arrays.stream(util.getAdmin().listNamespaceDescriptors())
        .noneMatch(ns -> ns.getName().equals(namespace))) {
      NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
      util.getAdmin().createNamespace(namespaceDescriptor);
    }
  }

  static Table createTable(HBaseTestingUtility util, TableName tableName) throws IOException {
    createNamespace(util, tableName.getNamespaceAsString());
    TableDescriptor td = getTableDescriptorBuilder(util, tableName)
        .setValue(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, "true").build();
    byte[][] splits = new byte[][] { Bytes.toBytes("2"), Bytes.toBytes("4") };
    return util.createTable(td, splits);
  }

  static Table createMobTable(HBaseTestingUtility util, TableName tableName) throws IOException {
    createNamespace(util, tableName.getNamespaceAsString());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN1).setMobEnabled(true)
            .setMobThreshold(0).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN2).setMobEnabled(true)
            .setMobThreshold(0).build())
        .setOwner(User.createUserForTesting(util.getConfiguration(), "owner", new String[] {}))
        .setValue(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, "true").build();
    byte[][] splits = new byte[][] { Bytes.toBytes("2"), Bytes.toBytes("4") };
    return util.createTable(td, splits);
  }

  static TableDescriptor createUserScanSnapshotDisabledTable(HBaseTestingUtility util,
      TableName tableName) throws IOException {
    createNamespace(util, tableName.getNamespaceAsString());
    TableDescriptor td = getTableDescriptorBuilder(util, tableName).build();
    byte[][] splits = new byte[][] { Bytes.toBytes("2"), Bytes.toBytes("4") };
    try (Table t = util.createTable(td, splits)) {
      put(t);
    }
    return td;
  }

  static TableDescriptorBuilder getTableDescriptorBuilder(HBaseTestingUtility util,
      TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN1).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN2).build())
        .setOwner(User.createUserForTesting(util.getConfiguration(), "owner", new String[] {}));
  }

  static void createTableAndPut(HBaseTestingUtility util, TableName tableNam) throws IOException {
    try (Table t = createTable(util, tableNam)) {
      put(t);
    }
  }

  static final byte[] COLUMN1 = Bytes.toBytes("A");
  static final byte[] COLUMN2 = Bytes.toBytes("B");

  static void put(Table hTable) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(COLUMN1, null, Bytes.toBytes(i));
      put.addColumn(COLUMN2, null, Bytes.toBytes(i + 1));
      puts.add(put);
    }
    hTable.put(puts);
  }

  static void put2(Table hTable) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        continue;
      }
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(COLUMN1, null, Bytes.toBytes(i + 2));
      put.addColumn(COLUMN2, null, Bytes.toBytes(i + 3));
      puts.add(put);
    }
    hTable.put(puts);
  }

  /**
   * Check if user is able to read expected rows from the specific snapshot
   * @param user the specific user
   * @param snapshot the snapshot to be scanned
   * @param expectedRowCount expected row count read from snapshot, -1 if expects
   *          AccessControlException
   * @throws IOException user scan snapshot error
   * @throws InterruptedException user scan snapshot error
   */
  static void canUserScanSnapshot(HBaseTestingUtility util, User user, String snapshot,
      int expectedRowCount) throws IOException, InterruptedException {
    PrivilegedExceptionAction<Void> action =
        getScanSnapshotAction(util.getConfiguration(), snapshot, expectedRowCount);
    user.runAs(action);
  }

  static PrivilegedExceptionAction<Void> getScanSnapshotAction(Configuration conf,
                                                               String snapshotName, long expectedRowCount) {
    return () -> {
      try {
        Path restoreDir = new Path(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
        Scan scan = new Scan();
        TableSnapshotScanner scanner =
            new TableSnapshotScanner(conf, restoreDir, snapshotName, scan);
        int rowCount = 0;
        while (true) {
          Result result = scanner.next();
          if (result == null) {
            break;
          }
          rowCount++;
        }
        scanner.close();
        assertEquals(expectedRowCount, rowCount);
      } catch (Exception e) {
        LOG.debug("Scan snapshot error, snapshot {}", snapshotName, e);
        assertEquals(expectedRowCount, -1);
      }
      return null;
    };
  }
}
