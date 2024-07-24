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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertFalse;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test CopyTable between clusters
 */
public abstract class CopyTableToPeerClusterTestBase extends CopyTableTestBase {

  protected static final HBaseTestingUtil UTIL1 = new HBaseTestingUtil();

  protected static final HBaseTestingUtil UTIL2 = new HBaseTestingUtil();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL1.startMiniCluster(3);
    UTIL2.startMiniCluster(3);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL1.shutdownMiniCluster();
    UTIL2.shutdownMiniCluster();
  }

  @Override
  protected Table createSourceTable(TableDescriptor desc) throws Exception {
    return UTIL1.createTable(desc, null);
  }

  @Override
  protected Table createTargetTable(TableDescriptor desc) throws Exception {
    return UTIL2.createTable(desc, null);
  }

  @Override
  protected void dropSourceTable(TableName tableName) throws Exception {
    UTIL1.deleteTable(tableName);
  }

  @Override
  protected void dropTargetTable(TableName tableName) throws Exception {
    UTIL2.deleteTable(tableName);
  }

  @Override
  protected String[] getPeerClusterOptions() throws Exception {
    return new String[] { "--peer.uri=" + UTIL2.getRpcConnnectionURI() };
  }

  /**
   * Simple end-to-end test
   */
  @Test
  public void testCopyTable() throws Exception {
    doCopyTableTest(UTIL1.getConfiguration(), false);
  }

  /**
   * Simple end-to-end test on table with MOB
   */
  @Test
  public void testCopyTableWithMob() throws Exception {
    doCopyTableTestWithMob(UTIL1.getConfiguration(), false);
  }

  @Test
  public void testStartStopRow() throws Exception {
    testStartStopRow(UTIL1.getConfiguration());
  }

  /**
   * Test copy of table from sourceTable to targetTable all rows from family a
   */
  @Test
  public void testRenameFamily() throws Exception {
    testRenameFamily(UTIL1.getConfiguration());
  }

  @Test
  public void testBulkLoadNotSupported() throws Exception {
    TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    try (Table t1 = UTIL1.createTable(tableName1, FAMILY_A);
      Table t2 = UTIL2.createTable(tableName2, FAMILY_A)) {
      String[] args = ArrayUtils.addAll(getPeerClusterOptions(),
        "--new.name=" + tableName2.getNameAsString(), "--bulkload", tableName1.getNameAsString());
      assertFalse(runCopy(UTIL1.getConfiguration(), args));
    } finally {
      UTIL1.deleteTable(tableName1);
      UTIL2.deleteTable(tableName2);
    }
  }

  @Test
  public void testSnapshotNotSupported() throws Exception {
    TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    String snapshot = tableName1.getNameAsString() + "_snapshot";
    try (Table t1 = UTIL1.createTable(tableName1, FAMILY_A);
      Table t2 = UTIL2.createTable(tableName2, FAMILY_A)) {
      UTIL1.getAdmin().snapshot(snapshot, tableName1);
      String[] args = ArrayUtils.addAll(getPeerClusterOptions(),
        "--new.name=" + tableName2.getNameAsString(), "--snapshot", snapshot);
      assertFalse(runCopy(UTIL1.getConfiguration(), args));
    } finally {
      UTIL1.getAdmin().deleteSnapshot(snapshot);
      UTIL1.deleteTable(tableName1);
      UTIL2.deleteTable(tableName2);
    }
  }
}
