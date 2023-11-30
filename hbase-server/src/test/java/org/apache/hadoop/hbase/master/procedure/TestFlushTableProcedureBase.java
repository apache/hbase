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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class TestFlushTableProcedureBase {

  protected static HBaseTestingUtil TEST_UTIL;

  protected TableName TABLE_NAME;
  protected byte[] FAMILY1;
  protected byte[] FAMILY2;
  protected byte[] FAMILY3;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    addConfiguration(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
    TABLE_NAME = TableName.valueOf(Bytes.toBytes("TestFlushTable"));
    FAMILY1 = Bytes.toBytes("cf1");
    FAMILY2 = Bytes.toBytes("cf2");
    FAMILY3 = Bytes.toBytes("cf3");
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table =
      TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY1, FAMILY2, FAMILY3 }, splitKeys);
    TEST_UTIL.loadTable(table, FAMILY1, false);
    TEST_UTIL.loadTable(table, FAMILY2, false);
    TEST_UTIL.loadTable(table, FAMILY3, false);
  }

  protected void addConfiguration(Configuration config) {
    // delay dispatch so that we can do something, for example kill a target server
    config.setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 10000);
    config.setInt(RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, 128);
  }

  protected void assertTableMemStoreNotEmpty() {
    long totalSize = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).stream()
      .mapToLong(HRegion::getMemStoreDataSize).sum();
    Assert.assertTrue(totalSize > 0);
  }

  protected void assertTableMemStoreEmpty() {
    long totalSize = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).stream()
      .mapToLong(HRegion::getMemStoreDataSize).sum();
    Assert.assertEquals(0, totalSize);
  }

  protected void assertColumnFamilyMemStoreNotEmpty(byte[] columnFamily) {
    long totalSize = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).stream()
      .mapToLong(r -> r.getStore(columnFamily).getMemStoreSize().getDataSize()).sum();
    Assert.assertTrue(totalSize > 0);
  }

  protected void assertColumnFamilyMemStoreEmpty(byte[] columnFamily) {
    long totalSize = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).stream()
      .mapToLong(r -> r.getStore(columnFamily).getMemStoreSize().getDataSize()).sum();
    Assert.assertEquals(0, totalSize);
  }

  @After
  public void teardown() throws Exception {
    if (TEST_UTIL.getHBaseCluster().getMaster() != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
        TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor(), false);
    }
    TEST_UTIL.shutdownMiniCluster();
  }
}
