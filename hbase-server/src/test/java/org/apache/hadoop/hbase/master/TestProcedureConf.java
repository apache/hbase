/**
 *
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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestProcedureConf {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testProcedureConfEnable() throws Exception {
    TableName tableName = TableName.valueOf("testProcedureConfEnable");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    MiniHBaseCluster cluster = null;

    try {
      TEST_UTIL.startMiniCluster();
      cluster = TEST_UTIL.getHBaseCluster();
      HMaster m = cluster.getMaster();
      long procid = m.createTable(htd, null, HConstants.NO_NONCE, HConstants.NO_NONCE);
      assertTrue(procid > 0);
    } finally {
      if (cluster != null) {
        TEST_UTIL.shutdownMiniCluster();
      }
    }
  }

  @Test
  public void testProcedureConfDisable() throws Exception {
    TableName tableName = TableName.valueOf("testProcedureConfDisable");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    MiniHBaseCluster cluster = null;

    try {
      TEST_UTIL.getConfiguration().set("hbase.master.procedure.tableddl", "disabled");
      TEST_UTIL.startMiniCluster();
      cluster = TEST_UTIL.getHBaseCluster();
      HMaster m = cluster.getMaster();
      long procid = m.createTable(htd, null, HConstants.NO_NONCE, HConstants.NO_NONCE);
      assertTrue(procid < 0);
    } finally {
      if (cluster != null) {
        TEST_UTIL.shutdownMiniCluster();
      }
    }
  }

  @Test
  public void testProcedureConfUnused() throws Exception {
    TableName tableName = TableName.valueOf("testProcedureConfUnused");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    MiniHBaseCluster cluster = null;

    try {
      TEST_UTIL.getConfiguration().set("hbase.master.procedure.tableddl", "unused");
      TEST_UTIL.startMiniCluster();
      cluster = TEST_UTIL.getHBaseCluster();
      HMaster m = cluster.getMaster();
      long procid = m.createTable(htd, null, HConstants.NO_NONCE, HConstants.NO_NONCE);
      assertTrue(procid < 0);
    } finally {
      if (cluster != null) {
        TEST_UTIL.shutdownMiniCluster();
      }
    }
  }
}
