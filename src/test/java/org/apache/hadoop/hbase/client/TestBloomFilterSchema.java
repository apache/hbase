/*
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestBloomFilterSchema {

  private static final Log LOG = LogFactory.getLog(TestBloomFilterSchema.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int NUM_SLAVES = 3;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    TEST_UTIL.startMiniCluster(NUM_SLAVES);
  }

  @Test
  public void testBloomFilterInSchema() throws IOException {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String tableName = "tbl";
    byte[] cf = Bytes.toBytes("cf");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor columnDesc = new HColumnDescriptor(cf);
    int rowKeyPrefix = 64;
    columnDesc.setRowKeyPrefixLengthForBloom(rowKeyPrefix);
    int hfileBucketCount = 4356;
    columnDesc.setHFileHistogramBucketCount(hfileBucketCount);
    int expectedUserRegions = 37;
    columnDesc.setMaxVersions(1);
    desc.addFamily(columnDesc);

    admin.createTable(desc, Bytes.toBytes("aaaa"),
        Bytes.toBytes("zzzz"), expectedUserRegions);

    TEST_UTIL.waitForOnlineRegionsToBeAssigned(expectedUserRegions);

    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    int regionCnt = 0;
    for (HRegionInfo info : table.getRegionsInfo().keySet()) {
      regionCnt++;
      assertTrue(info.getTableDesc().getFamily(cf).getRowPrefixLengthForBloom()
          == rowKeyPrefix);
      assertTrue(info.getTableDesc().getFamily(cf)
          .getHFileHistogramBucketCount() == hfileBucketCount);
    }
    assertTrue(regionCnt == expectedUserRegions);
  }
}
