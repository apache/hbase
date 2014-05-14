/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.thrift.swift;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A simple test which just brings up the MiniHBase cluster where the master ->
 * regionserver communication and also the client -> regionserver happens
 * through thrift. This test just does a simple put and get.
 *
 */
@Category(MediumTests.class)
public class TestMasterToRSUseThrift {
  private final Configuration conf = HBaseConfiguration.create();
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
  private final byte[] tableName = Bytes.toBytes("testMasterToRSUseThrift");
  private final byte[] family = Bytes.toBytes("family");
  private final byte[] row = Bytes.toBytes("row");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(3);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMasterToRSUseThrift() throws IOException {
    HTable table = TEST_UTIL.createTable(tableName, family);
    Put put = new Put(row);
    put.add(family, null, row);
    table.put(put);
    table.flushCommits();
    Result r = table.get(new Get(row));
    assertTrue(Bytes.equals(r.getValue(family, null), row));
  }
}
