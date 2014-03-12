/*
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestThriftMultiRSScenario {
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final int SLAVES = 3;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests a simple scenario where we can run unit tests with 3 slaves.
   * @throws IOException
   */
  @Test
  public void testMultiRSScenario() throws IOException {
    byte[] tableName = Bytes.toBytes("testMultiRSScenario");
    byte[] family = Bytes.toBytes("family");
    byte[] row = Bytes.toBytes("row");
    HTable table = TEST_UTIL.createTable(tableName, family);
    Put p = new Put(row);
    p.add(family, null, row);
    table.put(p);

    Get g = new Get(row);
    Result r = table.get(g);

    assertTrue(Bytes.equals(r.getValue(family, null), row));
  }

}
