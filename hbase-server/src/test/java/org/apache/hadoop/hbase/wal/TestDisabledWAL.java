/**
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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestDisabledWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDisabledWAL.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestDisabledWAL.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Table table;
  private TableName tableName;
  private byte[] fam = Bytes.toBytes("f1");

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.regionserver.hlog.enabled", false);
    try {
      TEST_UTIL.startMiniCluster();
    } catch (RuntimeException | IOException e) {
      LOG.error("Master failed to start.", e);
      fail("Failed to start cluster. Reason being: " + e.getCause().getMessage());
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    tableName = TableName.valueOf(name.getMethodName().replaceAll("[^a-zA-Z0-9]", "_"));
    LOG.info("Creating table " + tableName);
    table = TEST_UTIL.createTable(tableName, fam);
  }

  @After
  public void cleanup() throws Exception {
    LOG.info("Deleting table " + tableName);
    TEST_UTIL.deleteTable(tableName);
  }

  @Test
  public void testDisabledWAL() throws Exception {
    LOG.info("Writing data to table " + tableName);
    Put p = new Put(Bytes.toBytes("row"));
    p.addColumn(fam, Bytes.toBytes("qual"), Bytes.toBytes("val"));
    table.put(p);

    LOG.info("Flushing table " + tableName);
    TEST_UTIL.flush(tableName);

    LOG.info("Getting data from table " + tableName);
    Get get = new Get(Bytes.toBytes("row"));

    Result result = table.get(get);
    assertNotNull(result.getValue(fam, Bytes.toBytes("qual")));
  }
}
