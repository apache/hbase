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
package org.apache.hadoop.hbase.namequeues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.LogEntry;
import org.apache.hadoop.hbase.client.OnlineLogRecord;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestTooLargeLog {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTooLargeLog.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Admin ADMIN;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY, true);
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.warn.response.size", 100);
    TEST_UTIL.startMiniCluster(1);
    ADMIN = TEST_UTIL.getAdmin();
  }

  /**
   * Tests that we can trigger based on blocks scanned, and also that we properly pass the block
   * bytes scanned value through to the client.
   */
  @Test
  public void testLogLargeBlockBytesScanned() throws IOException, InterruptedException {
    byte[] family = Bytes.toBytes("0");
    Table table = TEST_UTIL.createTable(TableName.valueOf("testLogLargeBlockBytesScanned"), family);
    TEST_UTIL.loadTable(table, family);
    TEST_UTIL.flush(table.getName());

    Set<ServerName> server =
      Collections.singleton(TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName());
    Admin admin = TEST_UTIL.getAdmin();
    admin.clearSlowLogResponses(server);

    Scan scan = new Scan();
    scan.setCaching(1);

    try (ResultScanner scanner = table.getScanner(scan)) {
      scanner.next();
    }

    List<LogEntry> entries =
      admin.getLogEntries(server, "LARGE_LOG", ServerType.REGION_SERVER, 1, Collections.emptyMap());

    assertEquals(1, entries.size());

    OnlineLogRecord record = (OnlineLogRecord) entries.get(0);
    System.out.println(record.toJsonPrettyPrint());

    assertTrue("expected " + record.getBlockBytesScanned() + " to be >= 100",
      record.getBlockBytesScanned() >= 100);
    assertTrue("expected " + record.getResponseSize() + " to be < 100",
      record.getResponseSize() < 100);

  }
}
