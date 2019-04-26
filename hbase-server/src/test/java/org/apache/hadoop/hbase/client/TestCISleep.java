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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ClientTests.class, MediumTests.class })
public class TestCISleep extends AbstractTestCITimeout {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCISleep.class);

  private static Logger LOG = LoggerFactory.getLogger(TestCISleep.class);

  private TableName tableName;

  @Before
  public void setUp() {
    tableName = TableName.valueOf(name.getMethodName());
  }

  /**
   * Test starting from 0 index when RpcRetryingCaller calculate the backoff time.
   */
  @Test
  public void testRpcRetryingCallerSleep() throws Exception {
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAM_NAM))
      .setCoprocessor(CoprocessorDescriptorBuilder.newBuilder(SleepAndFailFirstTime.class.getName())
        .setProperty(SleepAndFailFirstTime.SLEEP_TIME_CONF_KEY, String.valueOf(2000))
        .build())
      .build();
    TEST_UTIL.getAdmin().createTable(htd);

    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_PAUSE, 3000);
    c.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 4000);

    try (Connection conn = ConnectionFactory.createConnection(c)) {
      SleepAndFailFirstTime.ct.set(0);
      try (Table table = conn.getTableBuilder(tableName, null).setOperationTimeout(8000).build()) {
        // Check that it works. Because 2s + 3s * RETRY_BACKOFF[0] + 2s < 8s
        table.get(new Get(FAM_NAM));
      }
      SleepAndFailFirstTime.ct.set(0);
      try (Table table = conn.getTableBuilder(tableName, null).setOperationTimeout(6000).build()) {
        // Will fail this time. After sleep, there are not enough time for second retry
        // Beacuse 2s + 3s + 2s > 6s
        table.get(new Get(FAM_NAM));
        fail("We expect an exception here");
      } catch (RetriesExhaustedException e) {
        LOG.info("We received an exception, as expected ", e);
      }
    }
  }
}
