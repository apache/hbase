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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, CoprocessorTests.class })
public class TestRegionCoprocessorQuotaUsage {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionCoprocessorQuotaUsage.class);

  private static HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static TableName TABLE_NAME = TableName.valueOf("TestRegionCoprocessorQuotaUsage");
  private static byte[] CF = Bytes.toBytes("CF");
  private static byte[] CQ = Bytes.toBytes("CQ");
  private static Connection CONN;
  private static Table TABLE;
  private static AtomicBoolean THROTTLING_OCCURRED = new AtomicBoolean(false);

  public static class MyRegionObserver implements RegionObserver {
    @Override
    public void preGetOp(ObserverContext<? extends RegionCoprocessorEnvironment> c, Get get,
      List<Cell> result) throws IOException {

      // For the purposes of this test, we only need to catch a throttle happening once, then
      // let future requests pass through so we don't make this test take any longer than necessary
      if (!THROTTLING_OCCURRED.get()) {
        try {
          c.getEnvironment().checkBatchQuota(c.getEnvironment().getRegion(),
            OperationQuota.OperationType.GET);
        } catch (RpcThrottlingException e) {
          THROTTLING_OCCURRED.set(true);
          throw e;
        }
      }
    }
  }

  public static class MyCoprocessor implements RegionCoprocessor {
    RegionObserver observer = new MyRegionObserver();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(observer);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean("hbase.quota.enabled", true);
    conf.setInt("hbase.quota.default.user.machine.read.num", 2);
    conf.set("hbase.quota.rate.limiter", "org.apache.hadoop.hbase.quotas.FixedIntervalRateLimiter");
    conf.set("hbase.quota.rate.limiter.refill.interval.ms", "300000");
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, MyCoprocessor.class.getName());
    UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    UTIL.createTable(TABLE_NAME, CF, splitKeys);
    CONN = UTIL.getConnection();
    TABLE = CONN.getTable(TABLE_NAME);
    TABLE.put(new Put(Bytes.toBytes(String.format("%d", 0))).addColumn(CF, CQ, Bytes.toBytes(0L)));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGet() throws InterruptedException, ExecutionException, IOException {
    // Hit the table 5 times which ought to be enough to make a throttle happen
    for (int i = 0; i < 5; i++) {
      TABLE.get(new Get(Bytes.toBytes("000")));
    }
    assertTrue("Throttling did not happen as expected", THROTTLING_OCCURRED.get());
  }
}
