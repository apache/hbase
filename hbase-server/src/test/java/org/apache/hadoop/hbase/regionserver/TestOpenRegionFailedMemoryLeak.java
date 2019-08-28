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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestOpenRegionFailedMemoryLeak {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOpenRegionFailedMemoryLeak.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestOpenRegionFailedMemoryLeak.class);

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @AfterClass
  public static void tearDown() throws IOException {
    EnvironmentEdgeManagerTestHelper.reset();
    LOG.info("Cleaning test directory: " + TEST_UTIL.getDataTestDir());
    TEST_UTIL.cleanupTestDir();
  }

  // make sure the region is successfully closed when the coprocessor config is wrong
  @Test
  public void testOpenRegionFailedMemoryLeak() throws Exception {
    final ServerName serverName = ServerName.valueOf("testOpenRegionFailed", 100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("testOpenRegionFailed"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam1))
        .setValue("COPROCESSOR$1", "hdfs://test/test.jar|test||").build();

    RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
    ScheduledExecutorService executor =
      CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    for (int i = 0; i < 20; i++) {
      try {
        HRegion.openHRegion(hri, htd, rss.getWAL(hri), TEST_UTIL.getConfiguration(), rss, null);
        fail("Should fail otherwise the test will be useless");
      } catch (Throwable t) {
        LOG.info("Expected exception, continue", t);
      }
    }
    TimeUnit.SECONDS.sleep(MetricsRegionWrapperImpl.PERIOD);
    Field[] fields = ThreadPoolExecutor.class.getDeclaredFields();
    boolean found = false;
    for (Field field : fields) {
      if (field.getName().equals("workQueue")) {
        field.setAccessible(true);
        BlockingQueue<Runnable> workQueue = (BlockingQueue<Runnable>) field.get(executor);
        // there are still two task not cancel, can not cause to memory lack
        Assert.assertTrue("ScheduledExecutor#workQueue should equals 2, now is " +
          workQueue.size() + ", please check region is close", 2 == workQueue.size());
        found = true;
      }
    }
    Assert.assertTrue("can not find workQueue, test failed", found);
  }

}