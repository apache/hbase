/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.compactionserver;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.compaction.CompactionOffloadManager;
import org.apache.hadoop.hbase.regionserver.throttle.PressureAwareCompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.TestCompactionWithThroughputControllerBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestCompactionServerWithThroughputController
    extends TestCompactionWithThroughputControllerBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionServerWithThroughputController.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCompactionServerWithThroughputController.class);

  protected void setThroughputLimitConf(long throughputLimit) {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(CompactionOffloadManager.COMPACTION_SERVER_MAX_THROUGHPUT_HIGHER_BOUND,
      throughputLimit);
    conf.setLong(CompactionOffloadManager.COMPACTION_SERVER_MAX_THROUGHPUT_LOWER_BOUND,
      throughputLimit);
    conf.setLong(
      PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
      throughputLimit);
    conf.setLong(
      PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
      throughputLimit);
  }

  protected void startMiniCluster() throws Exception {
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numCompactionServers(1).build());
    TEST_UTIL.getAdmin().switchCompactionOffload(true);
  }

  protected void shutdownMiniCluster() throws Exception {
    HCompactionServer compactionServer =
        TEST_UTIL.getMiniHBaseCluster().getCompactionServerThreads().get(0).getCompactionServer();
    Assert.assertTrue(compactionServer.requestCount.sum() > 0);
    TEST_UTIL.shutdownMiniCluster();
  }

  protected Table createTable() throws IOException {
    TableDescriptor tableDescriptor =
        TableDescriptorBuilder.newBuilder(tableName).setCompactionOffloadEnabled(true).build();
    return TEST_UTIL.createTable(tableDescriptor, Bytes.toByteArrays(family),
      TEST_UTIL.getConfiguration());
  }

  @Test
  public void testCompaction() throws Exception {
    long limitTime = testCompactionWithThroughputLimit();
    long noLimitTime = testCompactionWithoutThroughputLimit();
    LOG.info("With 1M/s limit, compaction use " + limitTime + "ms; without limit, compaction use "
        + noLimitTime + "ms");
    // usually the throughput of a compaction without limitation is about 40MB/sec at least, so this
    // is a very weak assumption.
    assertTrue(limitTime > noLimitTime * 2);
  }
}
