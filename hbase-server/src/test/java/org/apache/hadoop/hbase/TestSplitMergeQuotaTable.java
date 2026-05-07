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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Test that we can split and merge the quota table given the presence of various configuration
 * settings.
 */
@Tag(MiscTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: configMap = {0}")
public class TestSplitMergeQuotaTable {

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitMergeQuotaTable.class);

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(ImmutableMap.of(QuotaUtil.QUOTA_CONF_KEY, "false")),
      Arguments.of(ImmutableMap.of(QuotaUtil.QUOTA_CONF_KEY, "true")));
  }

  private final HBaseTestingUtil util = new HBaseTestingUtil();

  private final TableName tableName = QuotaUtil.QUOTA_TABLE_NAME;

  private Map<String, String> configMap;

  public TestSplitMergeQuotaTable(Map<String, String> configMap) {
    this.configMap = configMap;
  }

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    configMap.forEach(conf::set);
    util.startMiniCluster();
    if (!util.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      util.getHBaseCluster().getMaster().createSystemTable(QuotaUtil.QUOTA_TABLE_DESC);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @TestTemplate
  public void testSplitMerge() throws Exception {
    util.waitTableAvailable(tableName, 30_000);
    AsyncAdmin admin = util.getAsyncConnection().getAdmin();
    admin.split(tableName, Bytes.toBytes(0x10)).get(30, TimeUnit.SECONDS);
    util.waitFor(30_000, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        // can potentially observe the parent and both children via this interface.
        return admin.getRegions(tableName)
          .thenApply(val -> val.stream()
            .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID).toList())
          .get(30, TimeUnit.SECONDS).size() > 1;
      }

      @Override
      public String explainFailure() {
        return "Split has not finished yet";
      }
    });
    util.waitUntilNoRegionsInTransition();
    List<RegionInfo> regionInfos = admin.getRegions(tableName)
      .thenApply(val -> val.stream()
        .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID).toList())
      .get(30, TimeUnit.SECONDS);
    assertEquals(2, regionInfos.size());
    LOG.info("{}", regionInfos);
    admin.mergeRegions(regionInfos.stream().map(RegionInfo::getRegionName).toList(), false).get(30,
      TimeUnit.SECONDS);
    util.waitFor(30000, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        // can potentially observe the parent and both children via this interface.
        return admin.getRegions(tableName)
          .thenApply(val -> val.stream()
            .filter(info -> info.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID).toList())
          .get(30, TimeUnit.SECONDS).size() == 1;
      }

      @Override
      public String explainFailure() {
        return "Merge has not finished yet";
      }
    });
    assertEquals(1, admin.getRegions(tableName).get(30, TimeUnit.SECONDS).size());
  }
}
