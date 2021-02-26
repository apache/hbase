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
package org.apache.hadoop.hbase.master.normalizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.RateLimiter;

/**
 * Test that configuration changes are propagated to all children.
 */
@Category({ MasterTests.class, SmallTests.class})
public class TestRegionNormalizerManagerConfigurationObserver {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionNormalizerManagerConfigurationObserver.class);

  private static final HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private static final Pattern rateLimitPattern =
    Pattern.compile("RateLimiter\\[stableRate=(?<rate>.+)qps]");

  private Configuration conf;
  private SimpleRegionNormalizer normalizer;
  @Mock private MasterServices masterServices;
  @Mock private RegionNormalizerTracker tracker;
  @Mock private RegionNormalizerChore chore;
  @Mock private RegionNormalizerWorkQueue<TableName> queue;
  private RegionNormalizerWorker worker;
  private ConfigurationManager configurationManager;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    conf = testUtil.getConfiguration();
    normalizer = new SimpleRegionNormalizer();
    worker = new RegionNormalizerWorker(conf, masterServices, normalizer, queue);
    final RegionNormalizerManager normalizerManager =
      new RegionNormalizerManager(tracker, chore, queue, worker);
    configurationManager = new ConfigurationManager();
    configurationManager.registerObserver(normalizerManager);
  }

  @Test
  public void test() {
    assertTrue(normalizer.isMergeEnabled());
    assertEquals(3, normalizer.getMinRegionCount());
    assertEquals(1_000_000L, parseConfiguredRateLimit(worker.getRateLimiter()));

    final Configuration newConf = new Configuration(conf);
    // configs on SimpleRegionNormalizer
    newConf.setBoolean("hbase.normalizer.merge.enabled", false);
    newConf.setInt("hbase.normalizer.min.region.count", 100);
    // config on RegionNormalizerWorker
    newConf.set("hbase.normalizer.throughput.max_bytes_per_sec", "12g");

    configurationManager.notifyAllObservers(newConf);
    assertFalse(normalizer.isMergeEnabled());
    assertEquals(100, normalizer.getMinRegionCount());
    assertEquals(12_884L, parseConfiguredRateLimit(worker.getRateLimiter()));
  }

  /**
   * The {@link RateLimiter} class does not publicly expose its currently configured rate. It does
   * offer this information in the {@link RateLimiter#toString()} method. It's fragile, but parse
   * this value. The alternative would be to track the value explicitly in the worker, and the
   * associated coordination overhead paid at runtime. See the related note on
   * {@link RegionNormalizerWorker#getRateLimiter()}.
   */
  private static long parseConfiguredRateLimit(final RateLimiter rateLimiter) {
    final String val = rateLimiter.toString();
    final Matcher matcher = rateLimitPattern.matcher(val);
    assertTrue(matcher.matches());
    final String parsedRate = matcher.group("rate");
    return (long) Double.parseDouble(parsedRate);
  }
}
