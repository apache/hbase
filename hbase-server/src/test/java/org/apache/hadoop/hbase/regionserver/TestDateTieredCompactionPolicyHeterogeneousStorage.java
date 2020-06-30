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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.ExponentialCompactionWindowFactory;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestDateTieredCompactionPolicyHeterogeneousStorage
    extends AbstractTestDateTieredCompactionPolicy {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDateTieredCompactionPolicyHeterogeneousStorage.class);
  public static final String HOT_WINDOW_SP = "ALL_SSD";
  public static final String WARM_WINDOW_SP = "ONE_SSD";
  public static final String COLD_WINDOW_SP = "HOT";

  @Override
  protected void config() {
    super.config();

    // Set up policy
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY,
      "org.apache.hadoop.hbase.regionserver.DateTieredStoreEngine");
    conf.setLong(CompactionConfiguration.DATE_TIERED_MAX_AGE_MILLIS_KEY, 100);
    conf.setLong(CompactionConfiguration.DATE_TIERED_INCOMING_WINDOW_MIN_KEY, 3);
    conf.setLong(ExponentialCompactionWindowFactory.BASE_WINDOW_MILLIS_KEY, 6);
    conf.setInt(ExponentialCompactionWindowFactory.WINDOWS_PER_TIER_KEY, 4);
    conf.setBoolean(CompactionConfiguration.DATE_TIERED_SINGLE_OUTPUT_FOR_MINOR_COMPACTION_KEY,
      false);

    // Special settings for compaction policy per window
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 2);
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 12);
    this.conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.2F);

    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 20);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 5);

    // Set Storage Policy for different type window
    conf.setBoolean(CompactionConfiguration.DATE_TIERED_STORAGE_POLICY_ENABLE_KEY, true);
    conf.setLong(CompactionConfiguration.DATE_TIERED_HOT_WINDOW_AGE_MILLIS_KEY, 6);
    conf.set(CompactionConfiguration.DATE_TIERED_HOT_WINDOW_STORAGE_POLICY_KEY, HOT_WINDOW_SP);
    conf.setLong(CompactionConfiguration.DATE_TIERED_WARM_WINDOW_AGE_MILLIS_KEY, 12);
    conf.set(CompactionConfiguration.DATE_TIERED_WARM_WINDOW_STORAGE_POLICY_KEY, WARM_WINDOW_SP);
    conf.set(CompactionConfiguration.DATE_TIERED_COLD_WINDOW_STORAGE_POLICY_KEY, COLD_WINDOW_SP);
  }

  /**
   * Test for minor compaction of incoming window.
   * Incoming window start ts >= now - hot age. So it is HOT window, will use HOT_WINDOW_SP.
   * @throws IOException with error
   */
  @Test
  public void testIncomingWindowHot() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 25, 10, 11, 12, 13 };
    Map<Long, String> expected = new HashMap<>();
    // expected DateTieredCompactionRequest boundaries = { Long.MIN_VALUE, 12 }
    // test whether DateTieredCompactionRequest boundariesPolicies matches expected
    expected.put(12L, HOT_WINDOW_SP);
    compactEqualsStoragePolicy(16, sfCreate(minTimestamps, maxTimestamps, sizes),
      expected, false, true);
  }

  /**
   * Test for not incoming window.
   * now - hot age > window start >= now - warm age,
   * so this window and is WARM window, will use WARM_WINDOW_SP
   * @throws IOException with error
   */
  @Test
  public void testNotIncomingWindowWarm() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 25, 10, 11 };
    Map<Long, String> expected = new HashMap<>();
    // expected DateTieredCompactionRequest boundaries = { Long.MIN_VALUE, 6 }
    expected.put(6L, WARM_WINDOW_SP);
    compactEqualsStoragePolicy(16, sfCreate(minTimestamps, maxTimestamps, sizes),
      expected, false, true);
  }

  /**
   * Test for not incoming window.
   * this window start ts >= ow - hot age,
   * So this incoming window and is HOT window. Use HOT_WINDOW_SP
   * @throws IOException with error
   */
  @Test
  public void testNotIncomingWindowAndIsHot() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 25, 10, 11 };
    Map<Long, String> expected = new HashMap<>();
    // expected DateTieredCompactionRequest boundaries = { Long.MIN_VALUE, 6 }
    expected.put(6L, HOT_WINDOW_SP);
    compactEqualsStoragePolicy(12, sfCreate(minTimestamps, maxTimestamps, sizes),
      expected, false, true);
  }

  /**
   * Test for not incoming window.
   * COLD window start timestamp < now - warm age, so use COLD_WINDOW_SP
   * @throws IOException with error
   */
  @Test
  public void testColdWindow() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 25, 10 };
    Map<Long, String> expected = new HashMap<>();
    // expected DateTieredCompactionRequest boundaries = { Long.MIN_VALUE, 6 }
    expected.put(6L, COLD_WINDOW_SP);
    compactEqualsStoragePolicy(22, sfCreate(minTimestamps, maxTimestamps, sizes),
      expected, false, true);
  }

  /**
   * Test for not incoming window. but not all hfiles will be selected to compact.
   * Apply exploring logic on non-incoming window. More than one hfile left in this window.
   * this means minor compact single out is true. boundaries only contains Long.MIN_VALUE
   * @throws IOException with error
   */
  @Test
  public void testRatioT0() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 280, 23, 24, 1 };
    Map<Long, String> expected = new HashMap<>();
    // window start = 6, expected DateTieredCompactionRequest boundaries = { Long.MIN_VALUE }
    expected.put(Long.MIN_VALUE, WARM_WINDOW_SP);
    compactEqualsStoragePolicy(16, sfCreate(minTimestamps, maxTimestamps, sizes),
      expected, false, true);
  }

  /**
   * Test for Major compaction. It will compact all files and create multi output files
   * with different window storage policy.
   * @throws IOException with error
   */
  @Test
  public void testMajorCompation() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 44, 60, 61, 96, 100, 104, 105, 106, 113, 145, 157 };
    long[] sizes = new long[] { 0, 50, 51, 40, 41, 42, 33, 30, 31, 2, 1 };
    Map<Long, String> expected = new HashMap<>();
    expected.put(Long.MIN_VALUE, COLD_WINDOW_SP);
    expected.put(24L, COLD_WINDOW_SP);
    expected.put(48L, COLD_WINDOW_SP);
    expected.put(72L, COLD_WINDOW_SP);
    expected.put(96L, COLD_WINDOW_SP);
    expected.put(120L, COLD_WINDOW_SP);
    expected.put(144L, COLD_WINDOW_SP);
    expected.put(150L, WARM_WINDOW_SP);
    expected.put(156L, HOT_WINDOW_SP);
    compactEquals(161, sfCreate(minTimestamps, maxTimestamps, sizes),
      new long[] { 0, 50, 51, 40, 41, 42, 33, 30, 31, 2, 1 },
      new long[] { Long.MIN_VALUE, 24, 48, 72, 96, 120, 144, 150, 156 }, true, true);
    compactEqualsStoragePolicy(161, sfCreate(minTimestamps, maxTimestamps, sizes),
      expected,true, true);
  }
}
