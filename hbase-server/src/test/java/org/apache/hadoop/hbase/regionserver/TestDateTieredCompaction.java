/**
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
package org.apache.hadoop.hbase.regionserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionPolicy;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDateTieredCompaction extends TestCompactionPolicy {
  ArrayList<StoreFile> sfCreate(long[] minTimestamps, long[] maxTimestamps, long[] sizes)
      throws IOException {
    ArrayList<Long> ageInDisk = new ArrayList<Long>();
    for (int i = 0; i < sizes.length; i++) {
      ageInDisk.add(0L);
    }

    ArrayList<StoreFile> ret = Lists.newArrayList();
    for (int i = 0; i < sizes.length; i++) {
      MockStoreFile msf =
          new MockStoreFile(TEST_UTIL, TEST_FILE, sizes[i], ageInDisk.get(i), false, i);
      msf.setTimeRangeTracker(new TimeRangeTracker(minTimestamps[i], maxTimestamps[i]));
      ret.add(msf);
    }
    return ret;
  }

  @Override
  protected void config() {
    super.config();

    // Set up policy
    conf.setLong(CompactionConfiguration.MAX_AGE_KEY, 100);
    conf.setLong(CompactionConfiguration.INCOMING_WINDOW_MIN_KEY, 3);
    conf.setLong(CompactionConfiguration.BASE_WINDOW_MILLIS_KEY, 6);
    conf.setInt(CompactionConfiguration.WINDOWS_PER_TIER_KEY, 4);
    conf.set(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
      DateTieredCompactionPolicy.class.getName());

    // Special settings for compaction policy per window
    this.conf.setInt(CompactionConfiguration.MIN_KEY, 2);
    this.conf.setInt(CompactionConfiguration.MAX_KEY, 12);
    this.conf.setFloat(CompactionConfiguration.RATIO_KEY, 1.2F);
  }

  void compactEquals(long now, ArrayList<StoreFile> candidates, long... expected)
      throws IOException {
    Assert.assertTrue(((DateTieredCompactionPolicy) store.storeEngine.getCompactionPolicy())
        .needsCompaction(candidates, ImmutableList.<StoreFile> of(), now));

    List<StoreFile> actual =
        ((DateTieredCompactionPolicy) store.storeEngine.getCompactionPolicy())
            .applyCompactionPolicy(candidates, false, false, now);

    Assert.assertEquals(Arrays.toString(expected), Arrays.toString(getSizes(actual)));
  }

  /**
   * Test for incoming window
   * @throws IOException with error
   */
  @Test
  public void incomingWindow() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 25, 10, 11, 12, 13 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 13, 12, 11, 10);
  }

  /**
   * Not enough files in incoming window
   * @throws IOException with error
   */
  @Test
  public void NotIncomingWindow() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 25, 10, 11 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 25, 24, 23, 22, 21, 20);
  }

  /**
   * Test for file newer than incoming window
   * @throws IOException with error
   */
  @Test
  public void NewerThanIncomingWindow() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 25, 10, 11, 12, 13 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 13, 12, 11, 10);
  }

  /**
   * If there is no T1 window, we don't build 2
   * @throws IOException with error
   */
  @Test
  public void NoT2() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 44, 60, 61, 92, 95, 100 };
    long[] sizes = new long[] { 0, 20, 21, 22, 23, 1 };

    compactEquals(100, sfCreate(minTimestamps, maxTimestamps, sizes), 23, 22);
  }

  @Test
  public void T1() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 44, 60, 61, 96, 100, 104, 120, 124, 143, 145, 157 };
    long[] sizes = new long[] { 0, 50, 51, 40, 41, 42, 30, 31, 32, 2, 1 };

    compactEquals(161, sfCreate(minTimestamps, maxTimestamps, sizes), 32, 31, 30);
  }

  /**
   * Apply exploring logic on non-incoming window
   * @throws IOException with error
   */
  @Test
  public void RatioT0() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 280, 23, 24, 1 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 22, 21, 20);
  }

  /**
   * Also apply ratio-based logic on t2 window
   * @throws IOException with error
   */
  @Test
  public void RatioT2() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 44, 60, 61, 96, 100, 104, 120, 124, 143, 145, 157 };
    long[] sizes = new long[] { 0, 50, 51, 40, 41, 42, 350, 30, 31, 2, 1 };

    compactEquals(161, sfCreate(minTimestamps, maxTimestamps, sizes), 31, 30);
  }

  /**
   * The next compaction call after testTieredCompactionRatioT0 is compacted
   * @throws IOException with error
   */
  @Test
  public void RatioT0Next() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 8, 9, 10, 11, 12 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 22, 280, 23, 24, 1 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 24, 23);
  }

  /**
  * Older than now(161) - maxAge(100)
  * @throws IOException with error
  */
 @Test
 public void olderThanMaxAge() throws IOException {
   long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
   long[] maxTimestamps = new long[] { 44, 60, 61, 96, 100, 104, 105, 106, 113, 145, 157 };
   long[] sizes = new long[] { 0, 50, 51, 40, 41, 42, 33, 30, 31, 2, 1 };

   compactEquals(161, sfCreate(minTimestamps, maxTimestamps, sizes), 31, 30, 33, 42, 41, 40);
 }

  /**
   * Out-of-order data
   * @throws IOException with error
   */
  @Test
  public void OutOfOrder() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 0, 13, 3, 10, 11, 1, 2, 12, 14, 15 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 22, 28, 23, 24, 1 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 1, 24, 23, 28, 22, 34,
      33, 32, 31);
  }
}
