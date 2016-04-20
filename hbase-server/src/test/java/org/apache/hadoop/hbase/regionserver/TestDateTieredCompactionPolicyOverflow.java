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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.ExponentialCompactionWindowFactory;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestDateTieredCompactionPolicyOverflow extends AbstractTestDateTieredCompactionPolicy {

  @Override
  protected void config() {
    super.config();

    // Set up policy
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY,
      "org.apache.hadoop.hbase.regionserver.DateTieredStoreEngine");
    conf.setLong(CompactionConfiguration.DATE_TIERED_MAX_AGE_MILLIS_KEY, 100);
    conf.setLong(CompactionConfiguration.DATE_TIERED_INCOMING_WINDOW_MIN_KEY, 3);
    conf.setLong(ExponentialCompactionWindowFactory.BASE_WINDOW_MILLIS_KEY, Long.MAX_VALUE / 2);
    conf.setInt(ExponentialCompactionWindowFactory.WINDOWS_PER_TIER_KEY, 2);
    conf.setBoolean(CompactionConfiguration.DATE_TIERED_SINGLE_OUTPUT_FOR_MINOR_COMPACTION_KEY,
      false);

    // Special settings for compaction policy per window
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 2);
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 12);
    this.conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.2F);

    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 20);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 10);
  }

  /**
   * Major compaction with maximum values
   * @throws IOException with error
   */
  @Test
  public void maxValuesForMajor() throws IOException {
    long[] minTimestamps = new long[] { Long.MIN_VALUE, -100 };
    long[] maxTimestamps = new long[] { -8, Long.MAX_VALUE };
    long[] sizes = new long[] { 0, 1 };

    compactEquals(Long.MAX_VALUE, sfCreate(minTimestamps, maxTimestamps, sizes),
      new long[] { 0, 1 }, new long[] { Long.MIN_VALUE, -4611686018427387903L, 0,
          4611686018427387903L, 9223372036854775806L }, true, true);
  }
}
