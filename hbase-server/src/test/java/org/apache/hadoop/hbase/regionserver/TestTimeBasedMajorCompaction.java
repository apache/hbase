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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: compType={0}")
public class TestTimeBasedMajorCompaction extends MajorCompactionTestBase {

  public TestTimeBasedMajorCompaction(String compType) {
    super(compType);
  }

  @TestTemplate
  public void testTimeBasedMajorCompaction() throws Exception {
    // create 2 storefiles and force a major compaction to reset the time
    int delay = 10 * 1000; // 10 sec
    float jitterPct = 0.20f; // 20%
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, delay);
    conf.setFloat("hbase.hregion.majorcompaction.jitter", jitterPct);

    HStore s = ((HStore) r.getStore(COLUMN_FAMILY));
    s.storeEngine.getCompactionPolicy().setConf(conf);
    try {
      createStoreFile(r);
      createStoreFile(r);
      r.compact(true);

      // add one more file & verify that a regular compaction won't work
      createStoreFile(r);
      r.compact(false);
      assertEquals(2, s.getStorefilesCount());

      // ensure that major compaction time is deterministic
      RatioBasedCompactionPolicy c =
        (RatioBasedCompactionPolicy) s.storeEngine.getCompactionPolicy();
      Collection<HStoreFile> storeFiles = s.getStorefiles();
      long mcTime = c.getNextMajorCompactTime(storeFiles);
      for (int i = 0; i < 10; ++i) {
        assertEquals(mcTime, c.getNextMajorCompactTime(storeFiles));
      }

      // ensure that the major compaction time is within the variance
      long jitter = Math.round(delay * jitterPct);
      assertTrue(delay - jitter <= mcTime && mcTime <= delay + jitter);

      // wait until the time-based compaction interval
      Thread.sleep(mcTime);

      // trigger a compaction request and ensure that it's upgraded to major
      r.compact(false);
      assertEquals(1, s.getStorefilesCount());
    } finally {
      // reset the timed compaction settings
      conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 1000 * 60 * 60 * 24);
      conf.setFloat("hbase.hregion.majorcompaction.jitter", 0.20F);
      // run a major to reset the cache
      createStoreFile(r);
      r.compact(true);
      assertEquals(1, s.getStorefilesCount());
    }
  }
}
