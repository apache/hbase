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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionRequest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

public class AbstractTestDateTieredCompactionPolicy extends TestCompactionPolicy {

  protected ArrayList<HStoreFile> sfCreate(long[] minTimestamps, long[] maxTimestamps, long[] sizes)
      throws IOException {
    ManualEnvironmentEdge timeMachine = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(timeMachine);
    // Has to be > 0 and < now.
    timeMachine.setValue(1);
    ArrayList<Long> ageInDisk = new ArrayList<>();
    for (int i = 0; i < sizes.length; i++) {
      ageInDisk.add(0L);
    }

    ArrayList<HStoreFile> ret = Lists.newArrayList();
    for (int i = 0; i < sizes.length; i++) {
      MockHStoreFile msf =
          new MockHStoreFile(TEST_UTIL, TEST_FILE, sizes[i], ageInDisk.get(i), false, i);
      msf.setTimeRangeTracker(TimeRangeTracker.create(TimeRangeTracker.Type.SYNC, minTimestamps[i], maxTimestamps[i]));
      ret.add(msf);
    }
    return ret;
  }

  protected void compactEquals(long now, ArrayList<HStoreFile> candidates, long[] expectedFileSizes,
      long[] expectedBoundaries, boolean isMajor, boolean toCompact) throws IOException {
    DateTieredCompactionRequest request = getRequest(now, candidates, isMajor, toCompact);
    List<HStoreFile> actual = Lists.newArrayList(request.getFiles());
    assertEquals(Arrays.toString(expectedFileSizes), Arrays.toString(getSizes(actual)));
    assertEquals(Arrays.toString(expectedBoundaries),
      Arrays.toString(request.getBoundaries().toArray()));
  }

  private DateTieredCompactionRequest getRequest(long now, ArrayList<HStoreFile> candidates,
      boolean isMajor, boolean toCompact) throws IOException {
    ManualEnvironmentEdge timeMachine = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(timeMachine);
    timeMachine.setValue(now);
    DateTieredCompactionRequest request;
    DateTieredCompactionPolicy policy =
      (DateTieredCompactionPolicy) store.storeEngine.getCompactionPolicy();
    if (isMajor) {
      for (HStoreFile file : candidates) {
        ((MockHStoreFile) file).setIsMajor(true);
      }
      assertEquals(toCompact, policy.shouldPerformMajorCompaction(candidates));
      request = (DateTieredCompactionRequest) policy.selectMajorCompaction(candidates);
    } else {
      assertEquals(toCompact, policy.needsCompaction(candidates, ImmutableList.of()));
      request =
        (DateTieredCompactionRequest) policy.selectMinorCompaction(candidates, false, false);
    }
    return request;
  }

  protected void compactEqualsStoragePolicy(long now, ArrayList<HStoreFile> candidates,
      Map<Long, String> expectedBoundariesPolicies, boolean isMajor, boolean toCompact)
      throws IOException {
    DateTieredCompactionRequest request = getRequest(now, candidates, isMajor, toCompact);
    Map<Long, String> boundariesPolicies = request.getBoundariesPolicies();
    for (Map.Entry<Long, String> entry : expectedBoundariesPolicies.entrySet()) {
      assertEquals(entry.getValue(), boundariesPolicies.get(entry.getKey()));
    }
  }
}
