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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, SmallTests.class})
public class TestExploringCompactionPolicy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestExploringCompactionPolicy.class);

  @Rule
  public ExpectedException error = ExpectedException.none();

  private static final Logger LOG = LoggerFactory.getLogger(TestExploringCompactionPolicy.class);

  private static CompactionConfiguration comConf;

  static class Result {
    List<Integer> selection;
    int start;
    int iter;
    int opts;
    int optsInRatio;

    Result(List<Integer> selection, int start, int iter, int opts, int optsInRatio) {
      this.selection = selection;
      this.start = start;
      this.iter = iter;
      this.opts = opts;
      this.optsInRatio = optsInRatio;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Result)) {
        return false;
      }
      Result result = (Result) o;
      return start == result.start && iter == result.iter && opts == result.opts
        && optsInRatio == result.optsInRatio && selection.equals(result.selection);
    }

    @Override
    public int hashCode() {
      return Objects.hash(selection, start, iter, opts, optsInRatio);
    }

    @Override
    public String toString() {
      return "Result{" + "selection=" + selection + ", start=" + start + ", iter=" + iter
        + ", opts=" + opts + ", optsInRatio=" + optsInRatio + "}";
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    comConf = Mockito.mock(CompactionConfiguration.class);
    Mockito.doAnswer(invocation -> 1.2).when(comConf).getCompactionRatio();
    Mockito.doAnswer(invocation -> Long.MAX_VALUE).when(comConf).getMaxCompactSize();
    Mockito.doAnswer(invocation -> 128L).when(comConf).getMinCompactSize();
  }

  private Result applyCompactionPolicyBeforeModified(List<Integer> candidates, boolean mightBeStuck,
                                                     int minFiles, int maxFiles) throws Exception {
    final double currentRatio = comConf.getCompactionRatio();

    List<Integer> bestSelection = new ArrayList<>(0);
    List<Integer> smallest = mightBeStuck ? new ArrayList<>(0) : null;
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;

    int opts = 0, optsInRatio = 0, bestStart = -1, iter = 0;
    for (int start = 0; start < candidates.size(); start++) {
      for (int currentEnd = start + minFiles - 1; currentEnd < candidates.size(); currentEnd++) {
        List<Integer> potentialMatchFiles = candidates.subList(start, currentEnd + 1);

        iter++;
        if (potentialMatchFiles.size() < minFiles) {
          continue;
        }
        if (potentialMatchFiles.size() > maxFiles) {
          continue;
        }

        long size = getTotalStoreSize(potentialMatchFiles);

        if (mightBeStuck && size < smallestSize) {
          smallest = potentialMatchFiles;
          smallestSize = size;
        }

        if (size > comConf.getMaxCompactSize()) {
          continue;
        }

        ++opts;
        if (size >= comConf.getMinCompactSize()
          && !filesInRatio(potentialMatchFiles, currentRatio)) {
          continue;
        }

        ++optsInRatio;
        if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
          bestStart = start;
        }
      }
    }
    if (bestSelection.isEmpty() && mightBeStuck) {
      return new Result(new ArrayList<>(smallest), bestStart, iter, opts, optsInRatio);
    }
    return new Result(new ArrayList<>(bestSelection), bestStart, iter, opts, optsInRatio);
  }

  private Result applyCompactionPolicyAfterModified(List<Integer> candidates, boolean mightBeStuck,
                                                    int minFiles, int maxFiles) throws Exception {
    final double currentRatio = comConf.getCompactionRatio();

    List<Integer> bestSelection = new ArrayList<>(0);
    List<Integer> smallest = mightBeStuck ? new ArrayList<>(0) : null;
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;
    int bestNum = minFiles;

    int opts = 0, optsInRatio = 0, bestStart = -1, iter = 0;
    for (int start = 0; start < candidates.size(); start++) {
      int window = mightBeStuck ? minFiles : bestNum;
      for (int currentEnd = start + window - 1; currentEnd < candidates.size(); currentEnd++) {
        List<Integer> potentialMatchFiles = candidates.subList(start, currentEnd + 1);

        iter++;
        if (potentialMatchFiles.size() > maxFiles) {
          break;
        }

        long size = getTotalStoreSize(potentialMatchFiles);

        if (mightBeStuck && size < smallestSize) {
          smallest = potentialMatchFiles;
          smallestSize = size;
        }

        if (size > comConf.getMaxCompactSize()) {
          break;
        }

        ++opts;
        if (size >= comConf.getMinCompactSize()
          && !filesInRatio(potentialMatchFiles, currentRatio)) {
          continue;
        }

        ++optsInRatio;
        if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
          bestStart = start;
          bestNum = potentialMatchFiles.size();
        }
      }
    }
    if (bestSelection.isEmpty() && mightBeStuck) {
      return new Result(new ArrayList<>(smallest), bestStart, iter, opts, optsInRatio);
    }
    return new Result(new ArrayList<>(bestSelection), bestStart, iter, opts, optsInRatio);
  }

  private boolean isBetterSelection(List<Integer> bestSelection, long bestSize,
    List<Integer> selection, long size, boolean mightBeStuck) {
    if (mightBeStuck && bestSize > 0 && size > 0) {
      final double REPLACE_IF_BETTER_BY = 1.05;
      double thresholdQuality = ((double)bestSelection.size() / bestSize) * REPLACE_IF_BETTER_BY;
      return thresholdQuality < ((double)selection.size() / size);
    }
    return selection.size() > bestSelection.size()
       || (selection.size() == bestSelection.size() && size < bestSize);
  }

  private boolean filesInRatio(List<Integer> files, double currentRatio) {
    if (files.size() < 2) {
      return true;
    }

    long totalFileSize = getTotalStoreSize(files);

    for (Integer file : files) {
      long sumAllOtherFileSizes = totalFileSize - file;

      if (file > sumAllOtherFileSizes * currentRatio) {
        return false;
      }
    }
    return true;
  }

  private int getTotalStoreSize(List<Integer> files) {
    return files.stream().mapToInt(Integer::intValue).sum();
  }

  private void runTest(List<Integer> candidates, int minToCompact,
                       int maxToCompact) throws Exception {
    Result stuckBeforeModified = applyCompactionPolicyBeforeModified(candidates, true,
                                 minToCompact, maxToCompact);
    Result stuckAfterModified = applyCompactionPolicyAfterModified(candidates, true,
                                minToCompact, maxToCompact);
    LOG.info("applyCompactionPolicyBeforeModified when stuck: {}", stuckBeforeModified);
    LOG.info("applyCompactionPolicyAfterModified when stuck: {}", stuckAfterModified);
    Assert.assertEquals(stuckAfterModified.selection, stuckBeforeModified.selection);
    Assert.assertTrue(stuckAfterModified.iter < stuckBeforeModified.iter);
    Assert.assertEquals(stuckAfterModified.opts, stuckBeforeModified.opts);
    Assert.assertEquals(stuckAfterModified.optsInRatio, stuckBeforeModified.optsInRatio);

    Result unStuckBeforeModified = applyCompactionPolicyBeforeModified(candidates, false,
                                   minToCompact, maxToCompact);
    Result unStuckAfterModified = applyCompactionPolicyAfterModified(candidates, false,
                                  minToCompact, maxToCompact);
    LOG.info("applyCompactionPolicyBeforeModified when unstuck: {}", unStuckBeforeModified);
    LOG.info("applyCompactionPolicyAfterModified when unstuck: {}", unStuckAfterModified);
    Assert.assertEquals(unStuckBeforeModified.selection, unStuckAfterModified.selection);
    Assert.assertTrue(unStuckAfterModified.iter < unStuckBeforeModified.iter);
    Assert.assertTrue(unStuckAfterModified.opts < unStuckBeforeModified.opts);
    Assert.assertTrue(unStuckAfterModified.optsInRatio < unStuckBeforeModified.optsInRatio);
  }

  private void runRandomCandidatesFilesSelectionTest(int numOfFiles, int minToCompact,
                                                     int maxToCompact) throws Exception {
    List<Integer> candidates = new ArrayList<>(numOfFiles);
    Random rand = new Random();
    for (int i = 0; i < numOfFiles; i++) {
      candidates.add(rand.nextInt(10000));
    }
    runTest(candidates, minToCompact, maxToCompact);
  }

  @Test
  public void testApplyCompactionPolicy() throws Exception {
    runTest(Arrays.asList(100, 80, 80, 80, 80, 80), 2, 3);
    runTest(Arrays.asList(100, 20, 80, 80, 80, 20, 80, 80, 20, 80, 20), 2, 8);
    runRandomCandidatesFilesSelectionTest(20, 2, 10);
    runRandomCandidatesFilesSelectionTest(50, 4, 20);
    runRandomCandidatesFilesSelectionTest(100, 4, 20);
    runRandomCandidatesFilesSelectionTest(500, 2, 50);
    runRandomCandidatesFilesSelectionTest(1000, 2, 100);
    runRandomCandidatesFilesSelectionTest(10000, 10, 100);
  }

}
