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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Basic test of {@link RoundRobinTableInputFormat}; i.e. RRTIF.
 */
@Category({SmallTests.class})
public class TestRoundRobinTableInputFormat {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRoundRobinTableInputFormat.class);

  private static final int SERVERS_COUNT = 5;
  private static final String[] KEYS = {
    "aa", "ab", "ac", "ad", "ae",
    "ba", "bb", "bc", "bd", "be",
    "ca", "cb", "cc", "cd", "ce",
    "da", "db", "dc", "dd", "de",
    "ea", "eb", "ec", "ed", "ee",
    "fa", "fb", "fc", "fd", "fe",
    "ga", "gb", "gc", "gd", "ge",
    "ha", "hb", "hc", "hd", "he",
    "ia", "ib", "ic", "id", "ie",
    "ja", "jb", "jc", "jd", "je", "jf"
  };

  /**
   * Test default behavior.
   */
  @Test
  public void testRoundRobinSplit() throws IOException, InterruptedException {
    final List<InputSplit> splits = createSplits();
    Collections.shuffle(splits);
    List<InputSplit> sortedSplits = new RoundRobinTableInputFormat().roundRobin(splits);
    testDistribution(sortedSplits);
    // Now test that order is preserved even after being passed through the SplitComparator
    // that sorts InputSplit by length as is done up in Hadoop in JobSubmitter.
    List<InputSplit> copy = new ArrayList<>(sortedSplits);
    Arrays.sort(copy.toArray(new InputSplit[0]), new SplitComparator());
    // Assert the sort is retained even after passing through SplitComparator.
    for (int i = 0; i < sortedSplits.size(); i++) {
      TableSplit sortedTs = (TableSplit)sortedSplits.get(i);
      TableSplit copyTs = (TableSplit)copy.get(i);
      assertEquals(sortedTs.getEncodedRegionName(), copyTs.getEncodedRegionName());
    }
  }

  /**
   * @return Splits made out of {@link #KEYS}. Splits are for five Servers. Length is ZERO!
   */
  private List<InputSplit> createSplits() {
    List<InputSplit> splits = new ArrayList<>(KEYS.length - 1);
    for (int i = 0; i < KEYS.length - 1; i++) {
      InputSplit split = new TableSplit(TableName.valueOf("test"), new Scan(),
        Bytes.toBytes(KEYS[i]), Bytes.toBytes(KEYS[i + 1]), String.valueOf(i % SERVERS_COUNT + 1),
        "", 0);
      splits.add(split);
    }
    return splits;
  }

  private void testDistribution(List<InputSplit> list) throws IOException, InterruptedException {
    for (int i = 0; i < KEYS.length/SERVERS_COUNT; i++) {
      int [] counts = new int[SERVERS_COUNT];
      for (int j = i * SERVERS_COUNT; j < i * SERVERS_COUNT + SERVERS_COUNT; j++) {
        counts[Integer.parseInt(list.get(j).getLocations()[0]) - 1]++;
      }
      for (int value : counts) {
        assertEquals(value, 1);
      }
    }
  }

  /**
   * Private comparator copied from private JobSubmmiter Hadoop class...
   * hadoop/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/JobSubmitter.java
   * Used so we can run the sort done up in JobSubmitter here in tests.
   */
  private static class SplitComparator implements Comparator<InputSplit> {
    @Override
    public int compare(InputSplit o1, InputSplit o2) {
      try {
        return Long.compare(o1.getLength(), o2.getLength());
      } catch (IOException|InterruptedException e) {
        throw new RuntimeException("exception in compare", e);
      }
    }
  }

  /**
   * Assert that lengths are descending. RRTIF writes lengths in descending order so any
   * subsequent sort using dump SplitComparator as is done in JobSubmitter up in Hadoop keeps
   * our RRTIF ordering.
   */
  private void assertLengthDescending(List<InputSplit> list)
    throws IOException, InterruptedException {
    long previousLength = Long.MAX_VALUE;
    for (InputSplit is: list) {
      long length = is.getLength();
      assertTrue(previousLength + " " + length, previousLength > length);
      previousLength = length;
    }
  }

  /**
   * Test that configure/unconfigure set and properly undo the HBASE_REGIONSIZECALCULATOR_ENABLE
   * configuration.
   */
  @Test
  public void testConfigureUnconfigure() {
    Configuration configuration = HBaseConfiguration.create();
    RoundRobinTableInputFormat rrtif = new RoundRobinTableInputFormat();
    rrtif.setConf(configuration);
    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(configuration);
    // Assert when done, HBASE_REGIONSIZECALCULATOR_ENABLE is still unset.
    configuration.unset(RoundRobinTableInputFormat.HBASE_REGIONSIZECALCULATOR_ENABLE);
    rrtif.configure();
    rrtif.unconfigure();
    String value = configuration.get(RoundRobinTableInputFormat.HBASE_REGIONSIZECALCULATOR_ENABLE);
    assertNull(value);
    // Assert HBASE_REGIONSIZECALCULATOR_ENABLE is still false when done.
    checkRetainsBooleanValue(jobContext, rrtif, false);
    // Assert HBASE_REGIONSIZECALCULATOR_ENABLE is still true when done.
    checkRetainsBooleanValue(jobContext, rrtif, true);
  }

  private void checkRetainsBooleanValue(JobContext jobContext, RoundRobinTableInputFormat rrtif,
      final boolean b) {
    jobContext.getConfiguration().
      setBoolean(RoundRobinTableInputFormat.HBASE_REGIONSIZECALCULATOR_ENABLE, b);
    rrtif.configure();
    rrtif.unconfigure();
    String value = jobContext.getConfiguration().
      get(RoundRobinTableInputFormat.HBASE_REGIONSIZECALCULATOR_ENABLE);
    assertEquals(b, Boolean.valueOf(value));
  }
}
