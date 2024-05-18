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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestReservoirSample {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReservoirSample.class);

  @Test
  public void test() {
    int round = 100000;
    int containsOne = 0;
    for (int i = 0; i < round; i++) {
      ReservoirSample<Integer> rs = new ReservoirSample<>(10);
      for (int j = 0; j < 100; j++) {
        rs.add(j);
        if (j < 10) {
          assertEquals(j + 1, rs.getSamplingResult().size());
        } else {
          assertEquals(10, rs.getSamplingResult().size());
        }
      }
      if (rs.getSamplingResult().contains(1)) {
        containsOne++;
      }
    }
    // we assume a 5% error rate
    assertTrue(containsOne > round / 10 * 0.95);
    assertTrue(containsOne < round / 10 * 1.05);
  }

  @Test
  public void testIterator() {
    int round = 100000;
    int containsOne = 0;
    for (int i = 0; i < round; i++) {
      ReservoirSample<Integer> rs = new ReservoirSample<>(10);
      rs.add(IntStream.range(0, 100).mapToObj(Integer::valueOf).iterator());
      if (rs.getSamplingResult().contains(1)) {
        containsOne++;
      }
    }
    // we assume a 5% error rate
    assertTrue(containsOne > round / 10 * 0.95);
    assertTrue(containsOne < round / 10 * 1.05);
  }

  @Test
  public void testStream() {
    int round = 100000;
    int containsOne = 0;
    for (int i = 0; i < round; i++) {
      ReservoirSample<Integer> rs = new ReservoirSample<>(10);
      rs.add(IntStream.range(0, 100).mapToObj(Integer::valueOf));
      if (rs.getSamplingResult().contains(1)) {
        containsOne++;
      }
    }
    // we assume a 5% error rate
    assertTrue(containsOne > round / 10 * 0.95);
    assertTrue(containsOne < round / 10 * 1.05);
  }

  @Test
  public void testNegativeSamplingNumber() {
    IllegalArgumentException e =
      assertThrows(IllegalArgumentException.class, () -> new ReservoirSample<Integer>(-1));
    assertEquals("negative sampling number(-1) is not allowed", e.getMessage());
  }
}
