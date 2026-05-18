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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestMovingAverage {

  private long[] data = { 1, 12, 13, 24, 25, 26, 37, 38, 39, 40 };
  private double delta = 0.1;

  @Test
  public void testSimpleMovingAverage(TestInfo testInfo) throws Exception {
    MovingAverage<?> algorithm = new SimpleMovingAverage(testInfo.getTestMethod().get().getName());
    int index = 0;
    // [1, 12, 13, 24]
    int bound = 4;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(12.5, algorithm.getAverageTime(), delta);
    // [1, 12, 13, 24, 25]
    bound = 5;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(15.0, algorithm.getAverageTime(), delta);
    // [1, 12, 13, 24, 25, 26, 37, 38]
    bound = 8;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(22.0, algorithm.getAverageTime(), delta);
    // [1, 12, 13, 24, 25, 26, 37, 38, 39, 40]
    for (; index < data.length; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(25.5, algorithm.getAverageTime(), delta);
  }

  @Test
  public void testWindowMovingAverage(TestInfo testInfo) throws Exception {
    // Default size is 5.
    MovingAverage<?> algorithm = new WindowMovingAverage(testInfo.getTestMethod().get().getName());
    int index = 0;
    // [1, 12, 13, 24]
    int bound = 4;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(12.5, algorithm.getAverageTime(), delta);
    // [1, 12, 13, 24, 25]
    bound = 5;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(15.0, algorithm.getAverageTime(), delta);
    // [24, 25, 26, 37, 38]
    bound = 8;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(30.0, algorithm.getAverageTime(), delta);
    // [26, 37, 38, 39, 40]
    for (; index < data.length; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(36.0, algorithm.getAverageTime(), delta);
  }

  @Test
  public void testWeightedMovingAverage(TestInfo testInfo) throws Exception {
    // Default size is 5.
    MovingAverage<?> algorithm =
      new WeightedMovingAverage(testInfo.getTestMethod().get().getName());
    int index = 0;
    // [1, 12, 13, 24]
    int bound = 4;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(12.5, algorithm.getAverageTime(), delta);
    // [1, 12, 13, 24, 25]
    bound = 5;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(15.0, algorithm.getAverageTime(), delta);
    // [24, 25, 26, 37, 38]
    bound = 8;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(32.67, algorithm.getAverageTime(), delta);
    // [26, 37, 38, 39, 40]
    for (; index < data.length; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(38.0, algorithm.getAverageTime(), delta);
  }

  @Test
  public void testExponentialMovingAverage(TestInfo testInfo) throws Exception {
    // [1, 12, 13, 24, 25, 26, 37, 38, 39, 40]
    MovingAverage<?> algorithm =
      new ExponentialMovingAverage(testInfo.getTestMethod().get().getName());
    int index = 0;
    int bound = 5;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(15.0, algorithm.getAverageTime(), delta);
    bound = 6;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(18.67, algorithm.getAverageTime(), delta);
    bound = 8;
    for (; index < bound; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(29.16, algorithm.getAverageTime(), delta);
    for (; index < data.length; index++) {
      algorithm.updateMostRecentTime(data[index]);
    }
    assertEquals(34.97, algorithm.getAverageTime(), delta);
  }
}
