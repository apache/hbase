/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestConnectionUtils {

  @Test
  public void testRetryTimeJitter() {
    long[] retries = new long[200];
    long baseTime = 1000000;  //Larger number than reality to help test randomness.
    long maxTimeExpected = (long) (baseTime * 1.01f);
    for (int i = 0; i < retries.length; i++) {
      retries[i] = ConnectionUtils.getPauseTime(baseTime, 0);
    }

    Set<Long> retyTimeSet = new TreeSet<Long>();
    for (long l : retries) {
      /*make sure that there is some jitter but only 1%*/
      assertTrue(l >= baseTime);
      assertTrue(l <= maxTimeExpected);
      // Add the long to the set
      retyTimeSet.add(l);
    }

    //Make sure that most are unique.  some overlap will happen
    assertTrue(retyTimeSet.size() > (retries.length * 0.80));
  }

  @Test
  public void testAddJitter() {
    long basePause = 10000;
    long maxTimeExpected = (long) (basePause * 1.25f);
    long minTimeExpected = (long) (basePause * 0.75f);
    int testTries = 100;

    Set<Long> timeSet = new TreeSet<Long>();
    for (int i = 0; i < testTries; i++) {
      long withJitter = ConnectionUtils.addJitter(basePause, 0.5f);
      assertTrue(withJitter >= minTimeExpected);
      assertTrue(withJitter <= maxTimeExpected);
      // Add the long to the set
      timeSet.add(withJitter);
    }

    //Make sure that most are unique.  some overlap will happen
    assertTrue(timeSet.size() > (testTries * 0.90));
  }

  @Test
  public void testGetPauseTime() {
    long pauseTime;
    long baseTime = 100;
    pauseTime = ConnectionUtils.getPauseTime(baseTime, -1);
    assertTrue(pauseTime >= (baseTime * HConstants.RETRY_BACKOFF[0]));
    assertTrue(pauseTime <= (baseTime * HConstants.RETRY_BACKOFF[0] * 1.01f));

    for (int i = 0; i < HConstants.RETRY_BACKOFF.length; i++) {
      pauseTime = ConnectionUtils.getPauseTime(baseTime, i);
      assertTrue(pauseTime >= (baseTime * HConstants.RETRY_BACKOFF[i]));
      assertTrue(pauseTime <= (baseTime * HConstants.RETRY_BACKOFF[i] * 1.01f));
    }

    int length = HConstants.RETRY_BACKOFF.length;
    pauseTime = ConnectionUtils.getPauseTime(baseTime, length);
    assertTrue(pauseTime >= (baseTime * HConstants.RETRY_BACKOFF[length - 1]));
    assertTrue(pauseTime <= (baseTime * HConstants.RETRY_BACKOFF[length - 1] * 1.01f));
  }
}
