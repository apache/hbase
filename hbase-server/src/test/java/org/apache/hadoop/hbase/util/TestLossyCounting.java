/*
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

package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestLossyCounting {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLossyCounting.class);

  private final Configuration conf = HBaseConfiguration.create();

  @Test
  public void testBucketSize() {
    LossyCounting<?> lossyCounting = new LossyCounting<>("testBucketSize", 0.01);
    assertEquals(100L, lossyCounting.getBucketSize());
    LossyCounting<?> lossyCounting2 = new LossyCounting<>("testBucketSize2", conf);
    assertEquals(50L, lossyCounting2.getBucketSize());
  }

  @Test
  public void testAddByOne() {
    LossyCounting<String> lossyCounting = new LossyCounting<>("testAddByOne", 0.01);
    for (int i = 0; i < 100; i++) {
      String key = "" + i;
      lossyCounting.add(key);
    }
    assertEquals(100L, lossyCounting.getDataSize());
    for (int i = 0; i < 100; i++) {
      String key = "" + i;
      assertTrue(lossyCounting.contains(key));
    }
  }

  @Test
  public void testSweep1() throws Exception {
    LossyCounting<String> lossyCounting = new LossyCounting<>("testSweep1", 0.01);
    for(int i = 0; i < 400; i++){
      String key = "" + i;
      lossyCounting.add(key);
    }
    assertEquals(4L, lossyCounting.getCurrentTerm());
    waitForSweep(lossyCounting);

    //Do last one sweep as some sweep will be skipped when first one was running
    lossyCounting.sweep();
    assertEquals(lossyCounting.getBucketSize() - 1, lossyCounting.getDataSize());
  }

  private void waitForSweep(LossyCounting<?> lossyCounting) throws InterruptedException {
    //wait for sweep thread to complete
    int retry = 0;
    while (!lossyCounting.getSweepFuture().isDone() && retry < 10) {
      Thread.sleep(100);
      retry++;
    }
  }

  @Test
  public void testSweep2() throws Exception {
    LossyCounting<String> lossyCounting = new LossyCounting<>("testSweep2", 0.1);
    for (int i = 0; i < 10; i++) {
      String key = "" + i;
      lossyCounting.add(key);
    }
    waitForSweep(lossyCounting);
    assertEquals(10L, lossyCounting.getDataSize());
    for(int i = 0; i < 10; i++){
      String key = "1";
      lossyCounting.add(key);
    }
    waitForSweep(lossyCounting);
    assertEquals(1L, lossyCounting.getDataSize());
  }
}
