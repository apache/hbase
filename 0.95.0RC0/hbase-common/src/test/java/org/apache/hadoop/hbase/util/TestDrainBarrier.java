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

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestDrainBarrier {

  @Test
  public void testBeginEndStopWork() throws Exception {
    DrainBarrier barrier = new DrainBarrier();
    assertTrue(barrier.beginOp());
    assertTrue(barrier.beginOp());
    barrier.endOp();
    barrier.endOp();
    barrier.stopAndDrainOps();
    assertFalse(barrier.beginOp());
  }

  @Test
  public void testUnmatchedEndAssert() throws Exception {
    DrainBarrier barrier = new DrainBarrier();
    try {
      barrier.endOp();
      fail("Should have asserted");
    } catch (AssertionError e) {
    }

    barrier.beginOp();
    barrier.beginOp();
    barrier.endOp();
    barrier.endOp();
    try {
      barrier.endOp();
      fail("Should have asserted");
    } catch (AssertionError e) {
    }
  }

  @Test
  public void testStopWithoutOpsDoesntBlock() throws Exception {
    DrainBarrier barrier = new DrainBarrier();
    barrier.stopAndDrainOpsOnce();

    barrier = new DrainBarrier();
    barrier.beginOp();
    barrier.endOp();
    barrier.stopAndDrainOpsOnce();
  }

  @Test
  /** This test tests blocking and can have false positives in very bad timing cases. */
  public void testStopIsBlockedByOps() throws Exception {
    final DrainBarrier barrier = new DrainBarrier();
    barrier.beginOp();
    barrier.beginOp();
    barrier.beginOp();
    barrier.endOp();

    Thread stoppingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          barrier.stopAndDrainOpsOnce();
        } catch (InterruptedException e) {
          fail("Should not have happened");
        }
      }
    });
    stoppingThread.start();

    // First "end" should not unblock the thread, but the second should.
    barrier.endOp();
    stoppingThread.join(1000);
    assertTrue(stoppingThread.isAlive());
    barrier.endOp();
    stoppingThread.join(30000); // When not broken, will be a very fast wait; set safe value.
    assertFalse(stoppingThread.isAlive());
  }

  @Test
  public void testMultipleStopOnceAssert() throws Exception {
    DrainBarrier barrier = new DrainBarrier();
    barrier.stopAndDrainOpsOnce();
    try {
      barrier.stopAndDrainOpsOnce();
      fail("Should have asserted");
    } catch (AssertionError e) {
    }
  }

  @Test
  public void testMultipleSloppyStopsHaveNoEffect() throws Exception {
    DrainBarrier barrier = new DrainBarrier();
    barrier.stopAndDrainOps();
    barrier.stopAndDrainOps();
  }
}
