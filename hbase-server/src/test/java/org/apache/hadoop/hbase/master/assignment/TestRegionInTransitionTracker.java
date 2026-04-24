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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestRegionInTransitionTracker {

  private RegionInTransitionTracker tracker;
  private RegionInfo regionInfo;
  private RegionStateNode regionStateNode;
  private ManualEnvironmentEdge edge;
  private AtomicLong ritDuration;
  private AtomicInteger ritDurationCalls;

  @BeforeEach
  public void setUp() {
    ritDuration = new AtomicLong(-1L);
    ritDurationCalls = new AtomicInteger();
    tracker = new RegionInTransitionTracker(duration -> {
      ritDuration.set(duration);
      ritDurationCalls.incrementAndGet();
    });
    regionInfo = RegionInfoBuilder.FIRST_META_REGIONINFO;
    regionStateNode = new RegionStateNode(regionInfo, new AtomicInteger());

    edge = new ManualEnvironmentEdge();
    edge.setValue(1_000L);
    EnvironmentEdgeManagerTestHelper.injectEdge(edge);
  }

  @AfterEach
  public void tearDown() {
    EnvironmentEdgeManagerTestHelper.reset();
    tracker.stop();
  }

  @Test
  public void testInjectedRitDurationConsumerUsesFirstEnterTimestamp() throws Exception {
    regionStateNode.setState(RegionState.State.OPEN);
    tracker.handleRegionStateNodeOperation(regionStateNode);
    assertFalse(tracker.isRegionInTransition(regionInfo));
    assertFalse(tracker.hasRegionsInTransition());
    assertEquals(0, tracker.getRegionsInTransition().size());

    edge.incValue(100L);
    regionStateNode.transitionState(RegionState.State.CLOSING, RegionState.State.OPEN);
    tracker.handleRegionStateNodeOperation(regionStateNode);
    assertTrue(tracker.isRegionInTransition(regionInfo));
    assertEquals(1, tracker.getRegionsInTransition().size());

    edge.incValue(100L);
    regionStateNode.transitionState(RegionState.State.CLOSED, RegionState.State.CLOSING);
    tracker.handleRegionStateNodeOperation(regionStateNode);
    assertTrue(tracker.isRegionInTransition(regionInfo));
    assertEquals(1, tracker.getRegionsInTransition().size());

    edge.incValue(100L);
    regionStateNode.transitionState(RegionState.State.OPENING, RegionState.State.CLOSED);
    tracker.handleRegionStateNodeOperation(regionStateNode);
    assertTrue(tracker.isRegionInTransition(regionInfo));
    assertEquals(1, tracker.getRegionsInTransition().size());

    edge.incValue(100L);
    regionStateNode.transitionState(RegionState.State.OPEN, RegionState.State.OPENING);
    tracker.handleRegionStateNodeOperation(regionStateNode);

    assertFalse(tracker.isRegionInTransition(regionInfo));
    assertFalse(tracker.hasRegionsInTransition());
    assertEquals(1, ritDurationCalls.get());
    assertTrue(ritDuration.get() >= 0);
    assertEquals(300L, ritDuration.get());
  }

  @Test
  public void testRegionCrashUsesCrashTimestampAsRitStart() {
    regionStateNode.setState(RegionState.State.OPEN);

    edge.incValue(100L);
    regionStateNode.crashed(edge.currentTime());

    edge.incValue(100L);
    tracker.regionCrashed(regionStateNode);
    assertTrue(tracker.isRegionInTransition(regionInfo));

    edge.incValue(200L);
    tracker.handleRegionStateNodeOperation(regionStateNode);

    assertFalse(tracker.isRegionInTransition(regionInfo));
    assertEquals(1, ritDurationCalls.get());
    assertEquals(300L, ritDuration.get());
  }
}
