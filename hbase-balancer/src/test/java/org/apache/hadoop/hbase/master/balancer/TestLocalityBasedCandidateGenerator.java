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
package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.master.balancer.BalancerClusterState.LocalityType;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestLocalityBasedCandidateGenerator extends BalancerTestBase {

  private final LocalityBasedCandidateGenerator generator = new LocalityBasedCandidateGenerator();

  @ParameterizedTest
  @ValueSource(floats = { 0.0f, 0.75f, 1.0f })
  public void testSkipsLocalityTies(float locality) {
    float[][] localities = { { locality, locality }, { locality, locality } };
    BalancerClusterState moveCluster = createCluster(new int[] { 0, 2 }, localities);
    Assertions.assertSame(BalanceAction.NULL_ACTION, generator.generate(moveCluster));

    BalancerClusterState swapCluster = createCluster(new int[] { 1, 1 }, localities);
    Assertions.assertSame(BalanceAction.NULL_ACTION, generator.generate(swapCluster));
  }

  @Test
  public void testSkipsLocalityTiesAfterSwap() {
    BalancerClusterState cluster =
      createCluster(new int[] { 1, 1 }, new float[][] { { 0.75f, 0.75f }, { 0.75f, 0.75f } });
    cluster.getOrComputeRegionsToMostLocalEntities(LocalityType.SERVER);
    cluster.doAction(new SwapRegionsAction(0, 0, 1, 1));
    Assertions.assertSame(BalanceAction.NULL_ACTION, generator.generate(cluster));
  }

  @Test
  public void testMoveToHigherLocality() {
    BalancerClusterState cluster =
      createCluster(new int[] { 0, 2 }, new float[][] { { 0.75f, 0.25f }, { 0.75f, 0.25f } });
    MoveRegionAction action =
      Assertions.assertInstanceOf(MoveRegionAction.class, generator.generate(cluster));
    Assertions.assertEquals(1, action.getFromServer());
    Assertions.assertEquals(0, action.getToServer());
  }

  @Test
  public void testSwapToHigherLocality() {
    BalancerClusterState cluster =
      createCluster(new int[] { 1, 1 }, new float[][] { { 0.25f, 0.75f }, { 0.75f, 0.25f } });
    Assertions.assertInstanceOf(SwapRegionsAction.class, generator.generate(cluster));
  }

  @Test
  public void testAllowsLocalityNeutralSwap() {
    BalancerClusterState cluster =
      createCluster(new int[] { 1, 1 }, new float[][] { { 0.25f, 0.75f }, { 0.25f, 0.75f } });
    Assertions.assertInstanceOf(SwapRegionsAction.class, generator.generate(cluster));
  }

  private BalancerClusterState createCluster(int[] regionCounts, float[][] localities) {
    return new BalancerClusterState(mockClusterServers(new int[][] { regionCounts }), null, null,
      null) {
      @Override
      float getLocalityOfRegion(int region, int server) {
        return localities[region][server];
      }

      @Override
      public int getRegionSizeMB(int region) {
        return 1;
      }
    };
  }
}
