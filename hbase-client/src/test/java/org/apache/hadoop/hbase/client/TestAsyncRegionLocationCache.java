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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

@Tag(SmallTests.TAG)
@Tag(ClientTests.TAG)
public class TestAsyncRegionLocationCache {

  @Test
  public void testAddingIndividualReplicasPreservesSiblings() {
    RegionInfo primary =
      RegionInfoBuilder.newBuilder(TableName.valueOf("test")).setRegionId(1).build();

    for (int[] order : new int[][] { { 0, 1, 2 }, { 2, 1, 0 } }) {
      AsyncNonMetaRegionLocator locator = createLocator();
      for (int replicaId : order) {
        locator.addLocationToCache(location(primary, replicaId));
      }

      RegionLocations locations =
        locator.getCachedLocation(primary.getTable(), primary.getStartKey());
      assertEquals(3, locations.numNonNullElements(), Arrays.toString(order));
    }
  }

  @Test
  public void testAddingDifferentRegionReplacesReplicas() {
    TableName tableName = TableName.valueOf("test");
    RegionInfo oldPrimary = RegionInfoBuilder.newBuilder(tableName).setRegionId(1).build();
    RegionInfo newPrimary = RegionInfoBuilder.newBuilder(tableName).setRegionId(2).build();
    AsyncNonMetaRegionLocator locator = createLocator();

    locator.addLocationToCache(location(oldPrimary, 0));
    locator.addLocationToCache(location(oldPrimary, 1));
    locator.addLocationToCache(location(newPrimary, 0));

    RegionLocations locations = locator.getCachedLocation(tableName, newPrimary.getStartKey());
    assertEquals(newPrimary, locations.getRegionLocation(0).getRegion());
    assertNull(locations.getRegionLocation(1));
  }

  private static AsyncNonMetaRegionLocator createLocator() {
    AsyncConnectionImpl connection = mock(AsyncConnectionImpl.class);
    when(connection.getConfiguration()).thenReturn(HBaseConfiguration.create());
    return new AsyncNonMetaRegionLocator(connection, mock(HashedWheelTimer.class));
  }

  private static HRegionLocation location(RegionInfo primary, int replicaId) {
    RegionInfo region = RegionReplicaUtil.getRegionInfoForReplica(primary, replicaId);
    return new HRegionLocation(region, ServerName.valueOf("server", 16000 + replicaId, 1));
  }
}
