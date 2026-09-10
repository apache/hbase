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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for encoded region-name collision detection (HBASE-30160). If two regions end up with the
 * same encoded name, we should fail fast instead of allowing subtle metadata corruption later.
 */

@Tag(SmallTests.TAG)
public class TestEncodedNameCollisionDetection {

  /**
   * Happy-path check: distinct candidate regions should pass without throwing.
   */
  @Test
  public void testAcceptsDistinctCandidates() {
    TableName tableName = TableName.valueOf("test_table");
    long regionId = System.currentTimeMillis();

    RegionInfo ri1 = RegionInfoBuilder.newBuilder(tableName).setStartKey(new byte[] { 0, 0 })
      .setEndKey(new byte[] { 1, 0 }).setSplit(false).setRegionId(regionId).build();

    RegionInfo ri2 = RegionInfoBuilder.newBuilder(tableName).setStartKey(new byte[] { 1, 0 })
      .setEndKey(new byte[] { 2, 0 }).setSplit(false).setRegionId(regionId + 1).build();

    RegionStates regionStates = mock(RegionStates.class);

    assertDoesNotThrow(
      () -> ModifyRegionUtils.checkForEncodedNameCollisions(Arrays.asList(ri1, ri2), regionStates));
  }

  /**
   * Verifies that duplicate encoded region names within candidate regions are rejected.
   */
  @Test
  public void testDetectsDuplicatesInCandidates() {
    TableName tableName = TableName.valueOf("test_table");
    RegionInfo ri1 = mockRegionInfo(tableName, "same-encoded-name");
    RegionInfo ri2 = mockRegionInfo(tableName, "same-encoded-name");

    RegionStates regionStates = mock(RegionStates.class);

    DoNotRetryIOException exception = assertThrows(DoNotRetryIOException.class,
      () -> ModifyRegionUtils.checkForEncodedNameCollisions(Arrays.asList(ri1, ri2), regionStates));
    assertTrue(exception.getMessage().contains("Encoded region name collision detected"));
  }

  /**
   * A candidate region should be rejected if its encoded name already exists.
   */
  @Test
  public void testDetectsCollisionWithExistingRegions() {
    TableName tableName = TableName.valueOf("test_table");
    RegionInfo candidateRegion = mockRegionInfo(tableName, "same-encoded-name");
    RegionStates regionStates = mock(RegionStates.class);
    when(regionStates.getRegionStateNodeFromEncodedRegionName("same-encoded-name"))
      .thenReturn(mock(RegionStateNode.class));

    DoNotRetryIOException exception =
      assertThrows(DoNotRetryIOException.class, () -> ModifyRegionUtils
        .checkForEncodedNameCollisions(Arrays.asList(candidateRegion), regionStates));
    assertTrue(exception.getMessage().contains("Encoded region name collision detected"));
  }

  /**
   * Test that checkForEncodedNameCollisions handles empty/null inputs and rejects null RegionStates
   * when candidates are present.
   */
  @Test
  public void testInputValidationAndNullRegionStatesBehavior() {
    assertDoesNotThrow(() -> ModifyRegionUtils.checkForEncodedNameCollisions(null, null));
    assertDoesNotThrow(
      () -> ModifyRegionUtils.checkForEncodedNameCollisions(Collections.emptyList(), null));
    assertDoesNotThrow(
      () -> ModifyRegionUtils.checkForEncodedNameCollisions(null, mock(RegionStates.class)));
    assertDoesNotThrow(() -> ModifyRegionUtils
      .checkForEncodedNameCollisions(Collections.emptyList(), mock(RegionStates.class)));

    RegionInfo candidateRegion = mockRegionInfo(TableName.valueOf("test_table"), "candidate");
    assertThrows(NullPointerException.class,
      () -> ModifyRegionUtils.checkForEncodedNameCollisions(Arrays.asList(candidateRegion), null));
  }

  private RegionInfo mockRegionInfo(TableName tableName, String encodedName) {
    RegionInfo regionInfo = mock(RegionInfo.class);
    when(regionInfo.getEncodedName()).thenReturn(encodedName);
    when(regionInfo.getTable()).thenReturn(tableName);
    return regionInfo;
  }
}
