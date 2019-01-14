/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSequenceIdAccounting {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSequenceIdAccounting.class);

  private static final byte [] ENCODED_REGION_NAME = Bytes.toBytes("r");
  private static final byte [] FAMILY_NAME = Bytes.toBytes("cf");
  private static final Set<byte[]> FAMILIES;
  static {
    FAMILIES = new HashSet<>();
    FAMILIES.add(FAMILY_NAME);
  }

  @Test
  public void testStartCacheFlush() {
    SequenceIdAccounting sida = new SequenceIdAccounting();
    sida.getOrCreateLowestSequenceIds(ENCODED_REGION_NAME);
    Map<byte[], Long> m = new HashMap<>();
    m.put(ENCODED_REGION_NAME, HConstants.NO_SEQNUM);
    assertEquals(HConstants.NO_SEQNUM, (long)sida.startCacheFlush(ENCODED_REGION_NAME, FAMILIES));
    sida.completeCacheFlush(ENCODED_REGION_NAME);
    long sequenceid = 1;
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid, true);
    // Only one family so should return NO_SEQNUM still.
    assertEquals(HConstants.NO_SEQNUM, (long)sida.startCacheFlush(ENCODED_REGION_NAME, FAMILIES));
    sida.completeCacheFlush(ENCODED_REGION_NAME);
    long currentSequenceId = sequenceid;
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid, true);
    final Set<byte[]> otherFamily = new HashSet<>(1);
    otherFamily.add(Bytes.toBytes("otherCf"));
    sida.update(ENCODED_REGION_NAME, FAMILIES, ++sequenceid, true);
    // Should return oldest sequence id in the region.
    assertEquals(currentSequenceId, (long)sida.startCacheFlush(ENCODED_REGION_NAME, otherFamily));
    sida.completeCacheFlush(ENCODED_REGION_NAME);
  }

  @Test
  public void testAreAllLower() {
    SequenceIdAccounting sida = new SequenceIdAccounting();
    sida.getOrCreateLowestSequenceIds(ENCODED_REGION_NAME);
    Map<byte[], Long> m = new HashMap<>();
    m.put(ENCODED_REGION_NAME, HConstants.NO_SEQNUM);
    assertTrue(sida.areAllLower(m, null));
    long sequenceid = 1;
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid++, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid++, true);
    assertTrue(sida.areAllLower(m, null));
    m.put(ENCODED_REGION_NAME, sequenceid);
    assertFalse(sida.areAllLower(m, null));
    ArrayList<byte[]> regions = new ArrayList<>();
    assertFalse(sida.areAllLower(m, regions));
    assertEquals(1, regions.size());
    assertArrayEquals(ENCODED_REGION_NAME, regions.get(0));
    long lowest = sida.getLowestSequenceId(ENCODED_REGION_NAME);
    assertEquals("Lowest should be first sequence id inserted", 1, lowest);
    m.put(ENCODED_REGION_NAME, lowest);
    assertFalse(sida.areAllLower(m, null));
    // Now make sure above works when flushing.
    sida.startCacheFlush(ENCODED_REGION_NAME, FAMILIES);
    assertFalse(sida.areAllLower(m, null));
    m.put(ENCODED_REGION_NAME, HConstants.NO_SEQNUM);
    assertTrue(sida.areAllLower(m, null));
    // Let the flush complete and if we ask if the sequenceid is lower, should be yes since no edits
    sida.completeCacheFlush(ENCODED_REGION_NAME);
    m.put(ENCODED_REGION_NAME, sequenceid);
    assertTrue(sida.areAllLower(m, null));
    // Flush again but add sequenceids while we are flushing.
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid++, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid++, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid++, true);
    lowest = sida.getLowestSequenceId(ENCODED_REGION_NAME);
    m.put(ENCODED_REGION_NAME, lowest);
    assertFalse(sida.areAllLower(m, null));
    sida.startCacheFlush(ENCODED_REGION_NAME, FAMILIES);
    // The cache flush will clear out all sequenceid accounting by region.
    assertEquals(HConstants.NO_SEQNUM, sida.getLowestSequenceId(ENCODED_REGION_NAME));
    sida.completeCacheFlush(ENCODED_REGION_NAME);
    // No new edits have gone in so no sequenceid to work with.
    assertEquals(HConstants.NO_SEQNUM, sida.getLowestSequenceId(ENCODED_REGION_NAME));
    // Make an edit behind all we'll put now into sida.
    m.put(ENCODED_REGION_NAME, sequenceid);
    sida.update(ENCODED_REGION_NAME, FAMILIES, ++sequenceid, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, ++sequenceid, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, ++sequenceid, true);
    assertTrue(sida.areAllLower(m, null));
  }

  @Test
  public void testFindLower() {
    SequenceIdAccounting sida = new SequenceIdAccounting();
    sida.getOrCreateLowestSequenceIds(ENCODED_REGION_NAME);
    Map<byte[], Long> m = new HashMap<>();
    m.put(ENCODED_REGION_NAME, HConstants.NO_SEQNUM);
    long sequenceid = 1;
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid++, true);
    sida.update(ENCODED_REGION_NAME, FAMILIES, sequenceid++, true);
    assertTrue(sida.findLower(m) == null);
    m.put(ENCODED_REGION_NAME, sida.getLowestSequenceId(ENCODED_REGION_NAME));
    assertTrue(sida.findLower(m).length == 1);
    m.put(ENCODED_REGION_NAME, sida.getLowestSequenceId(ENCODED_REGION_NAME) - 1);
    assertTrue(sida.findLower(m) == null);
  }
}
