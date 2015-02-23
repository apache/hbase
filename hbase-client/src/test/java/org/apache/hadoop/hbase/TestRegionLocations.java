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

package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegionLocations {

  ServerName sn0 = ServerName.valueOf("host0", 10, 10);
  ServerName sn1 = ServerName.valueOf("host1", 10, 10);
  ServerName sn2 = ServerName.valueOf("host2", 10, 10);
  ServerName sn3 = ServerName.valueOf("host3", 10, 10);

  HRegionInfo info0 = hri(0);
  HRegionInfo info1 = hri(1);
  HRegionInfo info2 = hri(2);
  HRegionInfo info9 = hri(9);

  long regionId1 = 1000;
  long regionId2 = 2000;

  @Test
  public void testSizeMethods() {
    RegionLocations list = new RegionLocations();
    assertTrue(list.isEmpty());
    assertEquals(0, list.size());
    assertEquals(0, list.numNonNullElements());

    list = hrll((HRegionLocation)null);
    assertTrue(list.isEmpty());
    assertEquals(1, list.size());
    assertEquals(0, list.numNonNullElements());

    HRegionInfo info0 = hri(0);
    list = hrll(hrl(info0, null));
    assertTrue(list.isEmpty());
    assertEquals(1, list.size());
    assertEquals(0, list.numNonNullElements());

    HRegionInfo info9 = hri(9);
    list = hrll(hrl(info9, null));
    assertTrue(list.isEmpty());
    assertEquals(10, list.size());
    assertEquals(0, list.numNonNullElements());

    list = hrll(hrl(info0, null), hrl(info9, null));
    assertTrue(list.isEmpty());
    assertEquals(10, list.size());
    assertEquals(0, list.numNonNullElements());
  }

  private HRegionInfo hri(int replicaId) {
    return hri(regionId1, replicaId);
  }

  private HRegionInfo hri(long regionId, int replicaId) {
    TableName table = TableName.valueOf("table");
    byte[] startKey = HConstants.EMPTY_START_ROW;
    byte[] endKey = HConstants.EMPTY_END_ROW;
    HRegionInfo info = new HRegionInfo(table, startKey, endKey, false, regionId, replicaId);
    return info;
  }

  private HRegionLocation hrl(HRegionInfo hri, ServerName sn) {
    return new HRegionLocation(hri, sn);
  }

  private HRegionLocation hrl(HRegionInfo hri, ServerName sn, long seqNum) {
    return new HRegionLocation(hri, sn, seqNum);
  }

  private RegionLocations hrll(HRegionLocation ... locations) {
    return new RegionLocations(locations);
  }

  @Test
  public void testRemoveByServer() {
    RegionLocations list;

    // test remove from empty list
    list = new RegionLocations();
    assertTrue(list == list.removeByServer(sn0));

    // test remove from single element list
    list = hrll(hrl(info0, sn0));
    assertTrue(list == list.removeByServer(sn1));
    list = list.removeByServer(sn0);
    assertEquals(0, list.numNonNullElements());

    // test remove from multi element list
    list = hrll(hrl(info0, sn0), hrl(info1, sn1), hrl(info2, sn2), hrl(info9, sn2));
    assertTrue(list == list.removeByServer(sn3)); // no region is mapped to sn3
    list = list.removeByServer(sn0);
    assertNull(list.getRegionLocation(0));
    assertEquals(sn1, list.getRegionLocation(1).getServerName());
    assertEquals(sn2, list.getRegionLocation(2).getServerName());
    assertNull(list.getRegionLocation(5));
    assertEquals(sn2, list.getRegionLocation(9).getServerName());

    // test multi-element remove from multi element list
    list = hrll(hrl(info0, sn1), hrl(info1, sn1), hrl(info2, sn0), hrl(info9, sn0));
    list = list.removeByServer(sn0);
    assertEquals(sn1, list.getRegionLocation(0).getServerName());
    assertEquals(sn1, list.getRegionLocation(1).getServerName());
    assertNull(list.getRegionLocation(2));
    assertNull(list.getRegionLocation(5));
    assertNull(list.getRegionLocation(9));
  }

  @Test
  public void testRemove() {
    RegionLocations list;

    // test remove from empty list
    list = new RegionLocations();
    assertTrue(list == list.remove(hrl(info0, sn0)));

    // test remove from single element list
    list = hrll(hrl(info0, sn0));
    assertTrue(list == list.remove(hrl(info0, sn1)));
    list = list.remove(hrl(info0, sn0));
    assertTrue(list.isEmpty());

    // test remove from multi element list
    list = hrll(hrl(info0, sn0), hrl(info1, sn1), hrl(info2, sn2), hrl(info9, sn2));
    assertTrue(list == list.remove(hrl(info1, sn3))); // no region is mapped to sn3
    list = list.remove(hrl(info0, sn0));
    assertNull(list.getRegionLocation(0));
    assertEquals(sn1, list.getRegionLocation(1).getServerName());
    assertEquals(sn2, list.getRegionLocation(2).getServerName());
    assertNull(list.getRegionLocation(5));
    assertEquals(sn2, list.getRegionLocation(9).getServerName());

    list = list.remove(hrl(info9, sn2));
    assertNull(list.getRegionLocation(0));
    assertEquals(sn1, list.getRegionLocation(1).getServerName());
    assertEquals(sn2, list.getRegionLocation(2).getServerName());
    assertNull(list.getRegionLocation(5));
    assertNull(list.getRegionLocation(9));


    // test multi-element remove from multi element list
    list = hrll(hrl(info0, sn1), hrl(info1, sn1), hrl(info2, sn0), hrl(info9, sn0));
    list = list.remove(hrl(info9, sn0));
    assertEquals(sn1, list.getRegionLocation(0).getServerName());
    assertEquals(sn1, list.getRegionLocation(1).getServerName());
    assertEquals(sn0, list.getRegionLocation(2).getServerName());
    assertNull(list.getRegionLocation(5));
    assertNull(list.getRegionLocation(9));
  }

  @Test
  public void testUpdateLocation() {
    RegionLocations list;

    // test add to empty list
    list = new RegionLocations();
    list = list.updateLocation(hrl(info0, sn1), false, false);
    assertEquals(sn1, list.getRegionLocation(0).getServerName());

    // test add to non-empty list
    list = list.updateLocation(hrl(info9, sn3, 10), false, false);
    assertEquals(sn3, list.getRegionLocation(9).getServerName());
    assertEquals(10, list.size());
    list = list.updateLocation(hrl(info2, sn2, 10), false, false);
    assertEquals(sn2, list.getRegionLocation(2).getServerName());
    assertEquals(10, list.size());

    // test update greater SeqNum
    list = list.updateLocation(hrl(info2, sn3, 11), false, false);
    assertEquals(sn3, list.getRegionLocation(2).getServerName());
    assertEquals(sn3, list.getRegionLocation(9).getServerName());

    // test update equal SeqNum
    list = list.updateLocation(hrl(info2, sn1, 11), false, false); // should not update
    assertEquals(sn3, list.getRegionLocation(2).getServerName());
    assertEquals(sn3, list.getRegionLocation(9).getServerName());
    list = list.updateLocation(hrl(info2, sn1, 11), true, false); // should update
    assertEquals(sn1, list.getRegionLocation(2).getServerName());
    assertEquals(sn3, list.getRegionLocation(9).getServerName());

    // test force update
    list = list.updateLocation(hrl(info2, sn2, 9), false, true); // should update
    assertEquals(sn2, list.getRegionLocation(2).getServerName());
    assertEquals(sn3, list.getRegionLocation(9).getServerName());
  }

  @Test
  public void testMergeLocations() {
    RegionLocations list1, list2;

    // test merge empty lists
    list1 = new RegionLocations();
    list2 = new RegionLocations();

    assertTrue(list1 == list1.mergeLocations(list2));

    // test merge non-empty and empty
    list2 = hrll(hrl(info0, sn0));
    list1 = list1.mergeLocations(list2);
    assertEquals(sn0, list1.getRegionLocation(0).getServerName());

    // test merge empty and non empty
    list1 = hrll();
    list1 = list2.mergeLocations(list1);
    assertEquals(sn0, list1.getRegionLocation(0).getServerName());

    // test merge non intersecting
    list1 = hrll(hrl(info0, sn0), hrl(info1, sn1));
    list2 = hrll(hrl(info2, sn2));
    list1 = list2.mergeLocations(list1);
    assertEquals(sn0, list1.getRegionLocation(0).getServerName());
    assertEquals(sn1, list1.getRegionLocation(1).getServerName());
    assertEquals(2, list1.size()); // the size is taken from the argument list to merge

    // do the other way merge as well
    list1 = hrll(hrl(info0, sn0), hrl(info1, sn1));
    list2 = hrll(hrl(info2, sn2));
    list1 = list1.mergeLocations(list2);
    assertEquals(sn0, list1.getRegionLocation(0).getServerName());
    assertEquals(sn1, list1.getRegionLocation(1).getServerName());
    assertEquals(sn2, list1.getRegionLocation(2).getServerName());

    // test intersecting lists same seqNum
    list1 = hrll(hrl(info0, sn0), hrl(info1, sn1));
    list2 = hrll(hrl(info0, sn2), hrl(info1, sn2), hrl(info9, sn3));
    list1 = list2.mergeLocations(list1); // list1 should override
    assertEquals(2, list1.size());
    assertEquals(sn0, list1.getRegionLocation(0).getServerName());
    assertEquals(sn1, list1.getRegionLocation(1).getServerName());

    // do the other way
    list1 = hrll(hrl(info0, sn0), hrl(info1, sn1));
    list2 = hrll(hrl(info0, sn2), hrl(info1, sn2), hrl(info9, sn3));
    list1 = list1.mergeLocations(list2); // list2 should override
    assertEquals(10, list1.size());
    assertEquals(sn2, list1.getRegionLocation(0).getServerName());
    assertEquals(sn2, list1.getRegionLocation(1).getServerName());
    assertEquals(sn3, list1.getRegionLocation(9).getServerName());

    // test intersecting lists different seqNum
    list1 = hrll(hrl(info0, sn0, 10), hrl(info1, sn1, 10));
    list2 = hrll(hrl(info0, sn2, 11), hrl(info1, sn2, 11), hrl(info9, sn3, 11));
    list1 = list1.mergeLocations(list2); // list2 should override because of seqNum
    assertEquals(10, list1.size());
    assertEquals(sn2, list1.getRegionLocation(0).getServerName());
    assertEquals(sn2, list1.getRegionLocation(1).getServerName());
    assertEquals(sn3, list1.getRegionLocation(9).getServerName());

    // do the other way
    list1 = hrll(hrl(info0, sn0, 10), hrl(info1, sn1, 10));
    list2 = hrll(hrl(info0, sn2, 11), hrl(info1, sn2, 11), hrl(info9, sn3, 11));
    list1 = list1.mergeLocations(list2); // list2 should override
    assertEquals(10, list1.size());
    assertEquals(sn2, list1.getRegionLocation(0).getServerName());
    assertEquals(sn2, list1.getRegionLocation(1).getServerName());
    assertEquals(sn3, list1.getRegionLocation(9).getServerName());
  }

  @Test
  public void testMergeLocationsWithDifferentRegionId() {
    RegionLocations list1, list2;

    // test merging two lists. But the list2 contains region replicas with a different region id
    HRegionInfo info0 = hri(regionId1, 0);
    HRegionInfo info1 = hri(regionId1, 1);
    HRegionInfo info2 = hri(regionId2, 2);

    list1 = hrll(hrl(info2, sn1));
    list2 = hrll(hrl(info0, sn2), hrl(info1, sn2));
    list1 = list2.mergeLocations(list1);
    assertNull(list1.getRegionLocation(0));
    assertNull(list1.getRegionLocation(1));
    assertNotNull(list1.getRegionLocation(2));
    assertEquals(sn1, list1.getRegionLocation(2).getServerName());
    assertEquals(3, list1.size());

    // try the other way merge
    list1 = hrll(hrl(info2, sn1));
    list2 = hrll(hrl(info0, sn2), hrl(info1, sn2));
    list2 = list1.mergeLocations(list2);
    assertNotNull(list2.getRegionLocation(0));
    assertNotNull(list2.getRegionLocation(1));
    assertNull(list2.getRegionLocation(2));
  }

  @Test
  public void testUpdateLocationWithDifferentRegionId() {
    RegionLocations list;

    HRegionInfo info0 = hri(regionId1, 0);
    HRegionInfo info1 = hri(regionId2, 1);
    HRegionInfo info2 = hri(regionId1, 2);

    list = new RegionLocations(hrl(info0, sn1), hrl(info2, sn1));

    list = list.updateLocation(hrl(info1, sn2), false, true); // force update

    // the other locations should be removed now
    assertNull(list.getRegionLocation(0));
    assertNotNull(list.getRegionLocation(1));
    assertNull(list.getRegionLocation(2));
    assertEquals(sn2, list.getRegionLocation(1).getServerName());
    assertEquals(3, list.size());
  }


  @Test
  public void testConstructWithNullElements() {
    // RegionLocations can contain null elements as well. These null elements can

    RegionLocations list = new RegionLocations((HRegionLocation)null);
    assertTrue(list.isEmpty());
    assertEquals(1, list.size());
    assertEquals(0, list.numNonNullElements());

    list = new RegionLocations(null, hrl(info1, sn0));
    assertFalse(list.isEmpty());
    assertEquals(2, list.size());
    assertEquals(1, list.numNonNullElements());

    list = new RegionLocations(hrl(info0, sn0), null);
    assertEquals(2, list.size());
    assertEquals(1, list.numNonNullElements());

    list = new RegionLocations(null, hrl(info2, sn0), null, hrl(info9, sn0));
    assertEquals(10, list.size());
    assertEquals(2, list.numNonNullElements());

    list = new RegionLocations(null, hrl(info2, sn0), null, hrl(info9, sn0), null);
    assertEquals(11, list.size());
    assertEquals(2, list.numNonNullElements());

    list = new RegionLocations(null, hrl(info2, sn0), null, hrl(info9, sn0), null, null);
    assertEquals(12, list.size());
    assertEquals(2, list.numNonNullElements());
  }
}
