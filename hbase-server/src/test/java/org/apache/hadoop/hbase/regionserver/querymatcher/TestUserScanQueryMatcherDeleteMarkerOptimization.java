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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import static org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil.enableVisiblityLabels;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestUserScanQueryMatcherDeleteMarkerOptimization extends AbstractTestScanQueryMatcher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestUserScanQueryMatcherDeleteMarkerOptimization.class);

  private List<Pair<KeyValue, MatchCode>> pairs;

  private void verify(List<Pair<KeyValue, MatchCode>> pairs) throws IOException {
    verify(pairs, 1, false, false);
  }

  private void verify(List<Pair<KeyValue, MatchCode>> pairs, int maxVersions,
    boolean visibilityEnabled, boolean newVersionBehavior) throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    scan.readVersions(maxVersions);
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam1, 0, maxVersions, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, newVersionBehavior),
      get.getFamilyMap().get(fam1), now - ttl, now, null);

    List<KeyValue> storedKVs = new ArrayList<>();
    for (Pair<KeyValue, MatchCode> Pair : pairs) {
      storedKVs.add(Pair.getFirst());
    }

    List<MatchCode> scannedKVs = new ArrayList<>();
    qm.setToNewRow(storedKVs.get(0));

    ExtendedCell prevCell = null;

    for (KeyValue kv : storedKVs) {
      MatchCode matchCode = qm.match(kv, prevCell, visibilityEnabled);
      prevCell = kv;
      scannedKVs.add(matchCode);
    }

    assertEquals(pairs.size(), scannedKVs.size());
    for (int i = 0; i < scannedKVs.size(); i++) {
      assertEquals("second, index - " + i, pairs.get(i).getSecond(), scannedKVs.get(i));
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    pairs = new ArrayList<>();
  }

  private KeyValue createKV(byte[] col, long timestamp, Type delete) {
    return new KeyValue(row1, fam1, col, timestamp, delete, data);
  }

  @Test
  public void testNotDuplicatDelete() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 2, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Delete), MatchCode.SKIP));
    verify(pairs);
  }

  @Test
  public void testEffectiveDelete() throws IOException {
    pairs.add(new Pair<>(createKV(null, 2, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 2, Type.Put), MatchCode.SEEK_NEXT_COL));
    pairs.add(new Pair<>(createKV(col1, 2, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 2, Type.Put), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testDeleteWithMaxVersions() throws IOException {
    // For simplicity, do not optimize when maxVersions > 1
    pairs.add(new Pair<>(createKV(col1, 2, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 2, Type.Put), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Put), MatchCode.INCLUDE));
    verify(pairs, 2, false, false);
  }

  @Test
  public void testDuplicatDelete() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 2, Type.Delete), MatchCode.SKIP));
    // After prevCell is read and it is checked duplicated, the second cell can be SEEK_NEXT_COL
    pairs.add(new Pair<>(createKV(col1, 2, Type.Delete), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testDeleteAfterPut() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 2, Type.Put), MatchCode.INCLUDE));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Delete), MatchCode.SKIP));
    verify(pairs);
  }

  @Test
  public void testChangeColumn() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 2, Type.Put), MatchCode.INCLUDE));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col2, 2, Type.Put), MatchCode.INCLUDE));
    verify(pairs);
  }

  @Test
  public void testNotDuplicatDeleteFamilyVersion() throws IOException {
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamilyVersion), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 1, Type.DeleteFamilyVersion), MatchCode.SKIP));
    verify(pairs);
  }

  @Test
  public void testEffectiveDeleteFamilyVersion() throws IOException {
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamilyVersion), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 2, Type.Put), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testDuplicatDeleteFamilyVersion() throws IOException {
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamilyVersion), MatchCode.SKIP));
    // After prevCell is read and it is checked duplicated, the second cell can be SEEK_NEXT_COL
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamilyVersion), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testDeleteFamilyVersionAfterPut() throws IOException {
    pairs.add(new Pair<>(createKV(null, 2, Type.Put), MatchCode.INCLUDE));
    pairs.add(new Pair<>(createKV(null, 1, Type.DeleteFamilyVersion), MatchCode.SKIP));
    verify(pairs);
  }

  @Test
  public void testDifferentDeleteFamilyVersion() throws IOException {
    // DeleteFamilyVersion cannot be optimized
    pairs.add(new Pair<>(createKV(null, 3, Type.DeleteFamilyVersion), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamilyVersion), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 1, Type.DeleteFamilyVersion), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 3, Type.Put), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 2, Type.Put), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Put), MatchCode.SKIP));
    verify(pairs);
  }

  @Test
  public void testDeleteColumn() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 2, Type.DeleteColumn), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testDeleteColumnAfterPut() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 2, Type.Put), MatchCode.INCLUDE));
    pairs.add(new Pair<>(createKV(col1, 1, Type.DeleteColumn), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testDeleteFamily() throws IOException {
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamily), MatchCode.SEEK_NEXT_COL));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Put), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testDeleteFamilyAfterPut() throws IOException {
    pairs.add(new Pair<>(createKV(null, 3, Type.Put), MatchCode.INCLUDE));
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamily), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testKeyForNextColumnForDeleteFamily() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(
      scan, new ScanInfo(this.conf, fam1, 0, 1, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, rowComparator, false),
      get.getFamilyMap().get(fam1), now - ttl, now, null);

    KeyValue kv;
    ExtendedCell nextColumnKey;

    // empty qualifier should not return the instance of LastOnRowColCell except for DeleteFamily
    kv = createKV(null, 2, Type.Put);
    nextColumnKey = qm.getKeyForNextColumn(kv);
    assertNotEquals("LastOnRowColCell", nextColumnKey.getClass().getSimpleName());

    // empty qualifier should not return the instance of LastOnRowColCell except for DeleteFamily
    kv = createKV(null, 2, Type.Delete);
    nextColumnKey = qm.getKeyForNextColumn(kv);
    assertNotEquals("LastOnRowColCell", nextColumnKey.getClass().getSimpleName());

    // empty qualifier should return the instance of LastOnRowColCell only for DeleteFamily
    kv = createKV(null, 2, Type.DeleteFamily);
    nextColumnKey = qm.getKeyForNextColumn(kv);
    assertEquals("LastOnRowColCell", nextColumnKey.getClass().getSimpleName());
  }

  @Test
  public void testNoDelete() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 2, Type.Put), MatchCode.INCLUDE));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Put), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testVisibilityLabelEnabled() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    enableVisiblityLabels(conf);
    pairs.add(new Pair<>(createKV(col1, 3, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 3, Type.Put), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamily), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Put), MatchCode.SEEK_NEXT_COL));
    verify(pairs, 1, VisibilityUtils.isVisibilityLabelEnabled(conf), false);
  }

  @Test
  public void testScanWithFilter() throws IOException {
    scan.setFilter(new FilterList());
    pairs.add(new Pair<>(createKV(col1, 3, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 3, Type.Put), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamily), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Put), MatchCode.SEEK_NEXT_COL));
    verify(pairs);
  }

  @Test
  public void testNewVersionBehavior() throws IOException {
    pairs.add(new Pair<>(createKV(col1, 3, Type.Delete), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 3, Type.Put), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(null, 2, Type.DeleteFamily), MatchCode.SKIP));
    pairs.add(new Pair<>(createKV(col1, 1, Type.Put), MatchCode.SKIP));
    verify(pairs, 2, false, true);
  }
}
