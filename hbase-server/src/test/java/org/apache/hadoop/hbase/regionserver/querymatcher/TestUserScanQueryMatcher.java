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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestUserScanQueryMatcher extends AbstractTestScanQueryMatcher {

  private static final Log LOG = LogFactory.getLog(TestUserScanQueryMatcher.class);

  /**
   * This is a cryptic test. It is checking that we don't include a fake cell, one that has a
   * timestamp of {@link HConstants#OLDEST_TIMESTAMP}. See HBASE-16074 for background.
   * @throws IOException
   */
  @Test
  public void testNeverIncludeFakeCell() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    // Do with fam2 which has a col2 qualifier.
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam2, 10, 1, ttl, KeepDeletedCells.FALSE, 0, rowComparator),
      get.getFamilyMap().get(fam2), now - ttl, now, null);
    Cell kv = new KeyValue(row1, fam2, col2, 1, data);
    Cell cell = CellUtil.createLastOnRowCol(kv);
    qm.setToNewRow(kv);
    MatchCode code = qm.match(cell);
    assertFalse(code.compareTo(MatchCode.SEEK_NEXT_COL) != 0);
  }

  @Test
  public void testMatchExplicitColumns() throws IOException {
    // Moving up from the Tracker by using Gets and List<KeyValue> instead
    // of just byte []

    // Expected result
    List<MatchCode> expected = new ArrayList<ScanQueryMatcher.MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.DONE);

    long now = EnvironmentEdgeManager.currentTime();
    // 2,4,5
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, 0, rowComparator),
      get.getFamilyMap().get(fam2), now - ttl, now, null);

    List<KeyValue> memstore = new ArrayList<KeyValue>();
    memstore.add(new KeyValue(row1, fam2, col1, 1, data));
    memstore.add(new KeyValue(row1, fam2, col2, 1, data));
    memstore.add(new KeyValue(row1, fam2, col3, 1, data));
    memstore.add(new KeyValue(row1, fam2, col4, 1, data));
    memstore.add(new KeyValue(row1, fam2, col5, 1, data));

    memstore.add(new KeyValue(row2, fam1, col1, data));

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<ScanQueryMatcher.MatchCode>();
    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (KeyValue kv : memstore) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      LOG.debug("expected " + expected.get(i) + ", actual " + actual.get(i));
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testMatch_Wildcard() throws IOException {
    // Moving up from the Tracker by using Gets and List<KeyValue> instead
    // of just byte []

    // Expected result
    List<MatchCode> expected = new ArrayList<ScanQueryMatcher.MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.DONE);

    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, 0, rowComparator), null,
      now - ttl, now, null);

    List<KeyValue> memstore = new ArrayList<KeyValue>();
    memstore.add(new KeyValue(row1, fam2, col1, 1, data));
    memstore.add(new KeyValue(row1, fam2, col2, 1, data));
    memstore.add(new KeyValue(row1, fam2, col3, 1, data));
    memstore.add(new KeyValue(row1, fam2, col4, 1, data));
    memstore.add(new KeyValue(row1, fam2, col5, 1, data));
    memstore.add(new KeyValue(row2, fam1, col1, 1, data));

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<ScanQueryMatcher.MatchCode>();

    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (KeyValue kv : memstore) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      LOG.debug("expected " + expected.get(i) + ", actual " + actual.get(i));
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  /**
   * Verify that {@link ScanQueryMatcher} only skips expired KeyValue instances and does not exit
   * early from the row (skipping later non-expired KeyValues). This version mimics a Get with
   * explicitly specified column qualifiers.
   * @throws IOException
   */
  @Test
  public void testMatch_ExpiredExplicit() throws IOException {

    long testTTL = 1000;
    MatchCode[] expected = new MatchCode[] { ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW, ScanQueryMatcher.MatchCode.DONE };

    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam2, 0, 1, testTTL, KeepDeletedCells.FALSE, 0, rowComparator),
      get.getFamilyMap().get(fam2), now - testTTL, now, null);

    KeyValue[] kvs = new KeyValue[] { new KeyValue(row1, fam2, col1, now - 100, data),
        new KeyValue(row1, fam2, col2, now - 50, data),
        new KeyValue(row1, fam2, col3, now - 5000, data),
        new KeyValue(row1, fam2, col4, now - 500, data),
        new KeyValue(row1, fam2, col5, now - 10000, data),
        new KeyValue(row2, fam1, col1, now - 10, data) };

    KeyValue k = kvs[0];
    qm.setToNewRow(k);

    List<MatchCode> actual = new ArrayList<MatchCode>(kvs.length);
    for (KeyValue kv : kvs) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      LOG.debug("expected " + expected[i] + ", actual " + actual.get(i));
      assertEquals(expected[i], actual.get(i));
    }
  }

  /**
   * Verify that {@link ScanQueryMatcher} only skips expired KeyValue instances and does not exit
   * early from the row (skipping later non-expired KeyValues). This version mimics a Get with
   * wildcard-inferred column qualifiers.
   * @throws IOException
   */
  @Test
  public void testMatch_ExpiredWildcard() throws IOException {

    long testTTL = 1000;
    MatchCode[] expected = new MatchCode[] { ScanQueryMatcher.MatchCode.INCLUDE,
        ScanQueryMatcher.MatchCode.INCLUDE, ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.INCLUDE, ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.DONE };

    long now = EnvironmentEdgeManager.currentTime();
    UserScanQueryMatcher qm = UserScanQueryMatcher.create(scan,
      new ScanInfo(this.conf, fam2, 0, 1, testTTL, KeepDeletedCells.FALSE, 0, rowComparator), null,
      now - testTTL, now, null);

    KeyValue[] kvs = new KeyValue[] { new KeyValue(row1, fam2, col1, now - 100, data),
        new KeyValue(row1, fam2, col2, now - 50, data),
        new KeyValue(row1, fam2, col3, now - 5000, data),
        new KeyValue(row1, fam2, col4, now - 500, data),
        new KeyValue(row1, fam2, col5, now - 10000, data),
        new KeyValue(row2, fam1, col1, now - 10, data) };
    KeyValue k = kvs[0];
    qm.setToNewRow(k);

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<ScanQueryMatcher.MatchCode>(kvs.length);
    for (KeyValue kv : kvs) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      LOG.debug("expected " + expected[i] + ", actual " + actual.get(i));
      assertEquals(expected[i], actual.get(i));
    }
  }
}
