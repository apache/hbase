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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.KeyValueTestUtil.create;
import static org.apache.hadoop.hbase.regionserver.KeyValueScanFixture.scanFixture;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreScannerDeleteMarkerOptimization {
  private static final String CF = "cf";
  private static final byte[] CF_BYTES = Bytes.toBytes(CF);
  private static final Configuration CONF = HBaseConfiguration.create();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreScannerDeleteMarkerOptimization.class);

  private List<KeyValue> kvs;
  private Pair<List<ExtendedCell>, Long> result;

  private Pair<List<ExtendedCell>, Long> scanAll() throws IOException {
    return scanAll(KeepDeletedCells.FALSE, 1);
  }

  private Pair<List<ExtendedCell>, Long> scanAll(KeepDeletedCells keepDeletedCells, int maxVersions)
    throws IOException {
    ScanInfo scanInfo = new ScanInfo(CONF, CF_BYTES, 0, maxVersions, Long.MAX_VALUE,
      keepDeletedCells, HConstants.DEFAULT_BLOCKSIZE, 0, CellComparator.getInstance(), false);
    List<KeyValueScanner> scanners = scanFixture(kvs.toArray(new KeyValue[0]));
    Scan scanSpec = new Scan().readVersions(maxVersions);
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, null, scanners)) {
      List<ExtendedCell> results = new ArrayList<>();
      // noinspection StatementWithEmptyBody
      while (scan.next(results)) {
      }
      return new Pair<>(results, scan.getEstimatedNumberOfKvsScanned());
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    org.apache.logging.log4j.Logger log =
      org.apache.logging.log4j.LogManager.getLogger(StoreScanner.class);
    org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
      org.apache.logging.log4j.Level.TRACE);
  }

  @Before
  public void setUp() {
    kvs = new ArrayList<>();
  }

  @Test
  public void testDelete() throws IOException {
    kvs.add(create("r", CF, "q", 2, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 1, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 1, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 1, Type.Put, "v"));

    result = scanAll();
    assertEquals(1, result.getFirst().size());
    assertEquals(kvs.get(4), result.getFirst().get(0));
    // kvs0, kvs2, kvs4
    assertEquals(3, result.getSecond().longValue());
  }

  @Test
  public void testDuplicatDelete() throws IOException {
    kvs.add(create("r", CF, "q", 3, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 3, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 3, Type.Delete, ""));

    result = scanAll();
    assertEquals(0, result.getFirst().size());
    // kvs0, kvs1
    assertEquals(2, result.getSecond().longValue());
  }

  @Test
  public void testNotDuplicatDelete() throws IOException {
    kvs.add(create("r", CF, "q", 3, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 2, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 1, Type.Put, "v"));

    result = scanAll();
    assertEquals(1, result.getFirst().size());
    assertEquals(kvs.get(2), result.getFirst().get(0));
    assertEquals(kvs.size(), result.getSecond().longValue());
  }

  @Test
  public void testDeleteEmptyQualifier() throws IOException {
    kvs.add(create("r", CF, "", 2, Type.Delete, ""));
    kvs.add(create("r", CF, "", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "", 1, Type.Delete, ""));
    kvs.add(create("r", CF, "", 1, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 1, Type.Put, "v"));

    result = scanAll();
    assertEquals(1, result.getFirst().size());
    assertEquals(kvs.get(4), result.getFirst().get(0));
    assertEquals(kvs.size(), result.getSecond().longValue());
  }

  @Test
  public void testDeleteFamilyVersion() throws IOException {
    // DeleteFamilyVersion cannot be optimized
    kvs.add(create("r", CF, "", 3, Type.DeleteFamilyVersion, ""));
    kvs.add(create("r", CF, "", 2, Type.DeleteFamilyVersion, ""));
    kvs.add(create("r", CF, "", 1, Type.DeleteFamilyVersion, ""));
    kvs.add(create("r", CF, "q", 3, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 1, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 3, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 1, Type.Put, "v"));

    result = scanAll();
    assertEquals(0, result.getFirst().size());
    assertEquals(kvs.size(), result.getSecond().longValue());
  }

  @Test
  public void testDuplicatDeleteFamilyVersion() throws IOException {
    kvs.add(create("r", CF, "q", 3, Type.DeleteFamilyVersion, ""));
    kvs.add(create("r", CF, "q", 3, Type.DeleteFamilyVersion, ""));
    kvs.add(create("r", CF, "q", 3, Type.DeleteFamilyVersion, ""));

    result = scanAll();
    assertEquals(0, result.getFirst().size());
    // kvs0, kvs1
    assertEquals(2, result.getSecond().longValue());
  }

  @Test
  public void testNotDuplicatDeleteFamilyVersion() throws IOException {
    kvs.add(create("r", CF, "q", 3, Type.DeleteFamilyVersion, ""));
    kvs.add(create("r", CF, "q", 2, Type.DeleteFamilyVersion, ""));
    kvs.add(create("r", CF, "q", 1, Type.DeleteFamilyVersion, ""));

    result = scanAll();
    assertEquals(0, result.getFirst().size());
    assertEquals(kvs.size(), result.getSecond().longValue());
  }

  @Test
  public void testDeleteColumn() throws IOException {
    kvs.add(create("r", CF, "q", 3, Type.DeleteColumn, ""));
    kvs.add(create("r", CF, "q", 2, Type.DeleteColumn, ""));
    kvs.add(create("r", CF, "q", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 1, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 1, Type.Put, "v"));

    result = scanAll();
    assertEquals(1, result.getFirst().size());
    assertEquals(kvs.get(4), result.getFirst().get(0));
    // kvs0, kvs4
    assertEquals(2, result.getSecond().longValue());
  }

  @Test
  public void testDeleteFamily() throws IOException {
    kvs.add(create("r", CF, "", 3, Type.DeleteFamily, ""));
    kvs.add(create("r", CF, "", 2, Type.DeleteFamily, ""));
    kvs.add(create("r", CF, "", 1, Type.DeleteFamily, ""));
    kvs.add(create("r", CF, "q", 4, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 3, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 1, Type.Put, "v"));

    result = scanAll();
    assertEquals(1, result.getFirst().size());
    assertEquals(kvs.get(3), result.getFirst().get(0));
    // kvs0, kvs3, kvs4, kvs6
    assertEquals(4, result.getSecond().longValue());
  }

  @Test
  public void testKeepDeletedCells() throws IOException {
    kvs.add(create("r", CF, "q", 2, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 1, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 1, Type.Put, "v"));
    kvs.add(create("r", CF, "q1", 1, Type.Put, "v"));

    // optimization does not work. optimization works only for KeepDeletedCells.FALSE
    result = scanAll(KeepDeletedCells.TRUE, 1);
    assertEquals(1, result.getFirst().size());
    assertEquals(kvs.get(4), result.getFirst().get(0));
    assertEquals(kvs.size(), result.getSecond().longValue());

    // optimization does not work. optimization works only for KeepDeletedCells.FALSE
    result = scanAll(KeepDeletedCells.TTL, 1);
    assertEquals(1, result.getFirst().size());
    assertEquals(kvs.get(4), result.getFirst().get(0));
    assertEquals(kvs.size(), result.getSecond().longValue());
  }

  @Test
  public void testScanMaxVersions() throws IOException {
    kvs.add(create("r", CF, "q", 2, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 2, Type.Put, "v"));
    kvs.add(create("r", CF, "q", 1, Type.Delete, ""));
    kvs.add(create("r", CF, "q", 1, Type.Put, "v"));

    // optimization works only for maxVersions = 1
    result = scanAll(KeepDeletedCells.FALSE, 1);
    assertEquals(0, result.getFirst().size());
    // kvs0, kvs1
    assertEquals(2, result.getSecond().longValue());

    // optimization does not work
    result = scanAll(KeepDeletedCells.FALSE, 2);
    assertEquals(0, result.getFirst().size());
    assertEquals(kvs.size(), result.getSecond().longValue());
  }
}
