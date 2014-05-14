/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestParallelScanner {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("FAMILY");
  private static int SLAVES = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testParallelScanner() throws IOException {
    // Create and load the table
    byte [] name = Bytes.toBytes("testParallelScanner");
    int regionCnt = 25;
    HTable table = TEST_UTIL.createTable(name, new byte[][] { FAMILY }, 3,
            Bytes.toBytes("bbb"), Bytes.toBytes("yyy"), regionCnt);

    final int rowCount = TEST_UTIL.loadTable(table, FAMILY);
    TEST_UTIL.flush(name);

    Scan[] rowsPerRegionScanner = new Scan[regionCnt];
    Pair<byte[][],byte[][]> startAndEndKeys =  table.getStartEndKeys();
    assertEquals(regionCnt, startAndEndKeys.getFirst().length);
    assertEquals(regionCnt, startAndEndKeys.getSecond().length);

    // Get the rows per region in the sequential order.
    int totalRowsScannedInSequential = 0;
    for (int i = 0; i < regionCnt; i++) {
      Scan s = new Scan();
      s.setStartRow(startAndEndKeys.getFirst()[i]);
      s.setStopRow(startAndEndKeys.getSecond()[i]);
      s.addFamily(FAMILY);
      rowsPerRegionScanner[i] = s;
      totalRowsScannedInSequential += HBaseTestingUtility.countRows(table,
          new Scan(s));
    }
    assertEquals("Total rows scanned in sequential", rowCount,
        totalRowsScannedInSequential);

    // Construct a ParallelScanner
    ParallelScanner parallelScanner =
      new ParallelScanner(table, Arrays.asList(rowsPerRegionScanner), 10);

    int totalRowScannedInParallel = 0;
    parallelScanner.initialize();
    List<Result> results = null;
    while(!(results = parallelScanner.next()).isEmpty()) {
      totalRowScannedInParallel += results.size();
    }
    parallelScanner.close();
    assertEquals("Total row scanned in parallel", rowCount,
        totalRowScannedInParallel);
  }
}
