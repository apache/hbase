/*

 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A basic unit test that spins up a local HBase cluster.
 */
@Category(MediumTests.class)
public class TestProcessBasedCluster {

  private static final Log LOG = LogFactory.getLog(TestProcessBasedCluster.class);

  private static final int COLS_PER_ROW = 5;
  private static final int FLUSHES = 5;
  private static final int NUM_REGIONS = 5;
  private static final int ROWS_PER_FLUSH = 5;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // DISABLED BECAUSE FLAKEY @Test(timeout=300 * 1000)
  public void testProcessBasedCluster() throws Exception {
    ProcessBasedLocalHBaseCluster cluster = new ProcessBasedLocalHBaseCluster(
        TEST_UTIL.getConfiguration(), 2, 3);
    cluster.startMiniDFS();
    cluster.startHBase();
    try {
      TEST_UTIL.createRandomTable(HTestConst.DEFAULT_TABLE_STR,
          HTestConst.DEFAULT_CF_STR_SET,
          HColumnDescriptor.DEFAULT_VERSIONS, COLS_PER_ROW, FLUSHES, NUM_REGIONS,
          ROWS_PER_FLUSH);
      HTable table = new HTable(TEST_UTIL.getConfiguration(), HTestConst.DEFAULT_TABLE_BYTES);
      ResultScanner scanner = table.getScanner(HTestConst.DEFAULT_CF_BYTES);
      Result result;
      int rows = 0;
      int cols = 0;
      while ((result = scanner.next()) != null) {
        ++rows;
        cols += result.getFamilyMap(HTestConst.DEFAULT_CF_BYTES).size();
      }
      LOG.info("Read " + rows + " rows, " + cols + " columns");
      scanner.close();
      table.close();

      // These numbers are deterministic, seeded by table name.
      assertEquals(19, rows);
      assertEquals(35, cols);
    } catch (Exception ex) {
      LOG.error(ex);
      throw ex;
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testHomePath() {
    File pom = new File(HBaseHomePath.getHomePath(), "pom.xml");
    assertTrue(pom.getPath() + " does not exist", pom.exists());
  }

}
