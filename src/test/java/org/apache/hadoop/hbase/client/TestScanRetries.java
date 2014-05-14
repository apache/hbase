/**
 * Copyright 2010 The Apache Software Foundation
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
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ParamFormat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Test various scanner timeout issues.
 */
@Category(MediumTests.class)
public class TestScanRetries {

  private final static HBaseTestingUtility
          TEST_UTIL = new HBaseTestingUtility();

  final Log LOG = LogFactory.getLog(getClass());
  private final static byte[] SOME_BYTES = Bytes.toBytes("f");
  private final static byte[] TABLE_NAME = Bytes.toBytes("t");
  private final static int NB_ROWS = 6;
  private final static int SCANNER_TIMEOUT = 100000;
  private static HTable table;
  private static boolean enableFailure = false;

  public static class TestRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {
    private long nextCallCount = 0l;

    public TestRegionServer(Configuration conf)
        throws IOException {
      super(conf);
    }

    @ParamFormat(clazz = ScanParamsFormatter.class)
    @Override
    public Result[] next(final long scannerId, int nbRows) throws IOException {
      ++nextCallCount;
      LOG.info("nextCallCount: " + String.valueOf(nextCallCount));
      if (enableFailure && nextCallCount % 5 == 0) {
        super.next(scannerId, nbRows);
        LOG.info("Something bad happened on the way from server to client. Should force retry!");
        throw new IOException("Forcing retry");
      }
      return super.next(scannerId, nbRows);
    }
  }
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
      Configuration conf = TEST_UTIL.getConfiguration();
      conf.setInt("hbase.regionserver.lease.period", SCANNER_TIMEOUT);
      conf.setInt("hbase.client.retries.number", 5);
      TEST_UTIL.startMiniCluster(1, 2, TestRegionServer.class);
      table = TEST_UTIL.createTable(Bytes.toBytes("t"), SOME_BYTES);
      for (int i = 0; i < NB_ROWS; i++) {
          Put put = new Put(Bytes.toBytes(i));
          put.add(SOME_BYTES, SOME_BYTES, SOME_BYTES);
          table.put(put);
      }
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * Tests that we have right number of rows in scan
   * @throws Exception
   */
  @Test
  public void testNumberOfRowsInScanWithoutRetries() throws Exception {
    enableFailure = false;
    doTestNumberOfRowsInScan();
  }

  /**
   * Tests that we have right number of rows in scan even with retries
   * @throws Exception
   */
  @Test
  public void testNumberOfRowsInScanWithRetries() throws Exception {
    enableFailure = true;
    doTestNumberOfRowsInScan();
  }

  public void doTestNumberOfRowsInScan() throws Exception {
    Scan scan = new Scan();
    ResultScanner r = table.getScanner(scan);
    int count = 0;
    try {
      Result res = r.next();
      while (res != null) {
        count++;
        res = r.next();
      }
    } catch (Throwable e) {
      LOG.error("Got exception " + e.getMessage(), e);
      fail("Exception while counting rows!");
    }
    r.close();
    LOG.info("Number of rows read: " + String.valueOf(count));
    assertEquals(NB_ROWS, count);
  }
}

