/**
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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.regionserver.HRegion.warmupHRegion;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * Run tests that use the HBase clients; {@link HTable}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 */
@Category(LargeTests.class)
@SuppressWarnings ("deprecation")
public class TestWarmupRegion {
  private static final Log LOG = LogFactory.getLog(TestWarmupRegion.class);
  protected TableName TABLENAME = TableName.valueOf("testPurgeFutureDeletes");
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  private static byte[] COLUMN = Bytes.toBytes("column");
  private static int numRows = 10000;
  protected static int SLAVES = 3;
  private static MiniHBaseCluster myCluster;
  private static Table table;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(SLAVES);
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
    table = TEST_UTIL.createTable(TABLENAME, FAMILY);

    // future timestamp
    for (int i = 0; i < numRows; i++) {
      long ts = System.currentTimeMillis() * 2;
      Put put = new Put(ROW, ts);
      put.add(FAMILY, COLUMN, VALUE);
      table.put(put);
    }

    // major compaction, purged future deletes
    TEST_UTIL.getHBaseAdmin().flush(TABLENAME);
    TEST_UTIL.getHBaseAdmin().majorCompact(TABLENAME);

    // waiting for the major compaction to complete
    TEST_UTIL.waitFor(6000, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return TEST_UTIL.getHBaseAdmin().getCompactionState(TABLENAME) ==
            AdminProtos.GetRegionInfoResponse.CompactionState.NONE;
      }
    });

    table.close();
  }


  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  protected void runwarmup()  throws InterruptedException{
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
        HRegion region = TEST_UTIL.getMiniHBaseCluster().getRegions(TABLENAME).get(0);
        HRegionInfo info = region.getRegionInfo();

        try {
          HTableDescriptor htd = table.getTableDescriptor();
          for (int i = 0; i < 10; i++) {
            warmupHRegion(info, htd, rs.getWAL(info), rs.getConfiguration(), rs, null);
          }

        } catch (IOException ie) {
          LOG.error("Failed warming up region " + info.getRegionNameAsString(), ie);
        }
      }
    });
    thread.start();
    thread.join();
  }

  /**
   * Basic client side validation of HBASE-4536
   */
   @Test
   public void testWarmup() throws Exception {
     int serverid = 0;
     HRegion region = TEST_UTIL.getMiniHBaseCluster().getRegions(TABLENAME).get(0);
     HRegionInfo info = region.getRegionInfo();
     runwarmup();
     for (int i = 0; i < 10; i++) {
       HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(serverid);
       byte [] destName = Bytes.toBytes(rs.getServerName().toString());
       TEST_UTIL.getMiniHBaseCluster().getMaster().move(info.getEncodedNameAsBytes(), destName);
       //wait region online
       TEST_UTIL.waitUntilNoRegionsInTransition(1000);
       serverid = (serverid + 1) % 2;
     }
   }
}
