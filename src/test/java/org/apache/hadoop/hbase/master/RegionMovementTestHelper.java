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

package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RegionMovementTestHelper extends HBaseTestingUtility {

  private static final Log LOG = LogFactory.getLog(RegionMovementTestHelper.class);

  private static int sleepTime = 100;
  private int lastRegionOpenedCount = 0;

  public RegionMovementTestHelper() {
    super();
    sleepTime = 3 * this.getConfiguration().
        getInt("hbase.regionserver.msginterval", 1000);
  }

  /**
   * Create a table with specified table name and region number.
   *
   * @param table     name of the table
   * @param regionNum number of regions to create.
   * @throws java.io.IOException
   */
  public void createTable(String table, int regionNum)
      throws IOException, InterruptedException {
    byte[] tableName = Bytes.toBytes(table);
    byte[][] splitKeys = new byte[regionNum - 1][];
    byte[][] putKeys = new byte[regionNum - 1][];
    for (int i = 1; i < regionNum; i++) {
      byte splitKey = (byte) i;
      splitKeys[i - 1] = new byte[] { splitKey, splitKey, splitKey };
      putKeys[i - 1] = new byte[] { splitKey, splitKey, (byte) (i - 1) };
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    new HBaseAdmin(getConfiguration()).createTable(desc, splitKeys);

    HTable ht = new HTable(getConfiguration(), tableName);
    Map<HRegionInfo, HServerAddress> regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + regionNum + " regions "
        + "but only found " + regions.size(), regionNum, regions.size());

    // Try and make sure that everything is up and assigned
    waitForTableConsistent();
    // Try and make sure that everything is assigned to their final destination.
    waitOnStableRegionMovement();

    try {
      for (byte[] rk : putKeys) {
        Put p = new Put(rk);
        p.add(HConstants.CATALOG_FAMILY, Bytes.toBytes(0L), Bytes.toBytes("testValue"));
        ht.put(p);
      }
    } finally {
      ht.close();
    }
  }

  public void waitOnTable(String tableName) throws IOException {

    waitForTableConsistent();

    HTable ht = new HTable(getConfiguration(), tableName);
    Scan s = new Scan();
    ResultScanner rs = null;
    try {
      rs = ht.getScanner(s);
      Result r = null;
      do {
        r = rs.next();
      } while (r != null);
    } finally {
      if (rs != null) rs.close();
      if (ht != null) ht.close();
    }
  }


  public void waitOnStableRegionMovement() throws IOException, InterruptedException {
    int first = -1;
    int second = 0;

    int attempt = 0;
    waitForTableConsistent();
    while (first != second && attempt < 10) {
      first = second;
      second = getHBaseCluster().getMaster().getMetrics().getRegionsOpened();
      Thread.sleep((++attempt) * sleepTime);
    }
  }
  /**
   * Verify the number of region movement is expected
   *
   * @param expected
   * @throws InterruptedException
   */
  public void verifyRegionMovementNum(int expected)
      throws InterruptedException {
    MiniHBaseCluster cluster = getHBaseCluster();
    HMaster m = cluster.getMaster();

    int retry = 10;
    int attempt = 0;
    int currentRegionOpened, regionMovement;
    do {
      currentRegionOpened = m.getMetrics().getRegionsOpened();
      regionMovement = currentRegionOpened - lastRegionOpenedCount;
      LOG.debug("There are " + regionMovement + "/" + expected +
          " regions moved after " + attempt + " attempts");
      Thread.sleep((++attempt) * sleepTime);
    } while (regionMovement < expected && attempt <= retry);

    // update the lastRegionOpenedCount
    resetLastOpenedRegionCount(currentRegionOpened);

    assertTrue("There are only " + regionMovement + " instead of "
            + expected + " region movement for " + attempt + " attempts",
        expected <= regionMovement
    );

    int maxExpected = (int) (expected * 1.5f);

    // Because of how over-loaded some jvm's are during tests, region open can take quite a while
    // this will cause extra assignments as the region will get assigned to an intermediate
    // region server before being moved to the preferred server. This check allows for some of that
    // but not too much.  1.5x expected is pretty generous, but makes sure that some of the regions
    // made it to their preferred destination in one move.
    //
    // On a real cluster this is less likely to happen as there will be more region servers and they
    // will be less resource constrained.
    assertTrue("There are  " + regionMovement + " expecting max of "
            + maxExpected + " after " + attempt + " attempts",
        maxExpected >= regionMovement
    );
  }
  public void resetLastOpenedRegionCount() {
    resetLastOpenedRegionCount(getHBaseCluster().getMaster().getMetrics().getRegionsOpened());
  }

  public void resetLastOpenedRegionCount(int newCount) {
    this.lastRegionOpenedCount = newCount;
  }
}
