/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.CoprocessorHConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCoprocessorHConnection {

  static final Log LOG = LogFactory.getLog(TestCoprocessorHConnection.class);

  public final static byte[] A = Bytes.toBytes("a");
  private static final int ROWSIZE = 20;
  private static final byte[] rowSeperator1 = Bytes.toBytes(5);
  private static final byte[] rowSeperator2 = Bytes.toBytes(12);
  
  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  public static class FooCoprocessor extends BaseRegionObserver {
    private HRegion region;
    private CoprocessorEnvironment env;
    
    @Override
    public void start(CoprocessorEnvironment e) {
      region = ((RegionCoprocessorEnvironment)e).getRegion();
      env = e;
    }
    @Override
    public void stop(CoprocessorEnvironment e) {
      region = null;
    }
    
    public byte[] getRegionStartKey() {
      return region.getStartKey();
    }
    
    public Result getOnCoprocessorHConnection(TableName tableName, byte[] key)
        throws IOException {
      HConnection conn = CoprocessorHConnection.getConnectionForEnvironment(env);
      // conn returned by CoprocessorHConnection#getConnectionForEnvironment is the expected type
      assertTrue(conn instanceof CoprocessorHConnection);      
      HTableInterface hTable = conn.getTable(tableName);
      Get get = new Get(key);
      Result result = hTable.get(get);
      return result;
    }
  }
  
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.startMiniCluster();
    cluster = util.getMiniHBaseCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testHConnection() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();
    TableName testTable = TableName.valueOf("TestHConnection");

    try {
      // Check table exists
      if (admin.tableExists(testTable)) {
        admin.disableTable(testTable);
        admin.deleteTable(testTable);
      }

      HTableDescriptor htd = new HTableDescriptor(testTable);
      htd.addFamily(new HColumnDescriptor(A));

      // Register FooCoprocessor as a table coprocessor
      htd.addCoprocessor(FooCoprocessor.class.getName());

      // Create a table with 3 region
      admin.createTable(htd, new byte[][] { rowSeperator1, rowSeperator2 });
      util.waitUntilAllRegionsAssigned(testTable);
    } finally {
      admin.close();
    }

    //Get Table
    HTable table = new HTable(util.getConfiguration(), testTable);

    try{
      // Put some data
      for (long i = 0; i < ROWSIZE; i++) {
        byte[] iBytes = Bytes.toBytes(i);
        Put put = new Put(iBytes);
        put.add(A, A, iBytes);
        table.put(put);
      }

      // Get Table's First Region
      HRegion firstRegion = cluster.getRegions(testTable).get(0);

      // Look up the coprocessor instance running the Region
      Coprocessor cp = firstRegion.getCoprocessorHost().findCoprocessor(FooCoprocessor.class.getName());
      assertNotNull("FooCoprocessor coprocessor should be loaded", cp);
      FooCoprocessor fc = (FooCoprocessor) cp;

      // Find the start key for the region that FooCoprocessor is running on.
      byte[] regionStartKey = fc.getRegionStartKey();
      
      if (regionStartKey == null || regionStartKey.length <= 0) {
        // Its the start row.  Can't ask for null.  Ask for minimal key instead.
        regionStartKey = new byte [] {0};
      }
 
      // Get Key Data
      Get get =  new Get(regionStartKey);
      Result keyData = table.get(get);

      // Get Key Data using with CoprocessorHConnection
      Result cpData = fc.getOnCoprocessorHConnection(testTable, regionStartKey);
      // Check them equals
      assertEquals(keyData.getValue(A, A), cpData.getValue(A, A));
    } finally {
      table.close();
    }
  }
}