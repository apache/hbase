/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;


public class TestHBaseAdmin extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestHBaseAdmin.class.getName());
  
  private String TABLE_STR = "testTable";
  private byte [] TABLE = Bytes.toBytes(TABLE_STR);
  private byte [] ROW = Bytes.toBytes("testRow");
  private byte [] FAMILY = Bytes.toBytes("testFamily");
  private byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private byte [] VALUE = Bytes.toBytes("testValue");
  
  private HBaseAdmin admin = null;
  private HConnection connection = null;
  
  /**
   * Constructor does nothing special, start cluster.
   */
  public TestHBaseAdmin() throws Exception{
    super();
  }

  
  public void testCreateTable() throws IOException {
    init();
    
    HTableDescriptor [] tables = connection.listTables();
    int numTables = tables.length;
    
    createTable(TABLE, FAMILY);
    tables = connection.listTables();
    
    assertEquals(numTables + 1, tables.length);
  }
  
  public void testCreateTableWithRegions() throws IOException {
    init();

    byte [][] splitKeys = { 
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 4, 4, 4 },
        new byte [] { 5, 5, 5 },
        new byte [] { 6, 6, 6 },
        new byte [] { 7, 7, 7 },
        new byte [] { 8, 8, 8 },
        new byte [] { 9, 9, 9 },
    };
    int expectedRegions = splitKeys.length + 1;
    
    HTableDescriptor desc = new HTableDescriptor(TABLE);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc, splitKeys);
    
    HTable ht = new HTable(conf, TABLE);
    Map<HRegionInfo,HServerAddress> regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(), 
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");
    
    Iterator<HRegionInfo> hris = regions.keySet().iterator();
    HRegionInfo hri = hris.next();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[0]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[0]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[1]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[1]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[2]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[2]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[3]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[3]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[4]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[4]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[5]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[5]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[6]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[6]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[7]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[7]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[8]));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[8]));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);
    
    // Now test using start/end with a number of regions
    
    // Use 80 bit numbers to make sure we aren't limited
    byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };
    
    // Splitting into 10 regions, we expect (null,1) ... (9, null)
    // with (1,2) (2,3) (3,4) (4,5) (5,6) (6,7) (7,8) (8,9) in the middle
    
    expectedRegions = 10;
    
    byte [] TABLE_2 = Bytes.add(TABLE, Bytes.toBytes("_2"));
    
    desc = new HTableDescriptor(TABLE_2);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc, startKey, endKey, expectedRegions);
    
    ht = new HTable(conf, TABLE_2);
    regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(), 
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");
    
    hris = regions.keySet().iterator();
    hri = hris.next();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {1,1,1,1,1,1,1,1,1,1}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {1,1,1,1,1,1,1,1,1,1}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {2,2,2,2,2,2,2,2,2,2}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {2,2,2,2,2,2,2,2,2,2}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {3,3,3,3,3,3,3,3,3,3}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {3,3,3,3,3,3,3,3,3,3}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {4,4,4,4,4,4,4,4,4,4}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {4,4,4,4,4,4,4,4,4,4}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {5,5,5,5,5,5,5,5,5,5}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {5,5,5,5,5,5,5,5,5,5}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {6,6,6,6,6,6,6,6,6,6}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {6,6,6,6,6,6,6,6,6,6}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {7,7,7,7,7,7,7,7,7,7}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {7,7,7,7,7,7,7,7,7,7}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {8,8,8,8,8,8,8,8,8,8}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {8,8,8,8,8,8,8,8,8,8}));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte [] {9,9,9,9,9,9,9,9,9,9}));
    hri = hris.next();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte [] {9,9,9,9,9,9,9,9,9,9}));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);
    
    // Try once more with something that divides into something infinite
    
    startKey = new byte [] { 0, 0, 0, 0, 0, 0 };
    endKey = new byte [] { 1, 0, 0, 0, 0, 0 };
    
    expectedRegions = 5;

    byte [] TABLE_3 = Bytes.add(TABLE, Bytes.toBytes("_3"));
    
    desc = new HTableDescriptor(TABLE_3);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin = new HBaseAdmin(conf);
    admin.createTable(desc, startKey, endKey, expectedRegions);
    
    ht = new HTable(conf, TABLE_3);
    regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(), 
        expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");
    
    // Try an invalid case where there are duplicate split keys
    splitKeys = new byte [][] {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 2, 2, 2 }
    };
    
    byte [] TABLE_4 = Bytes.add(TABLE, Bytes.toBytes("_4"));
    desc = new HTableDescriptor(TABLE_4);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin = new HBaseAdmin(conf);
    try {
      admin.createTable(desc, splitKeys);
      assertTrue("Should not be able to create this table because of duplicate split keys", false);
    } catch(IllegalArgumentException iae) {
      // Expected
    }
  }
  
  public void testDisableAndEnableTable() throws IOException {
    init();
    
    HTable ht =  createTable(TABLE, FAMILY);
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    
    admin.disableTable(TABLE);
    
    //Test that table is disabled
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = true;
    }
    assertEquals(true, ok);
    
    admin.enableTable(TABLE);
    
    //Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertEquals(true, ok);
  }
  
  
  public void testTableExist() throws IOException {
    init();
    boolean exist = false;
    
    exist = admin.tableExists(TABLE);
    assertEquals(false, exist);
    
    createTable(TABLE, FAMILY);
    
    exist = admin.tableExists(TABLE);
    assertEquals(true, exist);    
  }
  

//  public void testMajorCompact() throws Exception {
//    init();
//    
//    int testTableCount = 0;
//    int flushSleep = 1000;
//    int majocCompactSleep = 7000;
//    
//    HTable ht = createTable(TABLE, FAMILY);
//    byte [][] ROWS = makeN(ROW, 5);
//    
//    Put put = new Put(ROWS[0]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.flush(TABLE);
//    Thread.sleep(flushSleep);
//    
//    put = new Put(ROWS[1]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.flush(TABLE);
//    Thread.sleep(flushSleep);
//    
//    put = new Put(ROWS[2]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.flush(TABLE);
//    Thread.sleep(flushSleep);
//    
//    put = new Put(ROWS[3]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.majorCompact(TABLE);
//    Thread.sleep(majocCompactSleep);
//    
//    HRegion [] regions = null;
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          testTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//        System.out.println(output);
//      }
//    }
//    assertEquals(1, testTableCount);
//  }
//  
//
//
//  public void testFlush_TableName() throws Exception {
//    init();
//
//    int initTestTableCount = 0;
//    int testTableCount = 0;
//    
//    HTable ht = createTable(TABLE, FAMILY);
//
//    Put put = new Put(ROW);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    HRegion [] regions = null;
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          initTestTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//    
//    //Flushing 
//    admin.flush(TABLE);
//    Thread.sleep(2000);
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          testTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//
//    assertEquals(initTestTableCount + 1, testTableCount);
//  }
// 
//
//  public void testFlush_RegionName() throws Exception{
//    init();
//    int initTestTableCount = 0;
//    int testTableCount = 0;
//    String regionName = null;
//    
//    HTable ht = createTable(TABLE, FAMILY);
//
//    Put put = new Put(ROW);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    HRegion [] regions = null;
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String reg = Bytes.toString(region.getRegionName());
//      String table = reg.split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        regionName = reg;
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          initTestTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//    
//    //Flushing 
//    admin.flush(regionName);
//    Thread.sleep(2000);
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          testTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//
//    assertEquals(initTestTableCount + 1, testTableCount);
//  }

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  private byte [][] makeN(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, new byte[]{(byte)i});
    }
    return ret;
  }

  private HTable createTable(byte [] tableName, byte [] ... families) 
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      desc.addFamily(new HColumnDescriptor(family));
    }
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    return new HTable(conf, tableName);
  }
  
  private void init() throws IOException {
    connection = new HBaseAdmin(conf).connection;
    admin = new HBaseAdmin(conf);
  }
  
}