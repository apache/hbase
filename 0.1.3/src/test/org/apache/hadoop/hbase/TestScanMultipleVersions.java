/**
 * Copyright 2008 The Apache Software Foundation
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

package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.io.BatchUpdate;

/**
 * Regression test for HBASE-613
 */
public class TestScanMultipleVersions extends HBaseTestCase {
  private final Text TABLE_NAME = new Text("TestScanMultipleVersions");
  private final HRegionInfo[] INFOS = new HRegionInfo[2];
  private final HRegion[] REGIONS = new HRegion[2];
  // Row keys
  private final Text[] ROWS = new Text[] {
      new Text("row_0200"),
      new Text("row_0800")
  };

  private final long[] TIMESTAMPS = new long[] {
      100L,
      1000L
  };
  private final Random rand = new Random();
  private HTableDescriptor desc = null;
  private Path rootdir = null;
  private MiniDFSCluster dfsCluster = null;

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    // Create table description

    this.desc = new HTableDescriptor(TABLE_NAME.toString());
    this.desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY_STR));

    // Region 0 will contain the key range [,row_0500)
    INFOS[0] = new HRegionInfo(this.desc, HConstants.EMPTY_START_ROW,
        new Text("row_0500"));
    // Region 1 will contain the key range [row_0500,)
    INFOS[1] = new HRegionInfo(this.desc, new Text("row_0500"),
          HConstants.EMPTY_TEXT);
    
    // start HDFS
    dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    try {
      // Set the hbase.rootdir to be the home directory in mini dfs.
      this.conf.set(HConstants.HBASE_DIR, 
          dfsCluster.getFileSystem().getHomeDirectory().toString());
      fs = dfsCluster.getFileSystem();
      this.rootdir = fs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR)));
      fs.mkdirs(this.rootdir);

      // Create root region
      HRegion root = HRegion.createHRegion(HRegionInfo.rootRegionInfo,
          this.rootdir, this.conf);
      // Create meta region
      HRegion meta = HRegion.createHRegion(HRegionInfo.firstMetaRegionInfo,
          this.rootdir, this.conf);
      // Insert meta into root region
      HRegion.addRegionToMETA(root, meta);
      // Create the regions
      for (int i = 0; i < REGIONS.length; i++) {
        REGIONS[i] =
          HRegion.createHRegion(this.INFOS[i], this.rootdir, this.conf);
        // Insert data
        for (int j = 0; j < TIMESTAMPS.length; j++) {
          BatchUpdate b = new BatchUpdate(rand.nextLong());
          long id = b.startUpdate(ROWS[i]);
          b.put(id, HConstants.COLUMN_FAMILY, toBytes(TIMESTAMPS[j]));
          REGIONS[i].batchUpdate(TIMESTAMPS[j], b);
        }
        // Insert the region we created into the meta
        HRegion.addRegionToMETA(meta, REGIONS[i]);
        // Close region
        REGIONS[i].close();
        REGIONS[i].getLog().closeAndDelete();
      }

      // Close root and meta regions
      root.close();
      root.getLog().closeAndDelete();
      meta.close();
      meta.getLog().closeAndDelete();
      // Call super.Setup last. Otherwise we get a local file system.
      super.setUp();
    } catch (Exception e) {
      if (dfsCluster != null) {
        StaticTestEnvironment.shutdownDfs(dfsCluster);
        dfsCluster = null;
      }
      throw e;
    }
  }

  /**
   * @throws Exception
   */
  public void testScanMultipleVersions() throws Exception {
    // Now start HBase
    MiniHBaseCluster cluster = new MiniHBaseCluster(conf, 1, dfsCluster, false);
    try {
      // At this point we have created multiple regions and both HDFS and HBase
      // are running. There are 5 cases we have to test. Each is described below.

      HTable t = new HTable(conf, TABLE_NAME);

      // Case 1: scan with LATEST_TIMESTAMP. Should get two rows

      int count = 0;
      HScannerInterface s = t.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY,
          HConstants.EMPTY_START_ROW);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while (s.next(key, results)) {
          count += 1;
        }
        assertEquals("Number of rows should be 2", 2, count);
      } finally {
        s.close();
      }

      // Case 2: Scan with a timestamp greater than most recent timestamp
      // (in this case > 1000 and < LATEST_TIMESTAMP. Should get 2 rows.

      count = 0;
      s = t.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY,
          HConstants.EMPTY_START_ROW, 10000L);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while (s.next(key, results)) {
          count += 1;
        }
        assertEquals("Number of rows should be 2", 2, count);
      } finally {
        s.close();
      }

      // Case 3: scan with timestamp equal to most recent timestamp
      // (in this case == 1000. Should get 2 rows.

      count = 0;
      s = t.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY,
          HConstants.EMPTY_START_ROW, 1000L);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while (s.next(key, results)) {
          count += 1;
        }
        assertEquals("Number of rows should be 2", 2, count);
      } finally {
        s.close();
      }

      // Case 4: scan with timestamp greater than first timestamp but less than
      // second timestamp (100 < timestamp < 1000). Should get 2 rows.

      count = 0;
      s = t.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY,
          HConstants.EMPTY_START_ROW, 500L);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while (s.next(key, results)) {
          count += 1;
        }
        assertEquals("Number of rows should be 2", 2, count);
      } finally {
        s.close();
      }

      // Case 5: scan with timestamp equal to first timestamp (100)
      // Should get 2 rows.

      count = 0;
      s = t.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY,
          HConstants.EMPTY_START_ROW, 100L);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while (s.next(key, results)) {
          count += 1;
        }
        assertEquals("Number of rows should be 2", 2, count);
      } finally {
        s.close();
      }
    } finally {
      cluster.shutdown();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void tearDown() throws Exception {
    if (dfsCluster != null) {
      StaticTestEnvironment.shutdownDfs(dfsCluster);
      dfsCluster = null;
    }
    super.tearDown();
  }
  
  /*
   * Convert a long value to a byte array
   * @param val
   * @return the byte array
   */
  private static byte[] toBytes(final long val) {
    ByteBuffer bb = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
    bb.putLong(val);
    return bb.array();
  }
}
