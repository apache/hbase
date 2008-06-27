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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;

/**
 * Utility class to build a table of multiple regions.
 */
public class MultiRegionTable extends HBaseTestCase {
  private static final Text[] KEYS = {
    null,
    new Text("bbb"),
    new Text("ccc"),
    new Text("ddd"),
    new Text("eee"),
    new Text("fff"),
    new Text("ggg"),
    new Text("hhh"),
    new Text("iii"),
    new Text("jjj"),
    new Text("kkk"),
    new Text("lll"),
    new Text("mmm"),
    new Text("nnn"),
    new Text("ooo"),
    new Text("ppp"),
    new Text("qqq"),
    new Text("rrr"),
    new Text("sss"),
    new Text("ttt"),
    new Text("uuu"),
    new Text("vvv"),
    new Text("www"),
    new Text("xxx"),
    new Text("yyy")
  };
  
  protected final String columnName;
  protected HTableDescriptor desc;
  protected MiniDFSCluster dfsCluster = null;

  /**
   * @param columnName the column to populate.
   */
  public MultiRegionTable(final String columnName) {
    super();
    this.columnName = columnName;
  }
  
  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR,
      this.dfsCluster.getFileSystem().getHomeDirectory().toString());
    
    // Note: we must call super.setUp after starting the mini cluster or
    // we will end up with a local file system
    
    super.setUp();

    try {
      // Create a bunch of regions

      HRegion[] regions = new HRegion[KEYS.length];
      for (int i = 0; i < regions.length; i++) {
        int j = (i + 1) % regions.length;
        regions[i] = createARegion(KEYS[i], KEYS[j]);
      }

      // Now create the root and meta regions and insert the data regions
      // created above into the meta

      HRegion root = HRegion.createHRegion(HRegionInfo.rootRegionInfo,
          testDir, this.conf);
      HRegion meta = HRegion.createHRegion(HRegionInfo.firstMetaRegionInfo,
          testDir, this.conf);
      HRegion.addRegionToMETA(root, meta);

      for(int i = 0; i < regions.length; i++) {
        HRegion.addRegionToMETA(meta, regions[i]);
      }

      closeRegionAndDeleteLog(root);
      closeRegionAndDeleteLog(meta);
    } catch (Exception e) {
      StaticTestEnvironment.shutdownDfs(dfsCluster);
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    StaticTestEnvironment.shutdownDfs(dfsCluster);
  }

  private HRegion createARegion(Text startKey, Text endKey) throws IOException {
    HRegion region = createNewHRegion(desc, startKey, endKey);
    addContent(region, this.columnName);
    closeRegionAndDeleteLog(region);
    return region;
  }
  
  private void closeRegionAndDeleteLog(HRegion region) throws IOException {
    region.close();
    region.getLog().closeAndDelete();
  }
}