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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

public class HBaseFsckRepair {

  public static boolean fixDupeAssignment(Configuration conf, HRegionInfo region,
      List<HServerAddress> servers)
  throws IOException {

    // ask if user really really wants to fix this region
    if (!prompt("Region " + region + " with duplicate assignment. ")) {
      return false;
    }

    HRegionInfo actualRegion = new HRegionInfo(region);

    // Clear status in master and zk
    clearInMaster(conf, actualRegion);
    clearInZK(conf, actualRegion);

    // Close region on the servers
    for(HServerAddress server : servers) {
      closeRegion(conf, server, actualRegion);
    }

    // It's unassigned so fix it as such
    fixUnassigned(conf, actualRegion);
    return true;
  }

  public static boolean fixUnassigned(Configuration conf, HRegionInfo region)
  throws IOException {

    HRegionInfo actualRegion = new HRegionInfo(region);

    // ask if user really really wants to fix this region
    if (!prompt("Region " + region + " is not assigned. ")) {
      return false;
    }

    // Clear status in master and zk
    clearInMaster(conf, actualRegion);
    clearInZK(conf, actualRegion);
    
    // Clear assignment in META or ROOT
    clearAssignment(conf, actualRegion);
    return true;
  }
  
  public static int getEstimatedFixTime(Configuration conf)
  throws IOException {
    // Fix Time ~=
    //   META rescan interval (when master notices region is unassigned)
    // + Time to Replay Recovered Edits (flushing HLogs == main bottleneck)

    int metaRescan = conf.getInt("hbase.master.meta.thread.rescanfrequency", 
        60 * 1000);
    // estimate = HLog Size * Max HLogs / Throughput [1 Gbps / 2 == 60MBps]
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);
    long logSize = conf.getLong("hbase.regionserver.hlog.blocksize",
        fs.getDefaultBlockSize()) 
        * conf.getInt("hbase.regionserver.maxlogs", 32);
    int recoverEdits = (int)(logSize / (60*1000*1000));    
    int pad = 1000; // 1 sec pad

    return metaRescan + recoverEdits + pad;
  }

  private static void clearInMaster(Configuration conf, HRegionInfo region)
  throws IOException {
    System.out.println("Region being cleared in master: " + region);
    HMasterInterface master = HConnectionManager.getConnection(conf).getMaster();
    long masterVersion =
      master.getProtocolVersion("org.apache.hadoop.hbase.ipc.HMasterInterface", 25);
    System.out.println("Master protocol version: " + masterVersion);
    try {
      master.clearFromTransition(region);
    } catch (Exception e) {}
  }

  private static void clearInZK(Configuration conf, HRegionInfo region)
  throws IOException {
    ZooKeeperWrapper zkw = HConnectionManager.getConnection(conf).getZooKeeperWrapper();
//    try {
      zkw.deleteUnassignedRegion(region.getEncodedName());
//    } catch(KeeperException ke) {}
  }

  private static void closeRegion(Configuration conf, HServerAddress server,
      HRegionInfo region)
  throws IOException {
    HRegionInterface rs = 
      HConnectionManager.getConnection(conf).getHRegionConnection(server);
    rs.closeRegion(region, false);
  }

  private static void clearAssignment(Configuration conf,
      HRegionInfo region)
  throws IOException {
    HTable ht = null;
    if (region.isMetaTable()) {
      // Clear assignment in ROOT
      ht = new HTable(conf, HConstants.ROOT_TABLE_NAME);
    }
    else {
      // Clear assignment in META
      ht = new HTable(conf, HConstants.META_TABLE_NAME);
    }
    Delete del = new Delete(region.getRegionName());
    del.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    del.deleteColumns(HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER);
    ht.delete(del);
  }

  /**
   * Ask the user whether we should fix this problem.
   * Returns true if we should continue to fix the problem.
   */
  private static boolean prompt(String msg) throws IOException {
    // if the user has already specified "yes" to all prompt questions,
    // short circuit this test.
    if (HBaseFsck.getPromptResponse()) {
      return true;
    }
    int inChar;
    while (true) {
      System.out.println(msg + "Fix(y/n):");
      inChar = System.in.read();
      if (inChar == 'n' || inChar == 'N') {
        System.out.println("Not fixing " + msg);
        return false;
      } else if (inChar == 'y' || inChar == 'Y') {
        return true;
      } else if (inChar == -1) {
        throw new IOException("Lost interactive session");
      }
    }
  }
}
