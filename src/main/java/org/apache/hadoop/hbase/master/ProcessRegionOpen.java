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
package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegionSeqidTransition;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

/**
 * ProcessRegionOpen is instantiated when a region server reports that it is
 * serving a region. This applies to all meta and user regions except the
 * root region which is handled specially.
 */
public class ProcessRegionOpen extends ProcessRegionStatusChange {
  protected final HServerInfo serverInfo;
  protected final HRegionSeqidTransition seqidTransition;

  /**
   * @param master
   * @param info
   * @param regionInfo
   */
  public ProcessRegionOpen(HMaster master, HServerInfo info,
                           HRegionInfo regionInfo) {
    this(master, info, regionInfo, null);
  }

  /**
   * @param master
   * @param info
   * @param regionInfo
   * @param seqidMsg region seqid jump information
   */
  public ProcessRegionOpen(HMaster master, HServerInfo info,
                           HRegionInfo regionInfo, HRegionSeqidTransition seqidTran) {
    super(master, info.getServerName(), regionInfo);
    this.serverInfo = info;
    this.seqidTransition = seqidTran;
  }

  @Override
  public String toString() {
    return "PendingOpenOperation from " + serverInfo.getServerName() +
        " for region " + this.regionInfo.getRegionNameAsString();
  }

  @Override
  protected RegionServerOperationResult process() throws IOException {
    // TODO: The below check is way too convoluted!!!
    if (!metaRegionAvailable()) {
      // We can't proceed unless the meta region we are going to update
      // is online.
      return RegionServerOperationResult.OPERATION_DELAYED;
    }
    writeToMeta(getMetaRegion());
    RegionManager regionManager = master.getRegionManager();
    synchronized (regionManager) {
      if (isMetaTable) {
        // It's a meta region.
        MetaRegion m =
            new MetaRegion(new HServerAddress(serverInfo.getServerAddress()),
                regionInfo);
        // Add it to the online meta regions
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding to onlineMetaRegions: " + m.toString());
        }
        regionManager.putMetaRegionOnline(m);
        // Interrupting the Meta Scanner sleep so that it can
        // process regions right away
        regionManager.metaScannerThread.triggerNow();
      }
      // If updated successfully, remove from pending list if the state
      // is consistent. For example, a disable could be called before the
      // synchronization.
      if (regionManager.isOfflined(regionInfo.getRegionNameAsString())) {
        LOG.warn("We opened a region while it was asked to be closed.");
        ZooKeeperWrapper zkWrapper =
            ZooKeeperWrapper.getInstance(master.getConfiguration(),
                master.getZKWrapperName());
        zkWrapper.deleteUnassignedRegion(regionInfo.getEncodedName());
      } else {
        regionManager.removeRegion(regionInfo);
      }
      return RegionServerOperationResult.OPERATION_SUCCEEDED;
    }
  }

  void writeToMeta(MetaRegion region) throws IOException {
    HRegionInterface server =
        master.getServerConnection().getHRegionConnection(region.getServer());
    String seqidLog = null;
    LOG.info(regionInfo.getRegionNameAsString() + " open on " + serverInfo.getServerName());
    // Register the newly-available Region's location.
    Put p = new Put(regionInfo.getRegionName());
    p.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
        Bytes.toBytes(serverInfo.getHostnamePort()));
    p.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
        Bytes.toBytes(serverInfo.getStartCode()));
    if (seqidTransition != null) {
      // if it is not metaTable, OK
      // if it is metaTable && meta region seqid recording enabled, OK
      // otherwise, the ROOT will not have the historian family, not appending.
      if (isMetaTable &&
          !HTableDescriptor.isMetaregionSeqidRecordEnabled(master.getConfiguration())) {
        seqidLog = ", sequence id of meta region not enabled, not recording.";
      } else {
        StringBuilder sb = new StringBuilder();
        p.add(HConstants.CATALOG_HISTORIAN_FAMILY, HConstants.SERVER_QUALIFIER,
              Bytes.toBytes(serverInfo.getHostnamePort()));
        sb.append(Bytes.toString(HConstants.CATALOG_HISTORIAN_FAMILY))
          .append(":").append(Bytes.toString(HConstants.SERVER_QUALIFIER))
          .append("=").append(serverInfo.getHostnamePort());
        p.add(HConstants.CATALOG_HISTORIAN_FAMILY, HConstants.STARTCODE_QUALIFIER,
              Bytes.toBytes(serverInfo.getStartCode()));
        sb.append(", ").append(Bytes.toString(HConstants.CATALOG_HISTORIAN_FAMILY))
          .append(":").append(Bytes.toString(HConstants.STARTCODE_QUALIFIER))
          .append("=").append(serverInfo.getStartCode());
        p.add(HConstants.CATALOG_HISTORIAN_FAMILY,
              HConstants.LAST_SEQID_QUALIFIER, Bytes.toBytes(seqidTransition.getLastSeqid()));
        sb.append(", ").append(Bytes.toString(HConstants.CATALOG_HISTORIAN_FAMILY))
          .append(":").append(Bytes.toString(HConstants.LAST_SEQID_QUALIFIER)).append("=")
          .append(seqidTransition.getLastSeqid());
        p.add(HConstants.CATALOG_HISTORIAN_FAMILY,
              HConstants.NEXT_SEQID_QUALIFIER, Bytes.toBytes(seqidTransition.getNextSeqid()));
        sb.append(", ").append(Bytes.toString(HConstants.CATALOG_HISTORIAN_FAMILY))
          .append(":").append(Bytes.toString(HConstants.NEXT_SEQID_QUALIFIER)).append("=")
          .append(seqidTransition.getNextSeqid());
        p.add(HConstants.CATALOG_HISTORIAN_FAMILY,
              HConstants.REGIONINFO_QUALIFIER, Writables.getBytes(regionInfo));
        sb.append(", ").append(Bytes.toString(HConstants.CATALOG_HISTORIAN_FAMILY))
          .append(":").append(Bytes.toString(HConstants.REGIONINFO_QUALIFIER)).append("=")
          .append("[regionInfo object of ").append(regionInfo.getRegionNameAsString()).append("]");

        seqidLog = sb.toString();
      }
    }
    server.put(region.getRegionName(), p);
    LOG.info("Updated row " + regionInfo.getRegionNameAsString() + " in region " +
        Bytes.toString(region.getRegionName()) + " with " +
        Bytes.toString(HConstants.CATALOG_FAMILY) + ":startcode=" + serverInfo.getStartCode() +
        ", " + Bytes.toString(HConstants.CATALOG_FAMILY) +
        ":server=" + serverInfo.getHostnamePort() +
        ((seqidLog == null) ? " and NO sequence id transition" : ", " + seqidLog) + ".");
    this.master.getServerManager().getRegionChecker().becameOpened(regionInfo);
  }

  @Override
  protected int getPriority() {
    return 0; // highest priority
  }
}
