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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Abstract class that performs common operations for
 * @see ProcessRegionClose and @see ProcessRegionOpen
 */
abstract class ProcessRegionStatusChange extends RegionServerOperation {
  protected final boolean isMetaTable;
  protected final boolean isRootTable;
  protected final HRegionInfo regionInfo;
  @SuppressWarnings({"FieldCanBeLocal"})
  private volatile MetaRegion metaRegion = null;
  protected volatile byte[] metaRegionName = null;

  /**
   * @param master the master
   * @param regionInfo region info
   */
  public ProcessRegionStatusChange(HMaster master, String serverName,
      HRegionInfo regionInfo) {
    super(master, serverName);
    this.regionInfo = regionInfo;
    this.isMetaTable = regionInfo.isMetaTable();
    this.isRootTable = regionInfo.isRootRegion();
  }

  protected boolean metaRegionAvailable() {
    boolean available = true;
    if (isRootTable) {
      return true;
    } else if (isMetaTable) {
      // This operation is for the meta table
      if (!rootAvailable()) {
        // But we can't proceed unless the root region is available
        available = false;
      }
    } else {
      // This operation is for a user table. 
      if (!metaTableAvailable()) {
        // The root region has not been scanned or the meta table is not
        // available so we can't proceed.
        available = false;
      }
    }
    return available;
  }

  protected MetaRegion getMetaRegion() {
    if (isMetaTable) {
      // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same regionName()
      this.metaRegionName = HRegionInfo.ROOT_REGIONINFO.getRegionName();
      this.metaRegion = HTableDescriptor.isMetaregionSeqidRecordEnabled(
          master.getConfiguration()) ?
            new MetaRegion(master.getRegionManager().getRootRegionLocation(),
                HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN) :
            new MetaRegion(master.getRegionManager().getRootRegionLocation(),
                HRegionInfo.ROOT_REGIONINFO);
    } else {
      this.metaRegion =
        master.getRegionManager().getFirstMetaRegionForRegion(regionInfo);
      if (this.metaRegion != null) {
        this.metaRegionName = this.metaRegion.getRegionName();
      }
    }
    if (metaRegion == null) {
      throw new NullPointerException("Could not identify meta region: " +
          "isMetaTable=" + isMetaTable + ", regionInfo=" + regionInfo);
    }
    return this.metaRegion;
  }
  
  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }
}
