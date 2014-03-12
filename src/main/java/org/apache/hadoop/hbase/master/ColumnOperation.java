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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import java.io.IOException;

abstract class ColumnOperation extends TableOperation {
  private final Log LOG = LogFactory.getLog(this.getClass());
  
  protected ColumnOperation(final HMaster master, final byte [] tableName)
  throws IOException {
    super(master, tableName);
  }

  @Override
  protected void processScanItem(String serverName, final HRegionInfo info)
      throws IOException {
    if (isEnabled(info)) {
      LOG.debug("Performing online schema change (region not disabled): "
          + info.getRegionNameAsString());
    }
  }

  protected void updateRegionInfo(HRegionInterface server, byte [] regionName,
    HRegionInfo i) throws IOException {
    Put put = new Put(i.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, Writables.getBytes(i));
    server.put(regionName, put);
    if (LOG.isDebugEnabled()) {
      LOG.debug("updated columns in row: " + i.getRegionNameAsString());
    }
  }

  /**
   * Simply updates the given table descriptor with the relevant changes 
   * for the given column operation
   * @param desc  The descriptor that will be updated.
   */
  protected abstract void updateTableDescriptor(HTableDescriptor desc)
    throws IOException;

  /**
   * Is run after META has been updated. Defaults to doing nothing, but 
   * some operations make use of this for housekeeping operations.
   * @param hri Info for the region that has just been updated.
   */
  protected void postProcess(HRegionInfo hri) throws IOException {
    // do nothing by default
  }

  /**
   * Contains all of the logic for updating meta for each type of possible 
   * schema change. By implementing the logic at this level, we are able to 
   * easily prevent excess writes to META
   * @param m the region
   */
  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    Set<HRegionInfo> regionsToReopen = new HashSet<HRegionInfo>();
    for (HRegionInfo i: regionsToProcess) {
      // All we need to do to change the schema is modify the table descriptor.
      // When the region is brought on-line, it will find the changes and
      // update itself accordingly.
      updateTableDescriptor(i.getTableDesc());
      updateRegionInfo(server, m.getRegionName(), i);
      // TODO: queue this to occur after reopening region
      postProcess(i);
      // Ignore regions that are split or disabled, 
      // as we do not want to reopen them  
      if (!(i.isSplit() || i.isOffline())) {
        regionsToReopen.add(i);
      }
    }
    if (regionsToReopen.size() > 0) {
      this.master.getRegionManager().getThrottledReopener(
          Bytes.toString(tableName)).addRegionsToReopen(regionsToReopen);
    }
  }
}
