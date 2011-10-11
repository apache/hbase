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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Instantiated to remove a column family from a table */
class DeleteColumn extends ColumnOperation {
  private final byte [] columnName;
  private static final Log LOG = LogFactory.getLog(DeleteColumn.class);

  DeleteColumn(final HMaster master, final byte [] tableName,
    final byte [] columnName)
  throws IOException {
    super(master, tableName);
    this.columnName = columnName;
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    Set<HRegionInfo> regionsToReopen = new HashSet<HRegionInfo>();
    for (HRegionInfo i: regionsToProcess) {
      i.getTableDesc().removeFamily(columnName);
      updateRegionInfo(server, m.getRegionName(), i);
      // Delete the directories used by the column
      Path tabledir =
        new Path(this.master.getRootDir(), i.getTableDesc().getNameAsString());
      this.master.getFileSystem().
        delete(Store.getStoreHomedir(tabledir, i.getEncodedName(),
        this.columnName), true);
      // Ignore regions that are split or disabled,
      // as we do not want to reopen them
      if (!(i.isSplit() || i.isOffline())) {
        regionsToReopen.add(i);
      }
    }
    if (regionsToReopen.size() > 0) {
      this.master.getRegionManager().getThrottledReopener(Bytes.toString(tableName)).
      addRegionsToReopen(regionsToReopen);
    }
  }
  @Override
  protected void processScanItem(String serverName, final HRegionInfo info)
      throws IOException {
    if (isEnabled(info)) {
      LOG.debug("Performing online schema change (region not disabled): "
          + info.getRegionNameAsString());
    }
  }
}