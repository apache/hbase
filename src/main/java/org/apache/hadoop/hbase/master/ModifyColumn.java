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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

/** Instantiated to modify an existing column family on a table */
class ModifyColumn extends ColumnOperation {
  private final HColumnDescriptor descriptor;
  private final byte [] columnName;
  private static final Log LOG = LogFactory.getLog(ModifyColumn.class);

  ModifyColumn(final HMaster master, final byte [] tableName,
    final byte [] columnName, HColumnDescriptor descriptor)
  throws IOException {
    super(master, tableName);
    this.descriptor = descriptor;
    this.columnName = columnName;
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    Set<HRegionInfo> regionsToReopen = new HashSet<HRegionInfo>();
    for (HRegionInfo i: regionsToProcess) {
      if (i.getTableDesc().hasFamily(columnName)) {
        i.getTableDesc().addFamily(descriptor);
        updateRegionInfo(server, m.getRegionName(), i);
        // Ignore regions that are split or disabled,
        // as we do not want to reopen them
        if (!(i.isSplit() || i.isOffline())) {
          regionsToReopen.add(i);
        }
      } else { // otherwise, we have an error.
        throw new InvalidColumnNameException("Column family '" +
          Bytes.toString(columnName) +
          "' doesn't exist, so cannot be modified.");
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
