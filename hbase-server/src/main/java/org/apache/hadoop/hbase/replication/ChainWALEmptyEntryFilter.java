/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import java.util.List;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link ChainWALEntryFilter} for providing more flexible options
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class ChainWALEmptyEntryFilter extends ChainWALEntryFilter {

  private boolean filterEmptyEntry = false;

  public ChainWALEmptyEntryFilter(final WALEntryFilter... filters) {
    super(filters);
  }

  public ChainWALEmptyEntryFilter(final List<WALEntryFilter> filters) {
    super(filters);
  }

  @Override
  public WAL.Entry filter(WAL.Entry entry) {
    entry = super.filter(entry);
    if (filterEmptyEntry && entry != null && entry.getEdit().isEmpty()) {
      return null;
    }
    return entry;
  }

  /**
   * To allow the empty entries to get filtered, we want to set this optional flag to decide
   * if we want to filter the entries which have no cells or all cells got filtered
   * though {@link WALCellFilter}.
   *
   * @param filterEmptyEntry flag
   */
  @VisibleForTesting
  public void setFilterEmptyEntry(final boolean filterEmptyEntry) {
    this.filterEmptyEntry = filterEmptyEntry;
  }
}