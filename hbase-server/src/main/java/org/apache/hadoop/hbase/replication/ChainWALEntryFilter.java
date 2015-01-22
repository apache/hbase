/**
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

package org.apache.hadoop.hbase.replication;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;

/**
 * A {@link WALEntryFilter} which contains multiple filters and applies them
 * in chain order
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class ChainWALEntryFilter implements WALEntryFilter {

  private final WALEntryFilter[] filters;

  public ChainWALEntryFilter(WALEntryFilter...filters) {
    this.filters = filters;
  }

  public ChainWALEntryFilter(List<WALEntryFilter> filters) {
    ArrayList<WALEntryFilter> rawFilters = new ArrayList<WALEntryFilter>(filters.size());
    // flatten the chains
    for (WALEntryFilter filter : filters) {
      if (filter instanceof ChainWALEntryFilter) {
        for (WALEntryFilter f : ((ChainWALEntryFilter) filter).filters) {
          rawFilters.add(f);
        }
      } else {
        rawFilters.add(filter);
      }
    }

    this.filters = rawFilters.toArray(new WALEntryFilter[rawFilters.size()]);
  }

  @Override
  public Entry filter(Entry entry) {
    for (WALEntryFilter filter : filters) {
      if (entry == null) {
        return null;
      }
      entry = filter.filter(entry);
    }
    return entry;
  }

}
