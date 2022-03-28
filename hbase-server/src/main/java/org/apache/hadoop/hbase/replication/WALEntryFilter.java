/*
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
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Filter for WAL entries before being sent over to replication. Multiple
 * filters might be chained together using {@link ChainWALEntryFilter}.
 * Applied on the replication source side.
 * <p>There is also a filter that can be installed on the sink end of a replication stream.
 * See {@link org.apache.hadoop.hbase.replication.regionserver.WALEntrySinkFilter}. Certain
 * use-cases may need such a facility but better to filter here on the source side rather
 * than later, after the edit arrives at the sink.</p>
 * @see org.apache.hadoop.hbase.replication.regionserver.WALEntrySinkFilter for filtering
 * replication on the sink-side.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public interface WALEntryFilter {
  /**
   * <p>
   * Applies the filter, possibly returning a different Entry instance. If null is returned, the
   * entry will be skipped.
   * </p>
   * <p>
   * Notice that you are free to modify the cell list of the give entry, but do not change the
   * content of the cell, it may be used by others at the same time(and usually you can not modify a
   * cell unless you cast it to the implementation class, which is not a good idea).
   * </p>
   * @param entry Entry to filter
   * @return a (possibly modified) Entry to use. Returning null or an entry with no cells will cause
   *         the entry to be skipped for replication.
   */
  Entry filter(Entry entry);
}
