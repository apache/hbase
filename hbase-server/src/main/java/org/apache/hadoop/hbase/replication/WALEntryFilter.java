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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

/**
 * A Filter for WAL entries before being sent over to replication. Multiple
 * filters might be chained together using {@link ChainWALEntryFilter}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public interface WALEntryFilter {

  /**
   * Applies the filter, possibly returning a different HLog.Entry instance.
   * If null is returned, the entry will be skipped.
   * @param entry WAL Entry to filter
   * @return a (possibly modified) HLog.Entry to use. Returning null or an entry with
   * no cells will cause the entry to be skipped for replication.
   */
  public HLog.Entry filter(HLog.Entry entry);

}
