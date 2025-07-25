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
 * Base class for {@link WALEntryFilter}, store the necessary common properties like
 * {@link #serial}.
 * <p>
 * Why need to treat serial replication specially:
 * <p>
 * Under some special cases, we may filter out some entries but we still need to record the last
 * pushed sequence id for these entries. For example, when we setup a bidirection replication A <->
 * B, if we write to both cluster A and cluster B, cluster A will not replicate the entries which
 * are replicated from cluster B, which means we may have holes in the replication sequence ids. So
 * if the region is closed abnormally, i.e, we do not have a close event for the region, and before
 * the closeing, we have some entries from cluster B, then the replication from cluster A to cluster
 * B will be stuck if we do not record the last pushed sequence id of these entries because we will
 * find out that the previous sequence id range will never finish. So we need to record the sequence
 * id for these entries so the last pushed sequence id can reach the region barrier.
 * @see https://issues.apache.org/jira/browse/HBASE-29463
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public abstract class WALEntryFilterBase implements WALEntryFilter {

  protected boolean serial;

  @Override
  public void setSerial(boolean serial) {
    this.serial = serial;
  }

  /**
   * Call this method when you do not need to replicate the entry.
   * <p>
   * For serial replication, since still need to WALKey for recording progress, we clear all the
   * cells of the WALEdit. For normal replication, we just return null.
   */
  protected final Entry clearOrNull(Entry entry) {
    if (serial) {
      entry.getEdit().getCells().clear();
      return entry;
    } else {
      return null;
    }
  }
}
