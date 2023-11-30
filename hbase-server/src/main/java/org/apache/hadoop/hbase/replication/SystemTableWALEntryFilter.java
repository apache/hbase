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

import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Skips WAL edits for all System tables including hbase:meta except hbase:acl. As of now, only 2
 * tables can be allowed for replication - hbase:acl and hbase:labels. Other tables which not be
 * replicated are 1. hbase:meta - not to be replicated 2. hbase:canary - for current cluster 3.
 * hbase:namespace - Deprecated and moved to meta 4. hbase:quota - related to namespace, quota for
 * the current cluster usage 5. hbase:rsgroup - contains hostnames
 */
@InterfaceAudience.Private
public class SystemTableWALEntryFilter implements WALEntryFilter {
  @Override
  public Entry filter(Entry entry) {
    if (
      entry.getKey().getTableName().equals(PermissionStorage.ACL_TABLE_NAME)
        || entry.getKey().getTableName().equals(VisibilityConstants.LABELS_TABLE_NAME)
    ) {
      return entry;
    }
    return entry.getKey().getTableName().isSystemTable() ? null : entry;
  }
}
