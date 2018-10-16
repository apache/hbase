/**
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
package org.apache.hadoop.hbase.client.replication;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used by {@link org.apache.hadoop.hbase.client.Admin#listReplicatedTableCFs()}.
 * The cfs is a map of &lt;ColumnFamily, ReplicationScope>.
 */
@InterfaceAudience.Public
public class TableCFs {
  private final TableName table;
  private final Map<String, Integer> cfs;

  public TableCFs(final TableName table, final Map<String, Integer> cfs) {
    this.table = table;
    this.cfs = cfs;
  }

  public TableName getTable() {
    return this.table;
  }

  public Map<String, Integer> getColumnFamilyMap() {
    return this.cfs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(table.getNameAsString());
    if (!cfs.isEmpty()) {
      sb.append(":");
      sb.append(StringUtils.join(cfs.keySet(), ','));
    }
    return sb.toString();
  }
}
