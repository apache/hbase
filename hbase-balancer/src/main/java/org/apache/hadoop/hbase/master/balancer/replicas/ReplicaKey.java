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
package org.apache.hadoop.hbase.master.balancer.replicas;

import java.util.Arrays;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class ReplicaKey {
  private final TableName tableName;
  private final byte[] start;
  private final byte[] stop;

  public ReplicaKey(RegionInfo regionInfo) {
    this.tableName = regionInfo.getTable();
    this.start = regionInfo.getStartKey();
    this.stop = regionInfo.getEndKey();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ReplicaKey other)) {
      return false;
    }
    return Arrays.equals(this.start, other.start) && Arrays.equals(this.stop, other.stop)
      && this.tableName.equals(other.tableName);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(tableName).append(start).append(stop).toHashCode();
  }
}
