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
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class ReplicaKey {
  private final Pair<ByteArrayWrapper, ByteArrayWrapper> startAndStopKeys;

  public ReplicaKey(RegionInfo regionInfo) {
    this.startAndStopKeys = new Pair<>(new ByteArrayWrapper(regionInfo.getStartKey()),
      new ByteArrayWrapper(regionInfo.getEndKey()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ReplicaKey)) {
      return false;
    }
    ReplicaKey other = (ReplicaKey) o;
    return this.startAndStopKeys.equals(other.startAndStopKeys);
  }

  @Override
  public int hashCode() {
    return startAndStopKeys.hashCode();
  }

  static class ByteArrayWrapper {
    private final byte[] bytes;

    ByteArrayWrapper(byte[] prefix) {
      this.bytes = Arrays.copyOf(prefix, prefix.length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ByteArrayWrapper)) {
        return false;
      }
      ByteArrayWrapper other = (ByteArrayWrapper) o;
      return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }
}
