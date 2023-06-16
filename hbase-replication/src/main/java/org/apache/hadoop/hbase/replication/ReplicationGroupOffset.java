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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ReplicationGroupOffset {

  public static final ReplicationGroupOffset BEGIN = new ReplicationGroupOffset("", 0L);

  private final String wal;

  private final long offset;

  public ReplicationGroupOffset(String wal, long offset) {
    this.wal = wal;
    this.offset = offset;
  }

  public String getWal() {
    return wal;
  }

  /**
   * A negative value means this file has already been fully replicated out
   */
  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return wal + ":" + offset;
  }

  public static ReplicationGroupOffset parse(String str) {
    int index = str.lastIndexOf(':');
    return new ReplicationGroupOffset(str.substring(0, index),
      Long.parseLong(str.substring(index + 1)));
  }
}
