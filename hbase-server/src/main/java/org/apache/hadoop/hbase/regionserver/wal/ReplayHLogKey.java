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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;

/**
 * An HLogKey specific to WalEdits coming from replay.
 */
@InterfaceAudience.Private
public class ReplayHLogKey extends HLogKey {

  public ReplayHLogKey(final byte [] encodedRegionName, final TableName tablename,
      final long now, List<UUID> clusterIds, long nonceGroup, long nonce) {
    super(encodedRegionName, tablename, now, clusterIds, nonceGroup, nonce);
  }

  public ReplayHLogKey(final byte [] encodedRegionName, final TableName tablename,
      long logSeqNum, final long now, List<UUID> clusterIds, long nonceGroup, long nonce) {
    super(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce);
  }

  /**
   * Returns the original sequence id
   * @return long the new assigned sequence number
   * @throws InterruptedException
   */
  @Override
  public long getSequenceId() throws IOException {
    return this.getOrigLogSeqNum();
  }
}
