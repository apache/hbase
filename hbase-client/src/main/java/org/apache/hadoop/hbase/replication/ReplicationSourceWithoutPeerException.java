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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This exception is thrown when the replication source is running with no
 * corresponding peer. This can due to race condition between PeersWatcher
 * zk listerner and source trying to remove the queues, or if zk listener for
 * delete node was never invoked for any reason. See HBASE-25583
 */
@InterfaceAudience.Private
public class ReplicationSourceWithoutPeerException extends ReplicationException {
  private static final long serialVersionUID = 1L;

  public ReplicationSourceWithoutPeerException(String m, Throwable t) {
    super(m, t);
  }

  public ReplicationSourceWithoutPeerException(String m) {
    super(m);
  }
}
