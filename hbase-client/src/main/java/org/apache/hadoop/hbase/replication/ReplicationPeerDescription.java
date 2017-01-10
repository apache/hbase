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
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * The POJO equivalent of ReplicationProtos.ReplicationPeerDescription
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ReplicationPeerDescription {

  private final String id;
  private final boolean enabled;
  private final ReplicationPeerConfig config;

  public ReplicationPeerDescription(String id, boolean enabled, ReplicationPeerConfig config) {
    this.id = id;
    this.enabled = enabled;
    this.config = config;
  }

  public String getPeerId() {
    return this.id;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public ReplicationPeerConfig getPeerConfig() {
    return this.config;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("id : ").append(id);
    builder.append(", enabled : " + enabled);
    builder.append(", config : " + config);
    return builder.toString();
  }
}