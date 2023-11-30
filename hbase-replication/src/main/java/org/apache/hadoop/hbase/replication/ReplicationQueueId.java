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

import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ReplicationQueueId {

  private final ServerName serverName;

  private final String peerId;

  private final Optional<ServerName> sourceServerName;

  // we do not allow '-' in peer names so it is safe to use it as the separator for peer id and
  // server name
  private static final char PEER_ID_SEPARATOR = '-';

  // The '/' character is not valid for a hostname or a nodename(FQDN, so it is safe to use it as
  // the separator for server names)
  private static final char SERVER_NAME_SEPARATOR = '/';

  public ReplicationQueueId(ServerName serverName, String peerId) {
    this.serverName = Objects.requireNonNull(serverName);
    this.peerId = Objects.requireNonNull(peerId);
    this.sourceServerName = Optional.empty();
  }

  public ReplicationQueueId(ServerName serverName, String peerId, ServerName sourceServerName) {
    this.serverName = Objects.requireNonNull(serverName);
    this.peerId = Objects.requireNonNull(peerId);
    this.sourceServerName = Optional.of(sourceServerName);
  }

  public ServerName getServerName() {
    return serverName;
  }

  public String getPeerId() {
    return peerId;
  }

  public Optional<ServerName> getSourceServerName() {
    return sourceServerName;
  }

  public ServerName getServerWALsBelongTo() {
    return sourceServerName.orElse(serverName);
  }

  public boolean isRecovered() {
    return sourceServerName.isPresent();
  }

  public ReplicationQueueId claim(ServerName targetServerName) {
    ServerName newSourceServerName = sourceServerName.orElse(serverName);
    return new ReplicationQueueId(targetServerName, peerId, newSourceServerName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(peerId, serverName, sourceServerName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ReplicationQueueId)) {
      return false;
    }
    ReplicationQueueId other = (ReplicationQueueId) obj;
    return Objects.equals(peerId, other.peerId) && Objects.equals(serverName, other.serverName)
      && Objects.equals(sourceServerName, other.sourceServerName);
  }

  @Override
  public String toString() {
    StringBuilder sb =
      new StringBuilder().append(peerId).append(PEER_ID_SEPARATOR).append(serverName);
    sourceServerName.ifPresent(s -> sb.append(SERVER_NAME_SEPARATOR).append(s.toString()));
    return sb.toString();
  }

  public static ReplicationQueueId parse(String str) {
    int dashIndex = str.indexOf(PEER_ID_SEPARATOR);
    String peerId = str.substring(0, dashIndex);
    int slashIndex = str.indexOf(SERVER_NAME_SEPARATOR, dashIndex + 1);
    if (slashIndex < 0) {
      String serverName = str.substring(dashIndex + 1);
      return new ReplicationQueueId(ServerName.valueOf(serverName), peerId);
    } else {
      String serverName = str.substring(dashIndex + 1, slashIndex);
      String sourceServerName = str.substring(slashIndex + 1);
      return new ReplicationQueueId(ServerName.valueOf(serverName), peerId,
        ServerName.valueOf(sourceServerName));
    }
  }

  public static String getPeerId(String str) {
    int dashIndex = str.indexOf(PEER_ID_SEPARATOR);
    return str.substring(0, dashIndex);
  }

  public static byte[] getScanPrefix(ServerName serverName, String peerId) {
    return Bytes.toBytes(peerId + PEER_ID_SEPARATOR + serverName.toString());
  }

  public static byte[] getScanPrefix(String peerId) {
    return Bytes.toBytes(peerId + PEER_ID_SEPARATOR);
  }

  private static char getNextChar(char c) {
    return (char) ((int) c + 1);
  }

  public static byte[] getScanStartRowForNextPeerId(String peerId) {
    return Bytes.toBytes(peerId + getNextChar(PEER_ID_SEPARATOR));
  }
}
