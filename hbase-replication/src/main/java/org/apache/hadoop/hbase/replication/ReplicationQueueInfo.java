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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for the parsing logic for a queue id representing a queue. It will
 * extract the peerId if it's recovered as well as the dead region servers that were part of the
 * queue's history.
 */
@InterfaceAudience.Private
public class ReplicationQueueInfo {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationQueueInfo.class);

  private final String peerId;
  private final String queueId;
  private boolean queueRecovered;
  // List of all the dead region servers that had this queue (if recovered)
  private List<ServerName> deadRegionServers = new ArrayList<>();

  /**
   * The passed queueId will be either the id of the peer or the handling story of that queue in the
   * form of id-servername-*
   */
  public ReplicationQueueInfo(String queueId) {
    this.queueId = queueId;
    String[] parts = queueId.split("-", 2);
    this.queueRecovered = parts.length != 1;
    this.peerId = this.queueRecovered ? parts[0] : queueId;
    if (parts.length >= 2) {
      // extract dead servers
      extractDeadServersFromZNodeString(parts[1], this.deadRegionServers);
    }
  }

  /**
   * Parse dead server names from queue id. servername can contain "-" such as
   * "ip-10-46-221-101.ec2.internal", so we need skip some "-" during parsing for the following
   * cases: 2-ip-10-46-221-101.ec2.internal,52170,1364333181125-&lt;server name>-...
   */
  private static void extractDeadServersFromZNodeString(String deadServerListStr,
    List<ServerName> result) {

    if (deadServerListStr == null || result == null || deadServerListStr.isEmpty()) return;

    // valid server name delimiter "-" has to be after "," in a server name
    int seenCommaCnt = 0;
    int startIndex = 0;
    int len = deadServerListStr.length();

    for (int i = 0; i < len; i++) {
      switch (deadServerListStr.charAt(i)) {
        case ',':
          seenCommaCnt += 1;
          break;
        case '-':
          if (seenCommaCnt >= 2) {
            if (i > startIndex) {
              String serverName = deadServerListStr.substring(startIndex, i);
              if (ServerName.isFullServerName(serverName)) {
                result.add(ServerName.valueOf(serverName));
              } else {
                LOG.error("Found invalid server name:" + serverName);
              }
              startIndex = i + 1;
            }
            seenCommaCnt = 0;
          }
          break;
        default:
          break;
      }
    }

    // add tail
    if (startIndex < len - 1) {
      String serverName = deadServerListStr.substring(startIndex, len);
      if (ServerName.isFullServerName(serverName)) {
        result.add(ServerName.valueOf(serverName));
      } else {
        LOG.error("Found invalid server name at the end:" + serverName);
      }
    }

    LOG.debug("Found dead servers:" + result);
  }

  public List<ServerName> getDeadRegionServers() {
    return Collections.unmodifiableList(this.deadRegionServers);
  }

  public String getPeerId() {
    return this.peerId;
  }

  public String getQueueId() {
    return this.queueId;
  }

  public boolean isQueueRecovered() {
    return queueRecovered;
  }
}
