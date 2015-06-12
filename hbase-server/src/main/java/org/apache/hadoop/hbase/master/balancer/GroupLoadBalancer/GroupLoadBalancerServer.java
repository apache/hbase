/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer.grouploadbalancer;

import org.apache.hadoop.hbase.ServerName;

/**
 * This object holds the ServerName object which it represents, and the group which it belongs to
 */
public class GroupLoadBalancerServer {

  private ServerName serverName;
  private String serverNameString;
  private String groupServerBelongsTo;

  public GroupLoadBalancerServer(String serverNameString, String groupServerBelongsTo) {
    this.serverNameString = serverNameString;
    this.groupServerBelongsTo = groupServerBelongsTo;
  }

  /**
   * @return the group which the server belongs to as a string
   */
  public String getGroupServerBelongsTo() {
    return this.groupServerBelongsTo;
  }

  /**
   * @return the ServerName object which this object relates to
   */
  public ServerName getServerName() {
    return this.serverName;
  }

  /**
   * @return the server name as a string
   */
  public String getServerNameString() {
    return this.serverNameString;
  }

  public void setGroupServerBelongsTo(String groupServerBelongsTo) {
    this.groupServerBelongsTo = groupServerBelongsTo;
  }

  /**
   * Set the name of the server. This string is later used to be matched up with a ServerName object
   * @param serverName the name of the server
   */
  public void setServerName(ServerName serverName) {
    this.serverName = serverName;
  }

  /**
   * @return a description of the serer including the ServerName object which it is related to,
   * its name as a string, and the name of the group which is belongs to
   */
  public String toString() {
    return "{serverName: " + serverName + ", serverNameString: " + this.serverNameString +
        ", groupServerBelongsTo: " + this.groupServerBelongsTo + "}";
  }
}
