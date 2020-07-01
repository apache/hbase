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
package org.apache.hadoop.hbase.master.assignment;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * State of Server; list of hosted regions, etc.
 */
@InterfaceAudience.Private
public class ServerStateNode implements Comparable<ServerStateNode> {
  private final Set<RegionStateNode> regions;
  private final ServerName serverName;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private volatile ServerState state = ServerState.ONLINE;

  public ServerStateNode(ServerName serverName) {
    this.serverName = serverName;
    this.regions = ConcurrentHashMap.newKeySet();
  }

  public ServerName getServerName() {
    return serverName;
  }

  public ServerState getState() {
    return state;
  }

  public boolean isInState(final ServerState... expected) {
    boolean expectedState = false;
    if (expected != null) {
      for (int i = 0; i < expected.length; ++i) {
        expectedState |= (state == expected[i]);
      }
    }
    return expectedState;
  }

  void setState(final ServerState state) {
    this.state = state;
  }

  public int getRegionCount() {
    return regions.size();
  }

  public List<RegionInfo> getRegionInfoList() {
    return regions.stream().map(RegionStateNode::getRegionInfo).collect(Collectors.toList());
  }

  public List<RegionInfo> getDefaultMetaRegionInfoList() {
    System.err.println("================" + regions);
    return regions.stream().map(RegionStateNode::getRegionInfo).filter(RegionInfo::isMetaRegion)
      .filter(RegionReplicaUtil::isDefaultReplica).collect(Collectors.toList());
  }

  public List<RegionInfo> getSystemRegionInfoList() {
    return regions.stream().filter(RegionStateNode::isSystemTable)
      .map(RegionStateNode::getRegionInfo).collect(Collectors.toList());
  }

  public void addRegion(final RegionStateNode regionNode) {
    this.regions.add(regionNode);
  }

  public void removeRegion(final RegionStateNode regionNode) {
    this.regions.remove(regionNode);
  }

  public Lock readLock() {
    return lock.readLock();
  }

  public Lock writeLock() {
    return lock.writeLock();
  }

  @Override
  public int compareTo(final ServerStateNode other) {
    return getServerName().compareTo(other.getServerName());
  }

  @Override
  public int hashCode() {
    return getServerName().hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ServerStateNode)) {
      return false;
    }
    return compareTo((ServerStateNode) other) == 0;
  }

  @Override
  public String toString() {
    return getServerName() + "/" + getState() + "/regionCount=" + this.regions.size() +
        "/lock=" + this.lock;
  }
}