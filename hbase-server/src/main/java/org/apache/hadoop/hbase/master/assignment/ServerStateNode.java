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

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * State of Server; list of hosted regions, etc.
 */
@InterfaceAudience.Private
class ServerStateNode implements Comparable<ServerStateNode> {

  private static final class ServerReportEvent extends ProcedureEvent<ServerName> {
    public ServerReportEvent(final ServerName serverName) {
      super(serverName);
    }
  }

  private final ServerReportEvent reportEvent;

  private final Set<RegionStateNode> regions;
  private final ServerName serverName;

  private volatile ServerState state = ServerState.ONLINE;

  public ServerStateNode(final ServerName serverName) {
    this.serverName = serverName;
    this.regions = ConcurrentHashMap.newKeySet();
    this.reportEvent = new ServerReportEvent(serverName);
  }

  public ServerName getServerName() {
    return serverName;
  }

  public ServerState getState() {
    return state;
  }

  public ProcedureEvent<?> getReportEvent() {
    return reportEvent;
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

  public Set<RegionStateNode> getRegions() {
    return regions;
  }

  public int getRegionCount() {
    return regions.size();
  }

  public ArrayList<RegionInfo> getRegionInfoList() {
    ArrayList<RegionInfo> hris = new ArrayList<RegionInfo>(regions.size());
    for (RegionStateNode region : regions) {
      hris.add(region.getRegionInfo());
    }
    return hris;
  }

  public void addRegion(final RegionStateNode regionNode) {
    this.regions.add(regionNode);
  }

  public void removeRegion(final RegionStateNode regionNode) {
    this.regions.remove(regionNode);
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
    return String.format("ServerStateNode(%s)", getServerName());
  }
}