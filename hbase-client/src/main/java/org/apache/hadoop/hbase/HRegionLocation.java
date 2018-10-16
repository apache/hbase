/**
 *
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.client.ImmutableHRegionInfo;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Data structure to hold RegionInfo and the address for the hosting
 * HRegionServer.  Immutable.  Comparable, but we compare the 'location' only:
 * i.e. the hostname and port, and *not* the regioninfo.  This means two
 * instances are the same if they refer to the same 'location' (the same
 * hostname and port), though they may be carrying different regions.
 *
 * On a big cluster, each client will have thousands of instances of this object, often
 *  100 000 of them if not million. It's important to keep the object size as small
 *  as possible.
 *
 * <br>This interface has been marked InterfaceAudience.Public in 0.96 and 0.98.
 */
@InterfaceAudience.Public
public class HRegionLocation implements Comparable<HRegionLocation> {
  private final RegionInfo regionInfo;
  private final ServerName serverName;
  private final long seqNum;

  public HRegionLocation(RegionInfo regionInfo, ServerName serverName) {
    this(regionInfo, serverName, HConstants.NO_SEQNUM);
  }

  public HRegionLocation(RegionInfo regionInfo, ServerName serverName, long seqNum) {
    this.regionInfo = regionInfo;
    this.serverName = serverName;
    this.seqNum = seqNum;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "region=" + (this.regionInfo == null ? "null" : this.regionInfo.getRegionNameAsString())
        + ", hostname=" + this.serverName + ", seqNum=" + seqNum;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof HRegionLocation)) {
      return false;
    }
    return this.compareTo((HRegionLocation)o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.serverName.hashCode();
  }

  /**
   *
   * @return Immutable HRegionInfo
   * @deprecated Since 2.0.0. Will remove in 3.0.0. Use {@link #getRegion()}} instead.
   */
  @Deprecated
  public HRegionInfo getRegionInfo(){
    return regionInfo == null ? null : new ImmutableHRegionInfo(regionInfo);
  }

  /**
   * @return regionInfo
   */
  public RegionInfo getRegion(){
    return regionInfo;
  }

  public String getHostname() {
    return this.serverName.getHostname();
  }

  public int getPort() {
    return this.serverName.getPort();
  }

  public long getSeqNum() {
    return seqNum;
  }

  /**
   * @return String made of hostname and port formatted as
   * per {@link Addressing#createHostAndPortStr(String, int)}
   */
  public String getHostnamePort() {
    return Addressing.createHostAndPortStr(this.getHostname(), this.getPort());
  }

  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public int compareTo(HRegionLocation o) {
    return serverName.compareTo(o.getServerName());
  }
}
