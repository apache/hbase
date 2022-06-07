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
package org.apache.hadoop.hbase.master.hbck;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.HbckRegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The result of an {@link HbckChore} execution.
 */
@InterfaceAudience.Private
public class HbckReport {

  private final Map<String, HbckRegionInfo> regionInfoMap = new HashMap<>();
  private final Set<String> disabledTableRegions = new HashSet<>();
  private final Set<String> splitParentRegions = new HashSet<>();
  private final Map<String, ServerName> orphanRegionsOnRS = new HashMap<>();
  private final Map<String, Path> orphanRegionsOnFS = new HashMap<>();
  private final Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
    new HashMap<>();

  private Instant checkingStartTimestamp = null;
  private Instant checkingEndTimestamp = null;

  /**
   * Used for web ui to show when the HBCK checking started.
   */
  public Instant getCheckingStartTimestamp() {
    return checkingStartTimestamp;
  }

  public void setCheckingStartTimestamp(Instant checkingStartTimestamp) {
    this.checkingStartTimestamp = checkingStartTimestamp;
  }

  /**
   * Used for web ui to show when the HBCK checking report generated.
   */
  public Instant getCheckingEndTimestamp() {
    return checkingEndTimestamp;
  }

  public void setCheckingEndTimestamp(Instant checkingEndTimestamp) {
    this.checkingEndTimestamp = checkingEndTimestamp;
  }

  /**
   * This map contains the state of all hbck items. It maps from encoded region name to
   * HbckRegionInfo structure. The information contained in HbckRegionInfo is used to detect and
   * correct consistency (hdfs/meta/deployment) problems.
   */
  public Map<String, HbckRegionInfo> getRegionInfoMap() {
    return regionInfoMap;
  }

  public Set<String> getDisabledTableRegions() {
    return disabledTableRegions;
  }

  public Set<String> getSplitParentRegions() {
    return splitParentRegions;
  }

  /**
   * The regions only opened on RegionServers, but no region info in meta.
   */
  public Map<String, ServerName> getOrphanRegionsOnRS() {
    return orphanRegionsOnRS;
  }

  /**
   * The regions have directory on FileSystem, but no region info in meta.
   */
  public Map<String, Path> getOrphanRegionsOnFS() {
    return orphanRegionsOnFS;
  }

  /**
   * The inconsistent regions. There are three case: case 1. Master thought this region opened, but
   * no regionserver reported it. case 2. Master thought this region opened on Server1, but
   * regionserver reported Server2 case 3. More than one regionservers reported opened this region
   */
  public Map<String, Pair<ServerName, List<ServerName>>> getInconsistentRegions() {
    return inconsistentRegions;
  }
}
