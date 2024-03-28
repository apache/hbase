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
package org.apache.hadoop.hbase.master.http.hbck.model;

import java.util.List;
import org.apache.hadoop.hbase.HbckEmptyRegionInfo;
import org.apache.hadoop.hbase.HbckInconsistentRegions;
import org.apache.hadoop.hbase.HbckOrphanRegionsOnFS;
import org.apache.hadoop.hbase.HbckOrphanRegionsOnRS;
import org.apache.hadoop.hbase.HbckOverlapRegions;
import org.apache.hadoop.hbase.HbckRegionHoles;
import org.apache.hadoop.hbase.HbckUnknownServers;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class exposes hbck.jsp report as JSON Output via /hbck/hbck-metrics API.
 */
@InterfaceAudience.Private
public class HbckMetrics {

  private final long hbckReportStartTime;
  private final long hbckReportEndTime;
  private final List<HbckOrphanRegionsOnFS> hbckOrphanRegionsOnFs;
  private final List<HbckOrphanRegionsOnRS> hbckOrphanRegionsOnRs;
  private final List<HbckInconsistentRegions> hbckInconsistentRegions;
  private final List<HbckRegionHoles> hbckHoles;
  private final List<HbckOverlapRegions> hbckOverlaps;
  private final List<HbckUnknownServers> hbckUnknownServers;
  private final List<HbckEmptyRegionInfo> hbckEmptyRegionInfo;

  public HbckMetrics(long hbckReportStartTime, long hbckReportEndTime,
    List<HbckOrphanRegionsOnFS> hbckOrphanRegionsOnFs,
    List<HbckOrphanRegionsOnRS> hbckOrphanRegionsOnRs,
    List<HbckInconsistentRegions> hbckInconsistentRegions, List<HbckRegionHoles> hbckHoles,
    List<HbckOverlapRegions> hbckOverlaps, List<HbckUnknownServers> hbckUnknownServers,
    List<HbckEmptyRegionInfo> hbckEmptyRegionInfo) {
    this.hbckReportStartTime = hbckReportStartTime;
    this.hbckReportEndTime = hbckReportEndTime;
    this.hbckOrphanRegionsOnFs = hbckOrphanRegionsOnFs;
    this.hbckOrphanRegionsOnRs = hbckOrphanRegionsOnRs;
    this.hbckInconsistentRegions = hbckInconsistentRegions;
    this.hbckHoles = hbckHoles;
    this.hbckOverlaps = hbckOverlaps;
    this.hbckUnknownServers = hbckUnknownServers;
    this.hbckEmptyRegionInfo = hbckEmptyRegionInfo;
  }

  public long gethbckReportStartTime() {
    return hbckReportStartTime;
  }

  public long gethbckReportEndTime() {
    return hbckReportEndTime;
  }

  public List<HbckOrphanRegionsOnFS> gethbckOrphanRegionsOnFs() {
    return hbckOrphanRegionsOnFs;
  }

  public List<HbckOrphanRegionsOnRS> gethbckOrphanRegionsOnRs() {
    return hbckOrphanRegionsOnRs;
  }

  public List<HbckInconsistentRegions> gethbckInconsistentRegions() {
    return hbckInconsistentRegions;
  }

  public List<HbckRegionHoles> gethbckHoles() {
    return hbckHoles;
  }

  public List<HbckOverlapRegions> gethbckOverlaps() {
    return hbckOverlaps;
  }

  public List<HbckUnknownServers> gethbckUnknownServers() {
    return hbckUnknownServers;
  }

  public List<HbckEmptyRegionInfo> gethbckEmptyRegionInfo() {
    return hbckEmptyRegionInfo;
  }
}
