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
package org.apache.hadoop.hbase.master.http.hbck.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.hbck.HbckReport;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckEmptyRegionInfo;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckInconsistentRegions;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckMetrics;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckOrphanRegionsOnFS;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckOrphanRegionsOnRS;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckOverlapRegions;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckRegionDetails;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckRegionHoles;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckServerName;
import org.apache.hadoop.hbase.master.http.hbck.model.HbckUnknownServers;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitorReport;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

@Path("hbck_metrics")
@Produces({ MediaType.APPLICATION_JSON })
@InterfaceAudience.Private
public class HbckMetricsResource {
  private final HbckReport hbckReport;
  private final CatalogJanitorReport catalogJanitorReport;

  @Inject
  public HbckMetricsResource(MasterServices master) {
    this.hbckReport = master.getHbckChore().getLastReport();
    this.catalogJanitorReport = master.getCatalogJanitor().getLastReport();
  }

  @GET
  public HbckMetrics getBaseHbckMetrics() {
    return new HbckMetrics(hbckReport.getCheckingStartTimestamp().toEpochMilli(),
      hbckReport.getCheckingEndTimestamp().toEpochMilli(), getOrphanRegionsOnFS(),
      getOrphanRegionsOnRS(), getInconsistentRegions(), getRegionChainHoles(),
      getRegionChainOverlap(), getUnknownServers(), getEmptyRegionInfo());
  }

  @GET
  @Path("/orphan_regions_on_fs")
  public List<HbckOrphanRegionsOnFS> getOrphanRegionsOnFS() {
    return hbckReport.getOrphanRegionsOnFS().entrySet().stream()
      .map(obj1 -> new HbckOrphanRegionsOnFS(obj1.getKey(), obj1.getValue().toString()))
      .collect(Collectors.toList());
  }

  @GET
  @Path("/orphan_regions_on_rs")
  public List<HbckOrphanRegionsOnRS> getOrphanRegionsOnRS() {
    return hbckReport.getOrphanRegionsOnRS().entrySet().stream()
      .map(obj1 -> new HbckOrphanRegionsOnRS(obj1.getKey(), parseServerName(obj1.getValue())))
      .collect(Collectors.toList());
  }

  @GET
  @Path("/inconsistent_regions")
  public List<HbckInconsistentRegions> getInconsistentRegions() {
    return hbckReport.getInconsistentRegions().entrySet().stream()
      .map(obj1 -> new HbckInconsistentRegions(obj1.getKey(),
        parseServerName(obj1.getValue().getFirst()), obj1.getValue().getSecond().stream()
          .map(this::parseServerName).collect(Collectors.toList())))
      .collect(Collectors.toList());
  }

  @GET
  @Path("/region_holes")
  public List<HbckRegionHoles> getRegionChainHoles() {
    return catalogJanitorReport.getHoles().stream()
      .map(obj1 -> new HbckRegionHoles(parseRegionInfo(obj1.getFirst()),
        parseRegionInfo(obj1.getSecond())))
      .collect(Collectors.toList());
  }

  @GET
  @Path("/region_overlaps")
  public List<HbckOverlapRegions> getRegionChainOverlap() {
    return catalogJanitorReport.getOverlaps().stream()
      .map(obj1 -> new HbckOverlapRegions(parseRegionInfo(obj1.getFirst()),
        parseRegionInfo(obj1.getSecond())))
      .collect(Collectors.toList());
  }

  @GET
  @Path("/unknown_servers")
  public List<HbckUnknownServers> getUnknownServers() {
    return catalogJanitorReport.getUnknownServers().stream()
      .map(obj1 -> new HbckUnknownServers(parseRegionInfo(obj1.getFirst()),
        parseServerName(obj1.getSecond())))
      .collect(Collectors.toList());
  }

  @GET
  @Path("/empty_regioninfo")
  public List<HbckEmptyRegionInfo> getEmptyRegionInfo() {
    return catalogJanitorReport.getEmptyRegionInfo().stream()
      .map(obj1 -> new HbckEmptyRegionInfo(Bytes.toString(obj1))).collect(Collectors.toList());
  }

  public HbckRegionDetails parseRegionInfo(RegionInfo regionInfo) {
    return new HbckRegionDetails(regionInfo.getEncodedName(),
      regionInfo.getTable().getNameAsString(), new String(regionInfo.getStartKey()),
      new String(regionInfo.getEndKey()));
  }

  public HbckServerName parseServerName(ServerName serverName) {
    return new HbckServerName(serverName.getHostname(), serverName.getPort(),
      serverName.getStartCode());
  }
}
