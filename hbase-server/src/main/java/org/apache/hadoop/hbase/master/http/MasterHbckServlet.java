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
package org.apache.hadoop.hbase.master.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.HbckChore;
import org.apache.hadoop.hbase.master.http.gson.GsonFactory;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor;
import org.apache.hadoop.hbase.master.janitor.Report;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.Gson;

@InterfaceAudience.Private
public class MasterHbckServlet extends HttpServlet {

  public static final String START_TIMESTAMP = "start_timestamp";
  public static final String END_TIMESTAMP = "end_timestamp";
  public static final String INCONSISTENT_REGIONS = "inconsistent_regions";
  public static final String ORPHAN_REGIONS_ON_RS = "orphan_regions_on_rs";
  public static final String ORPHAN_REGIONS_ON_FS = "orphan_regions_on_fs";
  public static final String HOLES = "holes";
  public static final String OVERLAPS = "overlaps";
  public static final String UNKNOWN_SERVERS = "unknown_servers";
  public static final String EMPTY_REGIONINFO = "empty_regioninfo";

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MasterHbckServlet.class);
  private static final Gson GSON = GsonFactory.buildGson();

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
    if (!master.isInitialized()) {
      LOG.warn("Master is not initialized yet");
      sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
        "master is not initialized yet");
      return;
    }
    final HbckChore hbckChore = master.getHbckChore();
    if (hbckChore == null || hbckChore.isDisabled()) {
      LOG.warn("Hbck chore is disabled");
      sendError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Hbck chore is disabled");
      return;
    }
    if (!Boolean.parseBoolean(request.getParameter("cache"))) {
      try {
        master.getMasterRpcServices().runHbckChore(null, null);
      } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException se) {
        LOG.warn("Failed generating a new hbck chore report; using cache", se);
      }
      try {
        master.getMasterRpcServices().runCatalogScan(null, null);
      } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException se) {
        LOG.warn("Failed generating a new catalogjanitor report; using cache", se);
      }
    }
    Map<String, Object> result = new HashMap<>();
    result.put(START_TIMESTAMP, hbckChore.getCheckingStartTimestamp());
    result.put(END_TIMESTAMP, hbckChore.getCheckingEndTimestamp());
    final Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
      hbckChore.getInconsistentRegions();
    if (inconsistentRegions != null && !inconsistentRegions.isEmpty()) {
      result.put(INCONSISTENT_REGIONS, inconsistentRegions);
    }
    final Map<String, ServerName> orphanRegionsOnRS = hbckChore.getOrphanRegionsOnRS();
    if (orphanRegionsOnRS != null && !orphanRegionsOnRS.isEmpty()) {
      result.put(ORPHAN_REGIONS_ON_RS, orphanRegionsOnRS);
    }
    final Map<String, Path> orphanRegionsOnFS = hbckChore.getOrphanRegionsOnFS();
    if (orphanRegionsOnFS != null && !orphanRegionsOnFS.isEmpty()) {
      result.put(ORPHAN_REGIONS_ON_FS, orphanRegionsOnFS);
    }
    final CatalogJanitor janitor = master.getCatalogJanitor();
    if (janitor != null) {
      final Report report = janitor.getLastReport();
      if (report != null && !report.isEmpty()) {
        List<Pair<RegionInfo, RegionInfo>> holes = report.getHoles();
        if (holes != null && !holes.isEmpty()) {
          result.put(HOLES, holes);
        }
        List<Pair<RegionInfo, RegionInfo>> overlaps = report.getOverlaps();
        if (overlaps != null && !overlaps.isEmpty()) {
          result.put(OVERLAPS, overlaps);
        }
        List<Pair<RegionInfo, ServerName>> unknownServers = report.getUnknownServers();
        if (unknownServers != null && !unknownServers.isEmpty()) {
          result.put(UNKNOWN_SERVERS, unknownServers);
        }
        List<byte[]> emptyRegionInfo = report.getEmptyRegionInfo();
        if (!emptyRegionInfo.isEmpty()) {
          result.put(EMPTY_REGIONINFO, emptyRegionInfo);
        }
      }
    }
    response.setContentType("application/json");
    PrintWriter out = response.getWriter();
    out.write(GSON.toJson(result));
    out.write('\n');
  }

  private static void sendError(HttpServletResponse response, int code, String message)
    throws IOException {
    response.setContentType("application/json");
    Map<String, Object> result = new HashMap<>();
    result.put("error", message);
    response.setStatus(code);
    PrintWriter out = response.getWriter();
    out.write(GSON.toJson(result));
    out.write('\n');
  }

}
