/*
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

package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.rest.model.StorageClusterStatusModel;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class StorageClusterStatusResource extends ResourceBase {
  private static final Logger LOG =
    LoggerFactory.getLogger(StorageClusterStatusResource.class);

  static CacheControl cacheControl;
  static {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  /**
   * Constructor
   * @throws IOException
   */
  public StorageClusterStatusResource() throws IOException {
    super();
  }

  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response get(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      ClusterMetrics status = servlet.getAdmin().getClusterMetrics(
        EnumSet.of(Option.LIVE_SERVERS, Option.DEAD_SERVERS));
      StorageClusterStatusModel model = new StorageClusterStatusModel();
      model.setRegions(status.getRegionCount());
      model.setRequests(status.getRequestCount());
      model.setAverageLoad(status.getAverageLoad());
      for (Map.Entry<ServerName, ServerMetrics> entry: status.getLiveServerMetrics().entrySet()) {
        ServerName sn = entry.getKey();
        ServerMetrics load = entry.getValue();
        StorageClusterStatusModel.Node node =
          model.addLiveNode(
            sn.getHostname() + ":" +
            Integer.toString(sn.getPort()),
            sn.getStartcode(), (int) load.getUsedHeapSize().get(Size.Unit.MEGABYTE),
            (int) load.getMaxHeapSize().get(Size.Unit.MEGABYTE));
        node.setRequests(load.getRequestCount());
        for (RegionMetrics region: load.getRegionMetrics().values()) {
          node.addRegion(region.getRegionName(), region.getStoreCount(),
            region.getStoreFileCount(),
            (int) region.getStoreFileSize().get(Size.Unit.MEGABYTE),
            (int) region.getMemStoreSize().get(Size.Unit.MEGABYTE),
            (long) region.getStoreFileIndexSize().get(Size.Unit.KILOBYTE),
            region.getReadRequestCount(),
            region.getWriteRequestCount(),
            (int) region.getStoreFileRootLevelIndexSize().get(Size.Unit.KILOBYTE),
            (int) region.getStoreFileUncompressedDataIndexSize().get(Size.Unit.KILOBYTE),
            (int) region.getBloomFilterSize().get(Size.Unit.KILOBYTE),
            region.getCompactingCellCount(),
            region.getCompactedCellCount());
        }
      }
      for (ServerName name: status.getDeadServerNames()) {
        model.addDeadNode(name.toString());
      }
      ResponseBuilder response = Response.ok(model);
      response.cacheControl(cacheControl);
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return response.build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    }
  }
}
