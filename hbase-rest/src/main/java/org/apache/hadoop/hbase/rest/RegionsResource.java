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
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.rest.model.TableInfoModel;
import org.apache.hadoop.hbase.rest.model.TableRegionModel;

@InterfaceAudience.Private
public class RegionsResource extends ResourceBase {
  private static final Log LOG = LogFactory.getLog(RegionsResource.class);

  static CacheControl cacheControl;
  static {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  TableResource tableResource;

  /**
   * Constructor
   * @param tableResource
   * @throws IOException
   */
  public RegionsResource(TableResource tableResource) throws IOException {
    super();
    this.tableResource = tableResource;
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
      TableName tableName = TableName.valueOf(tableResource.getName());
      TableInfoModel model = new TableInfoModel(tableName.getNameAsString());
      Map<HRegionInfo,ServerName> regions = MetaScanner.allTableRegions(
        servlet.getConfiguration(), null, tableName, false);
      for (Map.Entry<HRegionInfo,ServerName> e: regions.entrySet()) {
        HRegionInfo hri = e.getKey();
        ServerName addr = e.getValue();
        model.add(
          new TableRegionModel(tableName.getNameAsString(), hri.getRegionId(),
            hri.getStartKey(), hri.getEndKey(), addr.getHostAndPort()));
      }
      ResponseBuilder response = Response.ok(model);
      response.cacheControl(cacheControl);
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return response.build();
    } catch (TableNotFoundException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.NOT_FOUND)
        .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
        .build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    }
  }
}
