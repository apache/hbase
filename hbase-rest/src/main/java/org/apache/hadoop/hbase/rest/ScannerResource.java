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

package org.apache.hadoop.hbase.rest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.rest.model.ScannerModel;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.PUT;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.core.UriBuilder;
import org.apache.hbase.thirdparty.javax.ws.rs.core.UriInfo;

@InterfaceAudience.Private
public class ScannerResource extends ResourceBase {

  private static final Logger LOG = LoggerFactory.getLogger(ScannerResource.class);

  static final Map<String,ScannerInstanceResource> scanners =
   Collections.synchronizedMap(new HashMap<String,ScannerInstanceResource>());

  TableResource tableResource;

  /**
   * Constructor
   * @param tableResource
   * @throws IOException
   */
  public ScannerResource(TableResource tableResource)throws IOException {
    super();
    this.tableResource = tableResource;
  }

  static boolean delete(final String id) {
    ScannerInstanceResource instance = scanners.remove(id);
    if (instance != null) {
      instance.generator.close();
      return true;
    } else {
      return false;
    }
  }

  Response update(final ScannerModel model, final boolean replace,
      final UriInfo uriInfo) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    byte[] endRow = model.hasEndRow() ? model.getEndRow() : null;
    RowSpec spec = null;
    if (model.getLabels() != null) {
      spec = new RowSpec(model.getStartRow(), endRow, model.getColumns(), model.getStartTime(),
          model.getEndTime(), model.getMaxVersions(), model.getLabels());
    } else {
      spec = new RowSpec(model.getStartRow(), endRow, model.getColumns(), model.getStartTime(),
          model.getEndTime(), model.getMaxVersions());
    }

    try {
      Filter filter = ScannerResultGenerator.buildFilterFromModel(model);
      String tableName = tableResource.getName();
      ScannerResultGenerator gen =
        new ScannerResultGenerator(tableName, spec, filter, model.getCaching(),
          model.getCacheBlocks(), model.getLimit());
      String id = gen.getID();
      ScannerInstanceResource instance =
        new ScannerInstanceResource(tableName, id, gen, model.getBatch());
      scanners.put(id, instance);
      if (LOG.isTraceEnabled()) {
        LOG.trace("new scanner: " + id);
      }
      UriBuilder builder = uriInfo.getAbsolutePathBuilder();
      URI uri = builder.path(id).build();
      servlet.getMetrics().incrementSucessfulPutRequests(1);
      return Response.created(uri).build();
    } catch (Exception e) {
      LOG.error("Exception occurred while processing " + uriInfo.getAbsolutePath() + " : ", e);
      servlet.getMetrics().incrementFailedPutRequests(1);
      if (e instanceof TableNotFoundException) {
        return Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
          .build();
      } else if (e instanceof RuntimeException
          || e instanceof JsonMappingException | e instanceof JsonParseException) {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    }
  }

  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response put(final ScannerModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("PUT " + uriInfo.getAbsolutePath());
    }
    return update(model, true, uriInfo);
  }

  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response post(final ScannerModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("POST " + uriInfo.getAbsolutePath());
    }
    return update(model, false, uriInfo);
  }

  @Path("{scanner: .+}")
  public ScannerInstanceResource getScannerInstanceResource(
      final @PathParam("scanner") String id) throws IOException {
    ScannerInstanceResource instance = scanners.get(id);
    if (instance == null) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return new ScannerInstanceResource();
    } else {
      servlet.getMetrics().incrementSucessfulGetRequests(1);
    }
    return instance;
  }
}
