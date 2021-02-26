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
import java.util.Base64;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class ScannerInstanceResource extends ResourceBase {
  private static final Logger LOG =
    LoggerFactory.getLogger(ScannerInstanceResource.class);

  static CacheControl cacheControl;
  static {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  ResultGenerator generator = null;
  String id = null;
  int batch = 1;

  public ScannerInstanceResource() throws IOException { }

  public ScannerInstanceResource(String table, String id,
      ResultGenerator generator, int batch) throws IOException {
    this.id = id;
    this.generator = generator;
    this.batch = batch;
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response get(final @Context UriInfo uriInfo,
      @QueryParam("n") int maxRows, final @QueryParam("c") int maxValues) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    if (generator == null) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.NOT_FOUND)
        .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
        .build();
    } else {
      // Updated the connection access time for each client next() call
      RESTServlet.getInstance().getConnectionCache().updateConnectionAccessTime();
    }
    CellSetModel model = new CellSetModel();
    RowModel rowModel = null;
    byte[] rowKey = null;
    int limit = batch;
    if (maxValues > 0) {
      limit = maxValues;
    }
    int count = limit;
    do {
      Cell value = null;
      try {
        value = generator.next();
      } catch (IllegalStateException e) {
        if (ScannerResource.delete(id)) {
          servlet.getMetrics().incrementSucessfulDeleteRequests(1);
        } else {
          servlet.getMetrics().incrementFailedDeleteRequests(1);
        }
        servlet.getMetrics().incrementFailedGetRequests(1);
        return Response.status(Response.Status.GONE)
          .type(MIMETYPE_TEXT).entity("Gone" + CRLF)
          .build();
      } catch (IllegalArgumentException e) {
        Throwable t = e.getCause();
        if (t instanceof TableNotFoundException) {
          return Response.status(Response.Status.NOT_FOUND)
              .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
              .build();
        }
        throw e;
      }
      if (value == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("generator exhausted");
        }
        // respond with 204 (No Content) if an empty cell set would be
        // returned
        if (count == limit) {
          return Response.noContent().build();
        }
        break;
      }
      if (rowKey == null) {
        rowKey = CellUtil.cloneRow(value);
        rowModel = new RowModel(rowKey);
      }
      if (!Bytes.equals(CellUtil.cloneRow(value), rowKey)) {
        // if maxRows was given as a query param, stop if we would exceed the
        // specified number of rows
        if (maxRows > 0) {
          if (--maxRows == 0) {
            generator.putBack(value);
            break;
          }
        }
        model.addRow(rowModel);
        rowKey = CellUtil.cloneRow(value);
        rowModel = new RowModel(rowKey);
      }
      rowModel.addCell(
        new CellModel(CellUtil.cloneFamily(value), CellUtil.cloneQualifier(value),
          value.getTimestamp(), CellUtil.cloneValue(value)));
    } while (--count > 0);
    model.addRow(rowModel);
    ResponseBuilder response = Response.ok(model);
    response.cacheControl(cacheControl);
    servlet.getMetrics().incrementSucessfulGetRequests(1);
    return response.build();
  }

  @GET
  @Produces(MIMETYPE_BINARY)
  public Response getBinary(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath() + " as " +
        MIMETYPE_BINARY);
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      Cell value = generator.next();
      if (value == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("generator exhausted");
        }
        return Response.noContent().build();
      }
      ResponseBuilder response = Response.ok(CellUtil.cloneValue(value));
      response.cacheControl(cacheControl);
      response.header("X-Row", Bytes.toString(Base64.getEncoder().encode(
          CellUtil.cloneRow(value))));
      response.header("X-Column", Bytes.toString(Base64.getEncoder().encode(
          CellUtil.makeColumn(CellUtil.cloneFamily(value), CellUtil.cloneQualifier(value)))));
      response.header("X-Timestamp", value.getTimestamp());
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return response.build();
    } catch (IllegalStateException e) {
      if (ScannerResource.delete(id)) {
        servlet.getMetrics().incrementSucessfulDeleteRequests(1);
      } else {
        servlet.getMetrics().incrementFailedDeleteRequests(1);
      }
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.GONE)
        .type(MIMETYPE_TEXT).entity("Gone" + CRLF)
        .build();
    }
  }

  @DELETE
  public Response delete(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("DELETE " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    if (ScannerResource.delete(id)) {
      servlet.getMetrics().incrementSucessfulDeleteRequests(1);
    } else {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
    }
    return Response.ok().build();
  }
}
