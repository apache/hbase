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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.rest.model.ColumnSchemaModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class SchemaResource extends ResourceBase {
  private static final Log LOG = LogFactory.getLog(SchemaResource.class);

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
  public SchemaResource(TableResource tableResource) throws IOException {
    super();
    this.tableResource = tableResource;
  }

  private HTableDescriptor getTableSchema() throws IOException,
      TableNotFoundException {
    HTableInterface table = servlet.getTable(tableResource.getName());
    try {
      return table.getTableDescriptor();
    } finally {
      table.close();
    }
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
      ResponseBuilder response =
        Response.ok(new TableSchemaModel(getTableSchema()));
      response.cacheControl(cacheControl);
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return response.build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return processException(e);
    }
  }

  private Response replace(final byte[] name, final TableSchemaModel model,
      final UriInfo uriInfo, final HBaseAdmin admin) {
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    try {
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
      for (Map.Entry<QName,Object> e: model.getAny().entrySet()) {
        htd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
      }
      for (ColumnSchemaModel family: model.getColumns()) {
        HColumnDescriptor hcd = new HColumnDescriptor(family.getName());
        for (Map.Entry<QName,Object> e: family.getAny().entrySet()) {
          hcd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
        }
        htd.addFamily(hcd);
      }
      if (admin.tableExists(name)) {
        admin.disableTable(name);
        admin.modifyTable(name, htd);
        admin.enableTable(name);
        servlet.getMetrics().incrementSucessfulPutRequests(1);
      } else try {
        admin.createTable(htd);
        servlet.getMetrics().incrementSucessfulPutRequests(1);
      } catch (TableExistsException e) {
        // race, someone else created a table with the same name
        return Response.status(Response.Status.NOT_MODIFIED)
          .type(MIMETYPE_TEXT).entity("Not modified" + CRLF)
          .build();
      }
      return Response.created(uriInfo.getAbsolutePath()).build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    }
  }

  private Response update(final byte[] name, final TableSchemaModel model,
      final UriInfo uriInfo, final HBaseAdmin admin) {
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    try {
      HTableDescriptor htd = admin.getTableDescriptor(name);
      admin.disableTable(name);
      try {
        for (ColumnSchemaModel family: model.getColumns()) {
          HColumnDescriptor hcd = new HColumnDescriptor(family.getName());
          for (Map.Entry<QName,Object> e: family.getAny().entrySet()) {
            hcd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
          }
          if (htd.hasFamily(hcd.getName())) {
            admin.modifyColumn(name, hcd);
          } else {
            admin.addColumn(name, hcd);
          }
        }
      } catch (IOException e) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
          .build();
      } finally {
        admin.enableTable(tableResource.getName());
      }
      servlet.getMetrics().incrementSucessfulPutRequests(1);
      return Response.ok().build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    }
  }

  private Response update(final TableSchemaModel model, final boolean replace,
      final UriInfo uriInfo) {
    try {
      byte[] name = Bytes.toBytes(tableResource.getName());
      HBaseAdmin admin = servlet.getAdmin();
      if (replace || !admin.tableExists(name)) {
        return replace(name, model, uriInfo, admin);
      } else {
        return update(name, model, uriInfo, admin);
      }
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    }
  }

  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response put(final TableSchemaModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("PUT " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    return update(model, true, uriInfo);
  }

  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response post(final TableSchemaModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("PUT " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    return update(model, false, uriInfo);
  }

  @DELETE
  public Response delete(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("DELETE " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN).type(MIMETYPE_TEXT)
          .entity("Forbidden" + CRLF).build();
    }
    try {
      HBaseAdmin admin = servlet.getAdmin();
      try {
        admin.disableTable(tableResource.getName());
      } catch (TableNotEnabledException e) { /* this is what we want anyway */ }
      admin.deleteTable(tableResource.getName());
      servlet.getMetrics().incrementSucessfulDeleteRequests(1);
      return Response.ok().build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
      return processException(e);
    }
  }
}
