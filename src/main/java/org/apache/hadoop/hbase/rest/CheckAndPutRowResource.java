/*
 * Copyright The Apache Software Foundation
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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;


public class CheckAndPutRowResource extends ResourceBase {
  private static final Log LOG = LogFactory
    .getLog(CheckAndPutRowResource.class);

  CheckAndPutTableResource tableResource;
  RowSpec rowspec;

  /**
   * Constructor
   * 
   * @param tableResource
   * @param rowspec
   * @param versions
   * @throws IOException
   */
  public CheckAndPutRowResource(CheckAndPutTableResource tableResource,
      String rowspec, String versions) throws IOException {
    super();
    this.tableResource = tableResource;
    this.rowspec = new RowSpec(rowspec);
    if (versions != null) {
      this.rowspec.setMaxVersions(Integer.valueOf(versions));
    }
  }

  /**
   * Validates the input request parameters, parses columns from CellSetModel,
   * and invokes checkAndPut on HTable.
   * 
   * @param model instance of CellSetModel
   * @return Response 200 OK, 304 Not modified, 400 Bad request
   */
  Response update(final CellSetModel model) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      if (model.getRows().size() != 1) {
        throw new WebApplicationException(Response.Status.BAD_REQUEST);
      }
      
      RowModel rowModel = model.getRows().get(0);
      byte[] key = rowModel.getKey();
      if (key == null) {
        key = rowspec.getRow();
      }
      if (key == null) {
        throw new WebApplicationException(Response.Status.BAD_REQUEST);
      }

      if (rowModel.getCells().size() != 2) {
        throw new WebApplicationException(Response.Status.BAD_REQUEST);
      }

      Put put = new Put(key);

      CellModel valueToPutCell = rowModel.getCells().get(0);
      byte[] valueToPutColumn = valueToPutCell.getColumn();
      if (valueToPutColumn == null) {
        try {
          valueToPutColumn = rowspec.getColumns()[0];
        } catch (final ArrayIndexOutOfBoundsException e) {
          throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
      }

      byte[][] valueToPutParts = KeyValue.parseColumn(valueToPutColumn);
      if (valueToPutParts.length == 2 && valueToPutParts[1].length > 0) {
        put.add(valueToPutParts[0], valueToPutParts[1], valueToPutCell
            .getTimestamp(), valueToPutCell.getValue());
      } else {
        throw new WebApplicationException(Response.Status.BAD_REQUEST);
      }

      CellModel valueToCheckCell = rowModel.getCells().get(1);
      byte[] valueToCheckColumn = valueToCheckCell.getColumn();
      if (valueToCheckColumn == null) {
        try {
          valueToCheckColumn = rowspec.getColumns()[1];
        } catch (final ArrayIndexOutOfBoundsException e) {
          throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
      }

      table = pool.getTable(tableResource.getName());
      boolean retValue = table.checkAndPut(key, valueToPutParts[0],
          valueToPutParts[1], valueToCheckCell.getValue(), put);
      if (LOG.isDebugEnabled()) {
        LOG.debug("CHECK-AND-PUT " + put.toString() + ", returns " + retValue);
      }
      table.flushCommits();
      ResponseBuilder response = Response.ok();
      if (!retValue) {
        response = Response.status(304);
      }
      return response.build();
    } catch (final IOException e) {
      throw new WebApplicationException(e, Response.Status.SERVICE_UNAVAILABLE);
    } finally {
      try {
          pool.putTable(table);
      } catch (IOException ioe) {
          throw new WebApplicationException(ioe,
              Response.Status.SERVICE_UNAVAILABLE);
      }
    }
  }

  @PUT
  @Consumes({ MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF })
  public Response put(final CellSetModel model, final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath());
    }
    return update(model);
  }

  @POST
  @Consumes({ MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF })
  public Response post(final CellSetModel model, final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("POST " + uriInfo.getAbsolutePath());
    }
    return update(model);
  }
}

