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
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;

public class RowResource extends ResourceBase {
  private static final Log LOG = LogFactory.getLog(RowResource.class);

  static final String CHECK_PUT = "put";
  static final String CHECK_DELETE = "delete";

  TableResource tableResource;
  RowSpec rowspec;
  private String check = null;

  /**
   * Constructor
   * @param tableResource
   * @param rowspec
   * @param versions
   * @throws IOException
   */
  public RowResource(TableResource tableResource, String rowspec,
      String versions, String check) throws IOException {
    super();
    this.tableResource = tableResource;
    this.rowspec = new RowSpec(rowspec);
    if (versions != null) {
      this.rowspec.setMaxVersions(Integer.valueOf(versions));
    }
    this.check = check;
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response get(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      ResultGenerator generator =
        ResultGenerator.fromRowSpec(tableResource.getName(), rowspec, null);
      if (!generator.hasNext()) {
        return Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
          .build();
      }
      int count = 0;
      CellSetModel model = new CellSetModel();
      KeyValue value = generator.next();
      byte[] rowKey = value.getRow();
      RowModel rowModel = new RowModel(rowKey);
      do {
        if (!Bytes.equals(value.getRow(), rowKey)) {
          model.addRow(rowModel);
          rowKey = value.getRow();
          rowModel = new RowModel(rowKey);
        }
        rowModel.addCell(new CellModel(value.getFamily(), value.getQualifier(),
          value.getTimestamp(), value.getValue()));
        if (++count > rowspec.getMaxValues()) {
          break;
        }
        value = generator.next();
      } while (value != null);
      model.addRow(rowModel);
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return Response.ok(model).build();
    } catch (RuntimeException e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      if (e.getCause() instanceof TableNotFoundException) {
        return Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
          .build();
      }
      return Response.status(Response.Status.BAD_REQUEST)
        .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
        .build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    }
  }

  @GET
  @Produces(MIMETYPE_BINARY)
  public Response getBinary(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    servlet.getMetrics().incrementRequests(1);
    // doesn't make sense to use a non specific coordinate as this can only
    // return a single cell
    if (!rowspec.hasColumns() || rowspec.getColumns().length > 1) {
      return Response.status(Response.Status.BAD_REQUEST)
        .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
        .build();
    }
    try {
      ResultGenerator generator =
        ResultGenerator.fromRowSpec(tableResource.getName(), rowspec, null);
      if (!generator.hasNext()) {
        return Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
          .build();
      }
      KeyValue value = generator.next();
      ResponseBuilder response = Response.ok(value.getValue());
      response.header("X-Timestamp", value.getTimestamp());
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return response.build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    }
  }

  Response update(final CellSetModel model, final boolean replace) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }

    if (CHECK_PUT.equalsIgnoreCase(check)) {
      return checkAndPut(model);
    } else if (CHECK_DELETE.equalsIgnoreCase(check)) {
      return checkAndDelete(model);
    } else if (check != null && check.length() > 0) {
      return Response.status(Response.Status.BAD_REQUEST)
        .type(MIMETYPE_TEXT).entity("Invalid check value '" + check + "'" + CRLF)
        .build();
    }

    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      List<RowModel> rows = model.getRows();
      List<Put> puts = new ArrayList<Put>();
      for (RowModel row: rows) {
        byte[] key = row.getKey();
        if (key == null) {
          key = rowspec.getRow();
        }
        if (key == null) {
          return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
            .build();
        }
        Put put = new Put(key);
        int i = 0;
        for (CellModel cell: row.getCells()) {
          byte[] col = cell.getColumn();
          if (col == null) try {
            col = rowspec.getColumns()[i++];
          } catch (ArrayIndexOutOfBoundsException e) {
            col = null;
          }
          if (col == null) {
            return Response.status(Response.Status.BAD_REQUEST)
              .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
              .build();
          }
          byte [][] parts = KeyValue.parseColumn(col);
          if (parts.length == 2 && parts[1].length > 0) {
            put.add(parts[0], parts[1], cell.getTimestamp(), cell.getValue());
          } else {
            put.add(parts[0], null, cell.getTimestamp(), cell.getValue());
          }
        }
        puts.add(put);
        if (LOG.isDebugEnabled()) {
          LOG.debug("PUT " + put.toString());
        }
      }
      table = pool.getTable(tableResource.getName());
      table.put(puts);
      table.flushCommits();
      ResponseBuilder response = Response.ok();
      servlet.getMetrics().incrementSucessfulPutRequests(1);
      return response.build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    } finally {
      if (table != null) try {
        table.close();
      } catch (IOException ioe) {
        LOG.debug("Exception received while closing the table", ioe);
      }
    }
  }

  // This currently supports only update of one row at a time.
  Response updateBinary(final byte[] message, final HttpHeaders headers,
      final boolean replace) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      byte[] row = rowspec.getRow();
      byte[][] columns = rowspec.getColumns();
      byte[] column = null;
      if (columns != null) {
        column = columns[0];
      }
      long timestamp = HConstants.LATEST_TIMESTAMP;
      List<String> vals = headers.getRequestHeader("X-Row");
      if (vals != null && !vals.isEmpty()) {
        row = Bytes.toBytes(vals.get(0));
      }
      vals = headers.getRequestHeader("X-Column");
      if (vals != null && !vals.isEmpty()) {
        column = Bytes.toBytes(vals.get(0));
      }
      vals = headers.getRequestHeader("X-Timestamp");
      if (vals != null && !vals.isEmpty()) {
        timestamp = Long.valueOf(vals.get(0));
      }
      if (column == null) {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }
      Put put = new Put(row);
      byte parts[][] = KeyValue.parseColumn(column);
      if (parts.length == 2 && parts[1].length > 0) {
        put.add(parts[0], parts[1], timestamp, message);
      } else {
        put.add(parts[0], null, timestamp, message);
      }
      table = pool.getTable(tableResource.getName());
      table.put(put);
      if (LOG.isDebugEnabled()) {
        LOG.debug("PUT " + put.toString());
      }
      servlet.getMetrics().incrementSucessfulPutRequests(1);
      return Response.ok().build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    } finally {
      if (table != null) try {
        table.close();
      } catch (IOException ioe) { }
    }
  }

  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response put(final CellSetModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath()
        + " " + uriInfo.getQueryParameters());
    }
    return update(model, true);
  }

  @PUT
  @Consumes(MIMETYPE_BINARY)
  public Response putBinary(final byte[] message,
      final @Context UriInfo uriInfo, final @Context HttpHeaders headers) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, true);
  }

  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response post(final CellSetModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("POST " + uriInfo.getAbsolutePath()
        + " " + uriInfo.getQueryParameters());
    }
    return update(model, false);
  }

  @POST
  @Consumes(MIMETYPE_BINARY)
  public Response postBinary(final byte[] message,
      final @Context UriInfo uriInfo, final @Context HttpHeaders headers) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("POST " + uriInfo.getAbsolutePath() + " as "+MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, false);
  }

  @DELETE
  public Response delete(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETE " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    Delete delete = null;
    if (rowspec.hasTimestamp())
      delete = new Delete(rowspec.getRow(), rowspec.getTimestamp(), null);
    else
      delete = new Delete(rowspec.getRow());

    for (byte[] column: rowspec.getColumns()) {
      byte[][] split = KeyValue.parseColumn(column);
      if (rowspec.hasTimestamp()) {
        if (split.length == 2 && split[1].length != 0) {
          delete.deleteColumns(split[0], split[1], rowspec.getTimestamp());
        } else {
          delete.deleteFamily(split[0], rowspec.getTimestamp());
        }
      } else {
        if (split.length == 2 && split[1].length != 0) {
          delete.deleteColumns(split[0], split[1]);
        } else {
          delete.deleteFamily(split[0]);
        }
      }
    }
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      table = pool.getTable(tableResource.getName());
      table.delete(delete);
      servlet.getMetrics().incrementSucessfulDeleteRequests(1);
      if (LOG.isDebugEnabled()) {
        LOG.debug("DELETE " + delete.toString());
      }
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    } finally {
      if (table != null) try {
        table.close();
      } catch (IOException ioe) { }
    }
    return Response.ok().build();
  }

  /**
   * Validates the input request parameters, parses columns from CellSetModel,
   * and invokes checkAndPut on HTable.
   *
   * @param model instance of CellSetModel
   * @return Response 200 OK, 304 Not modified, 400 Bad request
   */
  Response checkAndPut(final CellSetModel model) {
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      if (model.getRows().size() != 1) {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }

      RowModel rowModel = model.getRows().get(0);
      byte[] key = rowModel.getKey();
      if (key == null) {
        key = rowspec.getRow();
      }

      List<CellModel> cellModels = rowModel.getCells();
      int cellModelCount = cellModels.size();
      if (key == null || cellModelCount <= 1) {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }

      Put put = new Put(key);
      CellModel valueToCheckCell = cellModels.get(cellModelCount - 1);
      byte[] valueToCheckColumn = valueToCheckCell.getColumn();
      byte[][] valueToPutParts = KeyValue.parseColumn(valueToCheckColumn);
      if (valueToPutParts.length == 2 && valueToPutParts[1].length > 0) {
        CellModel valueToPutCell = null;
        for (int i = 0, n = cellModelCount - 1; i < n ; i++) {
          if(Bytes.equals(cellModels.get(i).getColumn(),
              valueToCheckCell.getColumn())) {
            valueToPutCell = cellModels.get(i);
            break;
          }
        }
        if (valueToPutCell != null) {
          put.add(valueToPutParts[0], valueToPutParts[1], valueToPutCell
            .getTimestamp(), valueToPutCell.getValue());
        } else {
          return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
            .build();
        }
      } else {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }

      table = pool.getTable(this.tableResource.getName());
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
    } catch (IOException e) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    } finally {
      if (table != null) try {
        table.close();
      } catch (IOException ioe) { }
    }
  }

  /**
   * Validates the input request parameters, parses columns from CellSetModel,
   * and invokes checkAndDelete on HTable.
   *
   * @param model instance of CellSetModel
   * @return Response 200 OK, 304 Not modified, 400 Bad request
   */
  Response checkAndDelete(final CellSetModel model) {
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    Delete delete = null;
    try {
      if (model.getRows().size() != 1) {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }
      RowModel rowModel = model.getRows().get(0);
      byte[] key = rowModel.getKey();
      if (key == null) {
        key = rowspec.getRow();
      }
      if (key == null) {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }

      delete = new Delete(key);
      CellModel valueToDeleteCell = rowModel.getCells().get(0);
      byte[] valueToDeleteColumn = valueToDeleteCell.getColumn();
      if (valueToDeleteColumn == null) {
        try {
          valueToDeleteColumn = rowspec.getColumns()[0];
        } catch (final ArrayIndexOutOfBoundsException e) {
          return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
            .build();
        }
      }
      byte[][] parts = KeyValue.parseColumn(valueToDeleteColumn);
      if (parts.length == 2 && parts[1].length > 0) {
        delete.deleteColumns(parts[0], parts[1]);
      } else {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }

      table = pool.getTable(tableResource.getName());
      boolean retValue = table.checkAndDelete(key, parts[0], parts[1],
        valueToDeleteCell.getValue(), delete);
      if (LOG.isDebugEnabled()) {
        LOG.debug("CHECK-AND-DELETE " + delete.toString() + ", returns "
          + retValue);
      }
      table.flushCommits();
      ResponseBuilder response = Response.ok();
      if (!retValue) {
        response = Response.status(304);
      }
      return response.build();
    } catch (IOException e) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    } finally {
      if (table != null) try {
        table.close();
      } catch (IOException ioe) {
        LOG.debug("Exception received while closing the table", ioe);
      }
    }
  }
}
