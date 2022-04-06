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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RowResource extends ResourceBase {
  private static final Logger LOG = LoggerFactory.getLogger(RowResource.class);

  private static final String CHECK_PUT = "put";
  private static final String CHECK_DELETE = "delete";
  private static final String CHECK_APPEND = "append";
  private static final String CHECK_INCREMENT = "increment";

  private TableResource tableResource;
  private RowSpec rowspec;
  private String check = null;
  private boolean returnResult = false;

  /**
   * Constructor
   * @param tableResource
   * @param rowspec
   * @param versions
   * @param check
   * @param returnResult
   * @throws IOException
   */
  public RowResource(TableResource tableResource, String rowspec,
      String versions, String check, String returnResult) throws IOException {
    super();
    this.tableResource = tableResource;
    this.rowspec = new RowSpec(rowspec);
    if (versions != null) {
      this.rowspec.setMaxVersions(Integer.parseInt(versions));
    }
    this.check = check;
    if (returnResult != null) {
      this.returnResult = Boolean.valueOf(returnResult);
    }
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response get(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
    try {
      ResultGenerator generator =
        ResultGenerator.fromRowSpec(tableResource.getName(), rowspec, null,
          !params.containsKey(NOCACHE_PARAM_NAME));
      if (!generator.hasNext()) {
        servlet.getMetrics().incrementFailedGetRequests(1);
        return Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
          .build();
      }
      int count = 0;
      CellSetModel model = new CellSetModel();
      Cell value = generator.next();
      byte[] rowKey = CellUtil.cloneRow(value);
      RowModel rowModel = new RowModel(rowKey);
      do {
        if (!Bytes.equals(CellUtil.cloneRow(value), rowKey)) {
          model.addRow(rowModel);
          rowKey = CellUtil.cloneRow(value);
          rowModel = new RowModel(rowKey);
        }
        rowModel.addCell(new CellModel(CellUtil.cloneFamily(value), CellUtil.cloneQualifier(value),
          value.getTimestamp(), CellUtil.cloneValue(value)));
        if (++count > rowspec.getMaxValues()) {
          break;
        }
        value = generator.next();
      } while (value != null);
      model.addRow(rowModel);
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return Response.ok(model).build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    }
  }

  @GET
  @Produces(MIMETYPE_BINARY)
  public Response getBinary(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GET " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    servlet.getMetrics().incrementRequests(1);
    // doesn't make sense to use a non specific coordinate as this can only
    // return a single cell
    if (!rowspec.hasColumns() || rowspec.getColumns().length > 1) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.BAD_REQUEST).type(MIMETYPE_TEXT)
          .entity("Bad request: Default 'GET' method only works if there is exactly 1 column " +
                  "in the row. Using the 'Accept' header with one of these formats lets you " +
                  "retrieve the entire row if it has multiple columns: " +
                  // Same as the @Produces list for the get method.
                  MIMETYPE_XML + ", " + MIMETYPE_JSON + ", " +
                  MIMETYPE_PROTOBUF + ", " + MIMETYPE_PROTOBUF_IETF +
                  CRLF).build();
    }
    MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
    try {
      ResultGenerator generator =
        ResultGenerator.fromRowSpec(tableResource.getName(), rowspec, null,
          !params.containsKey(NOCACHE_PARAM_NAME));
      if (!generator.hasNext()) {
        servlet.getMetrics().incrementFailedGetRequests(1);
        return Response.status(Response.Status.NOT_FOUND)
          .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
          .build();
      }
      Cell value = generator.next();
      ResponseBuilder response = Response.ok(CellUtil.cloneValue(value));
      response.header("X-Timestamp", value.getTimestamp());
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return response.build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return processException(e);
    }
  }

  Response update(final CellSetModel model, final boolean replace) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }

    if (CHECK_PUT.equalsIgnoreCase(check)) {
      return checkAndPut(model);
    } else if (CHECK_DELETE.equalsIgnoreCase(check)) {
      return checkAndDelete(model);
    } else if (CHECK_APPEND.equalsIgnoreCase(check)) {
      return append(model);
    } else if (CHECK_INCREMENT.equalsIgnoreCase(check)) {
      return increment(model);
    } else if (check != null && check.length() > 0) {
      return Response.status(Response.Status.BAD_REQUEST)
        .type(MIMETYPE_TEXT).entity("Invalid check value '" + check + "'" + CRLF)
        .build();
    }

    Table table = null;
    try {
      List<RowModel> rows = model.getRows();
      List<Put> puts = new ArrayList<>();
      for (RowModel row: rows) {
        byte[] key = row.getKey();
        if (key == null) {
          key = rowspec.getRow();
        }
        if (key == null) {
          servlet.getMetrics().incrementFailedPutRequests(1);
          return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request: Row key not specified." + CRLF)
            .build();
        }
        Put put = new Put(key);
        int i = 0;
        for (CellModel cell: row.getCells()) {
          byte[] col = cell.getColumn();
          if (col == null) {
            try {
              col = rowspec.getColumns()[i++];
            } catch (ArrayIndexOutOfBoundsException e) {
              col = null;
            }
          }
          if (col == null) {
            servlet.getMetrics().incrementFailedPutRequests(1);
            return Response.status(Response.Status.BAD_REQUEST)
              .type(MIMETYPE_TEXT).entity("Bad request: Column found to be null." + CRLF)
              .build();
          }
          byte [][] parts = CellUtil.parseColumn(col);
          if (parts.length != 2) {
            return Response.status(Response.Status.BAD_REQUEST)
              .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
              .build();
          }
          put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(put.getRow())
              .setFamily(parts[0])
              .setQualifier(parts[1])
              .setTimestamp(cell.getTimestamp())
              .setType(Type.Put)
              .setValue(cell.getValue())
              .build());
        }
        puts.add(put);
        if (LOG.isTraceEnabled()) {
          LOG.trace("PUT " + put.toString());
        }
      }
      table = servlet.getTable(tableResource.getName());
      table.put(puts);
      ResponseBuilder response = Response.ok();
      servlet.getMetrics().incrementSucessfulPutRequests(1);
      return response.build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.debug("Exception received while closing the table", ioe);
        }
      }
    }
  }

  // This currently supports only update of one row at a time.
  Response updateBinary(final byte[] message, final HttpHeaders headers,
      final boolean replace) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    Table table = null;
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
        timestamp = Long.parseLong(vals.get(0));
      }
      if (column == null) {
        servlet.getMetrics().incrementFailedPutRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request: Column found to be null." + CRLF)
            .build();
      }
      Put put = new Put(row);
      byte parts[][] = CellUtil.parseColumn(column);
      if (parts.length != 2) {
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
          .build();
      }
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(put.getRow())
        .setFamily(parts[0])
        .setQualifier(parts[1])
        .setTimestamp(timestamp)
        .setType(Type.Put)
        .setValue(message)
        .build());
      table = servlet.getTable(tableResource.getName());
      table.put(put);
      if (LOG.isTraceEnabled()) {
        LOG.trace("PUT " + put.toString());
      }
      servlet.getMetrics().incrementSucessfulPutRequests(1);
      return Response.ok().build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.debug("Exception received while closing the table", ioe);
        }
      }
    }
  }

  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response put(final CellSetModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("PUT " + uriInfo.getAbsolutePath()
        + " " + uriInfo.getQueryParameters());
    }
    return update(model, true);
  }

  @PUT
  @Consumes(MIMETYPE_BINARY)
  public Response putBinary(final byte[] message,
      final @Context UriInfo uriInfo, final @Context HttpHeaders headers) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("PUT " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, true);
  }

  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
  public Response post(final CellSetModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("POST " + uriInfo.getAbsolutePath()
        + " " + uriInfo.getQueryParameters());
    }
    return update(model, false);
  }

  @POST
  @Consumes(MIMETYPE_BINARY)
  public Response postBinary(final byte[] message,
      final @Context UriInfo uriInfo, final @Context HttpHeaders headers) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("POST " + uriInfo.getAbsolutePath() + " as "+MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, false);
  }

  @DELETE
  public Response delete(final @Context UriInfo uriInfo) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("DELETE " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
      return Response.status(Response.Status.FORBIDDEN)
        .type(MIMETYPE_TEXT).entity("Forbidden" + CRLF)
        .build();
    }
    Delete delete = null;
    if (rowspec.hasTimestamp()) {
      delete = new Delete(rowspec.getRow(), rowspec.getTimestamp());
    } else {
      delete = new Delete(rowspec.getRow());
    }

    for (byte[] column: rowspec.getColumns()) {
      byte[][] split = CellUtil.parseColumn(column);
      if (rowspec.hasTimestamp()) {
        if (split.length == 1) {
          delete.addFamily(split[0], rowspec.getTimestamp());
        } else if (split.length == 2) {
          delete.addColumns(split[0], split[1], rowspec.getTimestamp());
        } else {
          return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
            .build();
        }
      } else {
        if (split.length == 1) {
          delete.addFamily(split[0]);
        } else if (split.length == 2) {
          delete.addColumns(split[0], split[1]);
        } else {
          return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
            .build();
        }
      }
    }
    Table table = null;
    try {
      table = servlet.getTable(tableResource.getName());
      table.delete(delete);
      servlet.getMetrics().incrementSucessfulDeleteRequests(1);
      if (LOG.isTraceEnabled()) {
        LOG.trace("DELETE " + delete.toString());
      }
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
      return processException(e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.debug("Exception received while closing the table", ioe);
        }
      }
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
    Table table = null;
    try {
      table = servlet.getTable(tableResource.getName());
      if (model.getRows().size() != 1) {
        servlet.getMetrics().incrementFailedPutRequests(1);
        return Response.status(Response.Status.BAD_REQUEST).type(MIMETYPE_TEXT)
            .entity("Bad request: Number of rows specified is not 1." + CRLF).build();
      }

      RowModel rowModel = model.getRows().get(0);
      byte[] key = rowModel.getKey();
      if (key == null) {
        key = rowspec.getRow();
      }

      List<CellModel> cellModels = rowModel.getCells();
      int cellModelCount = cellModels.size();
      if (key == null || cellModelCount <= 1) {
        servlet.getMetrics().incrementFailedPutRequests(1);
        return Response
            .status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT)
            .entity(
              "Bad request: Either row key is null or no data found for columns specified." + CRLF)
            .build();
      }

      Put put = new Put(key);
      boolean retValue;
      CellModel valueToCheckCell = cellModels.get(cellModelCount - 1);
      byte[] valueToCheckColumn = valueToCheckCell.getColumn();
      byte[][] valueToPutParts = CellUtil.parseColumn(valueToCheckColumn);
      if (valueToPutParts.length == 2 && valueToPutParts[1].length > 0) {
        CellModel valueToPutCell = null;

        // Copy all the cells to the Put request
        // and track if the check cell's latest value is also sent
        for (int i = 0, n = cellModelCount - 1; i < n ; i++) {
          CellModel cell = cellModels.get(i);
          byte[] col = cell.getColumn();

          if (col == null) {
            servlet.getMetrics().incrementFailedPutRequests(1);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MIMETYPE_TEXT).entity("Bad request: Column found to be null." + CRLF)
                    .build();
          }

          byte [][] parts = CellUtil.parseColumn(col);

          if (parts.length != 2) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MIMETYPE_TEXT).entity("Bad request" + CRLF)
                    .build();
          }
          put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(put.getRow())
              .setFamily(parts[0])
              .setQualifier(parts[1])
              .setTimestamp(cell.getTimestamp())
              .setType(Type.Put)
              .setValue(cell.getValue())
              .build());
          if(Bytes.equals(col,
                  valueToCheckCell.getColumn())) {
            valueToPutCell = cell;
          }
        }

        if (valueToPutCell == null) {
          servlet.getMetrics().incrementFailedPutRequests(1);
          return Response.status(Response.Status.BAD_REQUEST).type(MIMETYPE_TEXT)
              .entity("Bad request: The column to put and check do not match." + CRLF).build();
        } else {
          retValue = table.checkAndMutate(key, valueToPutParts[0]).qualifier(valueToPutParts[1])
            .ifEquals(valueToCheckCell.getValue()).thenPut(put);
        }
      } else {
        servlet.getMetrics().incrementFailedPutRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request: Column incorrectly specified." + CRLF)
          .build();
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("CHECK-AND-PUT " + put.toString() + ", returns " + retValue);
      }
      if (!retValue) {
        servlet.getMetrics().incrementFailedPutRequests(1);
        return Response.status(Response.Status.NOT_MODIFIED)
          .type(MIMETYPE_TEXT).entity("Value not Modified" + CRLF)
          .build();
      }
      ResponseBuilder response = Response.ok();
      servlet.getMetrics().incrementSucessfulPutRequests(1);
      return response.build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedPutRequests(1);
      return processException(e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.debug("Exception received while closing the table", ioe);
        }
      }
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
    Table table = null;
    Delete delete = null;
    try {
      table = servlet.getTable(tableResource.getName());
      if (model.getRows().size() != 1) {
        servlet.getMetrics().incrementFailedDeleteRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request: Number of rows specified is not 1." + CRLF)
          .build();
      }
      RowModel rowModel = model.getRows().get(0);
      byte[] key = rowModel.getKey();
      if (key == null) {
        key = rowspec.getRow();
      }
      if (key == null) {
        servlet.getMetrics().incrementFailedDeleteRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request: Row key found to be null." + CRLF)
          .build();
      }

      List<CellModel> cellModels = rowModel.getCells();
      int cellModelCount = cellModels.size();

      delete = new Delete(key);
      boolean retValue;
      CellModel valueToDeleteCell = rowModel.getCells().get(cellModelCount -1);
      byte[] valueToDeleteColumn = valueToDeleteCell.getColumn();
      if (valueToDeleteColumn == null) {
        try {
          valueToDeleteColumn = rowspec.getColumns()[0];
        } catch (final ArrayIndexOutOfBoundsException e) {
          servlet.getMetrics().incrementFailedDeleteRequests(1);
          return Response.status(Response.Status.BAD_REQUEST)
            .type(MIMETYPE_TEXT).entity("Bad request: Column not specified for check." + CRLF)
            .build();
        }
      }

      byte[][] parts ;
      // Copy all the cells to the Delete request if extra cells are sent
      if(cellModelCount > 1) {
        for (int i = 0, n = cellModelCount - 1; i < n; i++) {
          CellModel cell = cellModels.get(i);
          byte[] col = cell.getColumn();

          if (col == null) {
            servlet.getMetrics().incrementFailedPutRequests(1);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MIMETYPE_TEXT).entity("Bad request: Column found to be null." + CRLF)
                    .build();
          }

          parts = CellUtil.parseColumn(col);

          if (parts.length == 1) {
            // Only Column Family is specified
            delete.addFamily(parts[0], cell.getTimestamp());
          } else if (parts.length == 2) {
            delete.addColumn(parts[0], parts[1], cell.getTimestamp());
          } else {
            servlet.getMetrics().incrementFailedDeleteRequests(1);
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MIMETYPE_TEXT)
                    .entity("Bad request: Column to delete incorrectly specified." + CRLF)
                    .build();
          }
        }
      }

      parts = CellUtil.parseColumn(valueToDeleteColumn);
      if (parts.length == 2) {
        if (parts[1].length != 0) {
          // To support backcompat of deleting a cell
          // if that is the only cell passed to the rest api
          if(cellModelCount == 1) {
            delete.addColumns(parts[0], parts[1]);
          }
          retValue = table.checkAndMutate(key, parts[0]).qualifier(parts[1])
              .ifEquals(valueToDeleteCell.getValue()).thenDelete(delete);
        } else {
          // The case of empty qualifier.
          if(cellModelCount == 1) {
            delete.addColumns(parts[0], Bytes.toBytes(StringUtils.EMPTY));
          }
          retValue = table.checkAndMutate(key, parts[0])
              .ifEquals(valueToDeleteCell.getValue()).thenDelete(delete);
        }
      } else {
        servlet.getMetrics().incrementFailedDeleteRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
          .type(MIMETYPE_TEXT).entity("Bad request: Column to check incorrectly specified." + CRLF)
          .build();
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("CHECK-AND-DELETE " + delete.toString() + ", returns "
          + retValue);
      }

      if (!retValue) {
        servlet.getMetrics().incrementFailedDeleteRequests(1);
        return Response.status(Response.Status.NOT_MODIFIED)
            .type(MIMETYPE_TEXT).entity(" Delete check failed." + CRLF)
            .build();
      }
      ResponseBuilder response = Response.ok();
      servlet.getMetrics().incrementSucessfulDeleteRequests(1);
      return response.build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedDeleteRequests(1);
      return processException(e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.debug("Exception received while closing the table", ioe);
        }
      }
    }
  }

  /**
   * Validates the input request parameters, parses columns from CellSetModel,
   * and invokes Append on HTable.
   *
   * @param model instance of CellSetModel
   * @return Response 200 OK, 304 Not modified, 400 Bad request
   */
  Response append(final CellSetModel model) {
    Table table = null;
    Append append = null;
    try {
      table = servlet.getTable(tableResource.getName());
      if (model.getRows().size() != 1) {
        servlet.getMetrics().incrementFailedAppendRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
                .type(MIMETYPE_TEXT).entity("Bad request: Number of rows specified is not 1." + CRLF)
                .build();
      }
      RowModel rowModel = model.getRows().get(0);
      byte[] key = rowModel.getKey();
      if (key == null) {
        key = rowspec.getRow();
      }
      if (key == null) {
        servlet.getMetrics().incrementFailedAppendRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
                .type(MIMETYPE_TEXT).entity("Bad request: Row key found to be null." + CRLF)
                .build();
      }

      append = new Append(key);
      append.setReturnResults(returnResult);
      int i = 0;
      for (CellModel cell: rowModel.getCells()) {
        byte[] col = cell.getColumn();
        if (col == null) {
          try {
            col = rowspec.getColumns()[i++];
          } catch (ArrayIndexOutOfBoundsException e) {
            col = null;
          }
        }
        if (col == null) {
          servlet.getMetrics().incrementFailedAppendRequests(1);
          return Response.status(Response.Status.BAD_REQUEST)
                  .type(MIMETYPE_TEXT).entity("Bad request: Column found to be null." + CRLF)
                  .build();
        }
        byte [][] parts = CellUtil.parseColumn(col);
        if (parts.length != 2) {
          servlet.getMetrics().incrementFailedAppendRequests(1);
          return Response.status(Response.Status.BAD_REQUEST)
                  .type(MIMETYPE_TEXT).entity("Bad request: Column incorrectly specified." + CRLF)
                  .build();
        }
        append.add(parts[0], parts[1], cell.getValue());
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("APPEND " + append.toString());
      }
      Result result = table.append(append);
      if (returnResult) {
        if (result.isEmpty()) {
          servlet.getMetrics().incrementFailedAppendRequests(1);
          return Response.status(Response.Status.NOT_MODIFIED)
                  .type(MIMETYPE_TEXT).entity("Append return empty." + CRLF)
                  .build();
        }

        CellSetModel rModel = new CellSetModel();
        RowModel rRowModel = new RowModel(result.getRow());
        for (Cell cell : result.listCells()) {
          rRowModel.addCell(new CellModel(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
                  cell.getTimestamp(), CellUtil.cloneValue(cell)));
        }
        rModel.addRow(rRowModel);
        servlet.getMetrics().incrementSucessfulAppendRequests(1);
        return Response.ok(rModel).build();
      }
      servlet.getMetrics().incrementSucessfulAppendRequests(1);
      return Response.ok().build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedAppendRequests(1);
      return processException(e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.debug("Exception received while closing the table" + table.getName(), ioe);
        }
      }
    }
  }

  /**
   * Validates the input request parameters, parses columns from CellSetModel,
   * and invokes Increment on HTable.
   *
   * @param model instance of CellSetModel
   * @return Response 200 OK, 304 Not modified, 400 Bad request
   */
  Response increment(final CellSetModel model) {
    Table table = null;
    Increment increment = null;
    try {
      table = servlet.getTable(tableResource.getName());
      if (model.getRows().size() != 1) {
        servlet.getMetrics().incrementFailedIncrementRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
                .type(MIMETYPE_TEXT).entity("Bad request: Number of rows specified is not 1." + CRLF)
                .build();
      }
      RowModel rowModel = model.getRows().get(0);
      byte[] key = rowModel.getKey();
      if (key == null) {
        key = rowspec.getRow();
      }
      if (key == null) {
        servlet.getMetrics().incrementFailedIncrementRequests(1);
        return Response.status(Response.Status.BAD_REQUEST)
                .type(MIMETYPE_TEXT).entity("Bad request: Row key found to be null." + CRLF)
                .build();
      }

      increment = new Increment(key);
      increment.setReturnResults(returnResult);
      int i = 0;
      for (CellModel cell: rowModel.getCells()) {
        byte[] col = cell.getColumn();
        if (col == null) {
          try {
            col = rowspec.getColumns()[i++];
          } catch (ArrayIndexOutOfBoundsException e) {
            col = null;
          }
        }
        if (col == null) {
          servlet.getMetrics().incrementFailedIncrementRequests(1);
          return Response.status(Response.Status.BAD_REQUEST)
                  .type(MIMETYPE_TEXT).entity("Bad request: Column found to be null." + CRLF)
                  .build();
        }
        byte [][] parts = CellUtil.parseColumn(col);
        if (parts.length != 2) {
          servlet.getMetrics().incrementFailedIncrementRequests(1);
          return Response.status(Response.Status.BAD_REQUEST)
                  .type(MIMETYPE_TEXT).entity("Bad request: Column incorrectly specified." + CRLF)
                  .build();
        }
        increment.addColumn(parts[0], parts[1], Long.parseLong(Bytes.toStringBinary(cell.getValue())));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("INCREMENT " + increment.toString());
      }
      Result result = table.increment(increment);

      if (returnResult) {
        if (result.isEmpty()) {
          servlet.getMetrics().incrementFailedIncrementRequests(1);
          return Response.status(Response.Status.NOT_MODIFIED)
                  .type(MIMETYPE_TEXT).entity("Increment return empty." + CRLF)
                  .build();
        }

        CellSetModel rModel = new CellSetModel();
        RowModel rRowModel = new RowModel(result.getRow());
        for (Cell cell : result.listCells()) {
          rRowModel.addCell(new CellModel(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
                  cell.getTimestamp(), CellUtil.cloneValue(cell)));
        }
        rModel.addRow(rowModel);
        servlet.getMetrics().incrementSucessfulIncrementRequests(1);
        return Response.ok(rModel).build();
      }

      ResponseBuilder response = Response.ok();
      servlet.getMetrics().incrementSucessfulIncrementRequests(1);
      return response.build();
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedIncrementRequests(1);
      return processException(e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.debug("Exception received while closing the table " + table.getName(), ioe);
        }
      }
    }
  }
}
