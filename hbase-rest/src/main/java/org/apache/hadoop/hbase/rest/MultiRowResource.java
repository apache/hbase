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

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MultivaluedMap;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.core.UriInfo;

@InterfaceAudience.Private
public class MultiRowResource extends ResourceBase implements Constants {
  private static final Logger LOG = LoggerFactory.getLogger(MultiRowResource.class);

  TableResource tableResource;
  Integer versions = null;
  String[] columns = null;

  /**
   * Constructor nn * @throws java.io.IOException
   */
  public MultiRowResource(TableResource tableResource, String versions, String columnsStr)
    throws IOException {
    super();
    this.tableResource = tableResource;

    if (columnsStr != null && !columnsStr.equals("")) {
      this.columns = columnsStr.split(",");
    }

    if (versions != null) {
      this.versions = Integer.valueOf(versions);

    }
  }

  @GET
  @Produces({ MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF, MIMETYPE_PROTOBUF_IETF })
  public Response get(final @Context UriInfo uriInfo) {
    MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

    servlet.getMetrics().incrementRequests(1);
    try {
      CellSetModel model = new CellSetModel();
      for (String rk : params.get(ROW_KEYS_PARAM_NAME)) {
        RowSpec rowSpec = new RowSpec(rk);

        if (this.versions != null) {
          rowSpec.setMaxVersions(this.versions);
        }

        if (this.columns != null) {
          for (int i = 0; i < this.columns.length; i++) {
            rowSpec.addColumn(Bytes.toBytes(this.columns[i]));
          }
        }

        ResultGenerator generator = ResultGenerator.fromRowSpec(this.tableResource.getName(),
          rowSpec, null, !params.containsKey(NOCACHE_PARAM_NAME));
        Cell value = null;
        RowModel rowModel = new RowModel(rowSpec.getRow());
        if (generator.hasNext()) {
          while ((value = generator.next()) != null) {
            rowModel.addCell(new CellModel(CellUtil.cloneFamily(value),
              CellUtil.cloneQualifier(value), value.getTimestamp(), CellUtil.cloneValue(value)));
          }
          model.addRow(rowModel);
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("The row : " + rk + " not found in the table.");
          }
        }
      }

      if (model.getRows().isEmpty()) {
        // If no rows found.
        servlet.getMetrics().incrementFailedGetRequests(1);
        return Response.status(Response.Status.NOT_FOUND).type(MIMETYPE_TEXT)
          .entity("No rows found." + CRLF).build();
      } else {
        servlet.getMetrics().incrementSucessfulGetRequests(1);
        return Response.ok(model).build();
      }
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return processException(e);
    }
  }
}
