/**
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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.rest.ResourceBase;
import org.apache.hadoop.hbase.rest.RowSpec;
import org.apache.hadoop.hbase.rest.TableResource;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

public class MultiRowResource extends ResourceBase {
  public static final String ROW_KEYS_PARAM_NAME = "row";

  TableResource tableResource;
  Integer versions = null;

  /**
   * Constructor
   *
   * @param tableResource
   * @param versions
   * @throws java.io.IOException
   */
  public MultiRowResource(TableResource tableResource, String versions) throws IOException {
    super();
    this.tableResource = tableResource;

    if (versions != null) {
      this.versions = Integer.valueOf(versions);

    }
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_PROTOBUF_IETF})
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

        ResultGenerator generator =
          ResultGenerator.fromRowSpec(this.tableResource.getName(), rowSpec, null);
        if (!generator.hasNext()) {
          return Response.status(Response.Status.NOT_FOUND)
            .type(MIMETYPE_TEXT).entity("Not found" + CRLF)
            .build();
        }

        KeyValue value = null;
        RowModel rowModel = new RowModel(rk);

        while ((value = generator.next()) != null) {
          rowModel.addCell(new CellModel(value.getFamily(), value.getQualifier(),
            value.getTimestamp(), value.getValue()));
        }

        model.addRow(rowModel);
      }
      servlet.getMetrics().incrementSucessfulGetRequests(1);
      return Response.ok(model).build();
    } catch (IOException e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
        .build();
    }

  }
}
