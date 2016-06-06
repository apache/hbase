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

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;

@InterfaceAudience.Private
public class MultiRowResource extends ResourceBase implements Constants {
  private static final Log LOG = LogFactory.getLog(MultiRowResource.class);

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
        ResultGenerator generator =
          ResultGenerator.fromRowSpec(this.tableResource.getName(), rowSpec, null,
            !params.containsKey(NOCACHE_PARAM_NAME));
        Cell value = null;
        RowModel rowModel = new RowModel(rk);
        if (generator.hasNext()) {
          while ((value = generator.next()) != null) {
            rowModel.addCell(new CellModel(CellUtil.cloneFamily(value), CellUtil
                .cloneQualifier(value), value.getTimestamp(), CellUtil.cloneValue(value)));
          }
          model.addRow(rowModel);
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("The row : " + rk + " not found in the table.");
          }
        }
      }

      if (model.getRows().size() == 0) {
      //If no rows found.
        servlet.getMetrics().incrementFailedGetRequests(1);
        return Response.status(Response.Status.NOT_FOUND)
            .type(MIMETYPE_TEXT).entity("No rows found." + CRLF)
            .build();
      } else {
        servlet.getMetrics().incrementSucessfulGetRequests(1);
        return Response.ok(model).build();
      }
    } catch (Exception e) {
      servlet.getMetrics().incrementFailedGetRequests(1);
      return processException(e);
    }
  }
}
